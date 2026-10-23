package vfsbridge

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

// RootInodeID is the inode number for the root directory.
// FSKit reserves: 0 = invalid, 1 = parentOfRoot, 2 = rootDirectory.
const RootInodeID = uint64(2)

// InodeTable manages the mapping between inode IDs and VFS nodes.
type InodeTable struct {
	mu       sync.RWMutex
	nextID   atomic.Uint64
	idToNode map[uint64]vfs.Node
	idToPath map[uint64]string
	pathToID map[string]uint64
	// stale holds ids whose node belongs to a VFS that has been replaced
	// (see MarkSubtreeStale); GetNode re-resolves them by path through
	// resolve before answering, so the id survives the replacement.
	stale   map[uint64]bool
	resolve func(path string) (vfs.Node, error)
}

// NewInodeTable creates a new inode table.
func NewInodeTable() *InodeTable {
	t := &InodeTable{
		idToNode: make(map[uint64]vfs.Node),
		idToPath: make(map[uint64]string),
		pathToID: make(map[string]uint64),
		stale:    make(map[uint64]bool),
	}
	// Start after reserved IDs
	t.nextID.Store(RootInodeID + 1)
	return t
}

// GetOrCreateRootInfo creates the root ItemInfo for the FSKit activate call.
//
// modTime is when the set of exposed remotes last changed. It must NOT be the
// current time: the synthetic root's contents are the remote names, so its
// modification time is meaningful only if it stays put while that set does.
// Reporting time.Now() on every call made the root look modified on every
// getattr, which is indistinguishable from never being modified -- a client
// polling mtime to decide whether to re-read the directory gets noise either
// way, and Finder never picked up a remote that had just been added.
func (t *InodeTable) GetOrCreateRootInfo(modTime int64) ItemInfo {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.idToPath[RootInodeID] = ""

	return ItemInfo{
		ID:       RootInodeID,
		ParentID: 1, // parentOfRoot
		Name:     "/",
		IsDir:    true,
		Size:     0,
		Mode:     0755,
		ModTime:  modTime,
		UID:      501,
		GID:      20,
	}
}

// Assign assigns an inode ID to a VFS node, or returns the existing ID if already assigned.
func (t *InodeTable) Assign(node vfs.Node, path string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if path already has an ID
	if id, ok := t.pathToID[path]; ok {
		// Update the node reference (it may have been refreshed)
		t.idToNode[id] = node
		return id
	}

	id := t.nextID.Add(1) - 1
	t.idToNode[id] = node
	t.idToPath[id] = path
	t.pathToID[path] = id
	return id
}

// GetNode returns the VFS node for the given inode ID.
func (t *InodeTable) GetNode(id uint64) vfs.Node {
	t.mu.RLock()
	node := t.idToNode[id]
	stale := t.stale[id]
	path := t.idToPath[id]
	t.mu.RUnlock()
	if !stale || t.resolve == nil {
		return node
	}
	// The node belongs to a VFS that has been replaced. Resolve the same
	// path on the current one, off the lock: this can list a directory on
	// the backend. A path the new VFS does not have is gone; any other
	// failure keeps the old node, whose operations fail as a shut-down VFS
	// fails them, rather than answering not-found for a path that exists.
	fresh, err := t.resolve(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stale[id] {
		return t.idToNode[id] // someone else resolved it meanwhile
	}
	switch {
	case err == nil:
		t.idToNode[id] = fresh
		delete(t.stale, id)
		return fresh
	case errors.Is(err, vfs.ENOENT), errors.Is(err, fs.ErrorObjectNotFound), errors.Is(err, fs.ErrorDirNotFound):
		if t.pathToID[path] == id {
			delete(t.pathToID, path)
		}
		delete(t.idToNode, id)
		delete(t.idToPath, id)
		delete(t.stale, id)
		return nil
	default:
		return node
	}
}

// MarkSubtreeStale marks the remote's root id and every id below it as
// belonging to a replaced VFS, so the next GetNode on each re-resolves it by
// path on the current one. The ids themselves do not change: a client that
// holds them keeps working across the replacement.
func (t *InodeTable) MarkSubtreeStale(remote string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	prefix := remote + "/"
	for id, path := range t.idToPath {
		if path == remote || strings.HasPrefix(path, prefix) {
			t.stale[id] = true
		}
	}
}

// SubtreeIDs returns the remote's root id and every id below it.
func (t *InodeTable) SubtreeIDs(remote string) []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	prefix := remote + "/"
	ids := make([]uint64, 0)
	for id, path := range t.idToPath {
		if path == remote || strings.HasPrefix(path, prefix) {
			ids = append(ids, id)
		}
	}
	return ids
}

// GetPath returns the path for the given inode ID.
func (t *InodeTable) GetPath(id uint64) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.idToPath[id]
}

// GetParentID returns the parent inode ID for the given inode ID.
func (t *InodeTable) GetParentID(id uint64) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	path := t.idToPath[id]
	if path == "" {
		return 1 // parentOfRoot
	}

	// Find parent path
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			parentPath := path[:i]
			if parentID, ok := t.pathToID[parentPath]; ok {
				return parentID
			}
			break
		}
	}

	return RootInodeID
}

// UnbindSubtree detaches every inode belonging to the named remote -- its own
// root and everything cached below it -- from its vfs.Node, and returns their
// IDs so the caller can close whatever is still open on them.
//
// Paths in this table are remote-relative with the remote name as the first
// component ("drive", "drive/sub/file.txt"), which is what makes the subtree
// selectable by prefix.
//
// The path<->id mapping is deliberately KEPT. Only the node binding goes, so
// every id in the subtree resolves to nil while the remote is absent and the
// operations report ENOENT -- which is the right answer for a path under a
// remote that is not currently exposed.
//
// Dropping the mapping instead is a bug, and a subtle one: ids come from a
// monotonic counter and are never reissued, so a remote that was removed and
// re-added came back under a NEW id. FSKit treats the advertised id as the
// item's identity (see ESItem on the Swift side), so a re-added remote's name
// appeared in readdir while every lookup of it failed ENOENT -- the directory
// listing and the lookup disagreed about what "Google" was. Keeping the
// mapping means a re-added remote is the same item it was before, at every
// level of its subtree.
func (t *InodeTable) UnbindSubtree(remote string) []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	prefix := remote + "/"
	unbound := make([]uint64, 0)
	for id, path := range t.idToPath {
		if path != remote && !strings.HasPrefix(path, prefix) {
			continue
		}
		unbound = append(unbound, id)
		delete(t.idToNode, id)
		delete(t.stale, id)
	}
	return unbound
}

// Rename moves the inode at oldPath, and everything cached below it, to
// newPath, and unbinds whatever was at newPath and below it.
//
// Identity on rename used to come out wrong in a way that only showed with
// time. Ids are keyed by path, so after `mv incoming target` a lookup of
// target hit the path's existing entry and handed the renamed node the
// OVERWRITTEN item's id. The FSKit side then addressed the renamed file by
// that id -- and when FSKit reclaimed the overwritten item, as it does some
// time after any rename over it, Remove dropped the id and every operation
// on the renamed file failed ENOENT: `mv` onto a file that had itself been
// renamed onto, a Cocoa safe-save's backup, Sami's tag write. Whether it
// failed depended on when the reclaim landed, so the harness passed on luck.
//
// Now the renamed node keeps its own id under its new path, as POSIX wants,
// and the overwritten item's id answers ENOENT until its reclaim. Its path
// entries stay until then, which is why Remove checks it still owns a path
// before deleting the path's mapping.
func (t *InodeTable) Rename(oldPath, newPath string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	newPrefix := newPath + "/"
	for id, p := range t.idToPath {
		if p == newPath || strings.HasPrefix(p, newPrefix) {
			delete(t.idToNode, id)
		}
	}
	oldPrefix := oldPath + "/"
	for id, p := range t.idToPath {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			if _, bound := t.idToNode[id]; !bound {
				continue
			}
			moved := newPath + p[len(oldPath):]
			t.idToPath[id] = moved
			t.pathToID[moved] = id
			if t.pathToID[p] == id {
				delete(t.pathToID, p)
			}
		}
	}
}

// Remove removes an inode from the table.
func (t *InodeTable) Remove(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if path, ok := t.idToPath[id]; ok && t.pathToID[path] == id {
		delete(t.pathToID, path)
	}
	delete(t.idToNode, id)
	delete(t.idToPath, id)
	delete(t.stale, id)
}

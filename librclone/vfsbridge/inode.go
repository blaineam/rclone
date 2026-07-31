package vfsbridge

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
}

// NewInodeTable creates a new inode table.
func NewInodeTable() *InodeTable {
	t := &InodeTable{
		idToNode: make(map[uint64]vfs.Node),
		idToPath: make(map[uint64]string),
		pathToID: make(map[string]uint64),
	}
	// Start after reserved IDs
	t.nextID.Store(RootInodeID + 1)
	return t
}

// GetOrCreateRootInfo creates the root ItemInfo for the FSKit activate call.
func (t *InodeTable) GetOrCreateRootInfo() ItemInfo {
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
		ModTime:  time.Now().Unix(),
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
	defer t.mu.RUnlock()
	return t.idToNode[id]
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

// RemoveSubtree forgets every inode belonging to the named remote -- its own
// root and everything cached below it -- and returns their IDs so the caller
// can close whatever is still open on them.
//
// Paths in this table are remote-relative with the remote name as the first
// component ("drive", "drive/sub/file.txt"), which is what makes the subtree
// selectable by prefix.
func (t *InodeTable) RemoveSubtree(remote string) []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	prefix := remote + "/"
	removed := make([]uint64, 0)
	for id, path := range t.idToPath {
		if path != remote && !strings.HasPrefix(path, prefix) {
			continue
		}
		removed = append(removed, id)
		delete(t.pathToID, path)
		delete(t.idToNode, id)
		delete(t.idToPath, id)
	}
	return removed
}

// Remove removes an inode from the table.
func (t *InodeTable) Remove(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if path, ok := t.idToPath[id]; ok {
		delete(t.pathToID, path)
	}
	delete(t.idToNode, id)
	delete(t.idToPath, id)
}

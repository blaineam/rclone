package vfsbridge

import (
	"sync"

	"github.com/rclone/rclone/vfs"
)

// handleEntry is one open file handle and whether it was opened for writing.
type handleEntry struct {
	h     vfs.Handle
	write bool
	// written is how many bytes have gone through this handle. FSKit gives
	// the bridge no handle identity -- an open, a write and a close name only
	// the item -- so when an item is open for writing more than once the
	// bridge has to decide which handle a write feeds and which one a close
	// releases; a handle that has carried data is the one to keep.
	written int64
	// lazy marks a read handle the bridge opened itself to serve a read that
	// arrived with no handle open (see doRead). Nothing on the FSKit side
	// refers to it, so the bridge may close it whenever that helps.
	lazy bool
	// ra tracks this handle's read pattern to size its read-ahead (readahead.go).
	ra readAhead
}

// HandleTable manages open file handles, keyed by item ID.
//
// **An item can be open more than once at a time, and this table used to
// assume it could not.** It held a single `vfs.Handle` per item ID and `Put`
// overwrote it, so the moment anything opened an item twice — a read alongside
// a write, or the read-modify-write that a Cocoa safe-save performs — the first
// handle was dropped on the floor with no reference left to close it.
//
// Two serious failures came out of that, and they looked unrelated:
//
//   - **Silent data loss.** A leaked WRITE handle is never closed, so rclone's
//     VFS never flushes it and never queues the upload. The write was
//     acknowledged, reading back through the mount showed the new bytes (they
//     were in the cache), and the backing store still held the old content.
//     Only a ground-truth read off the backing store catches that.
//   - **A wedged volume.** The file also stays open in the VFS, so a later
//     rename onto that destination blocks waiting for writers that will never
//     finish — which hung the volume and everything queued behind it.
//
// So handles are now held as a list per item, and reads and writes select a
// handle that can actually service them rather than taking whichever one
// happened to be stored last.
type HandleTable struct {
	mu      sync.RWMutex
	handles map[uint64][]*handleEntry
}

// NewHandleTable creates a new handle table.
func NewHandleTable() *HandleTable {
	return &HandleTable{
		handles: make(map[uint64][]*handleEntry),
	}
}

// Put records a newly opened handle for the given item ID.
func (t *HandleTable) Put(itemID uint64, handle vfs.Handle, write bool) *handleEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := &handleEntry{h: handle, write: write}
	t.handles[itemID] = append(t.handles[itemID], e)
	return e
}

// PutLazy records a read handle the bridge opened on its own behalf.
func (t *HandleTable) PutLazy(itemID uint64, handle vfs.Handle) *handleEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := &handleEntry{h: handle, lazy: true}
	t.handles[itemID] = append(t.handles[itemID], e)
	return e
}

// PopLazy removes and returns the lazily opened read handles of an item,
// leaving every handle FSKit opened in place.
func (t *HandleTable) PopLazy(itemID uint64) []vfs.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.handles[itemID]
	var out []vfs.Handle
	kept := entries[:0]
	for _, e := range entries {
		if e.lazy {
			out = append(out, e.h)
		} else {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		delete(t.handles, itemID)
	} else {
		t.handles[itemID] = kept
	}
	return out
}

// GetForWrite returns a handle able to service a write of n bytes at offset,
// and records the write against it.
//
// Among the handles opened for writing: a streaming handle (vfs.WriteFileHandle)
// whose position is exactly offset comes first, because a stream can only
// take the next bytes and a copy must keep feeding the stream it started on
// when the kernel opens the file a second time; then the newest write handle;
// then, for callers that opened once and wrote through it, any handle.
//
// Measured on the harness (2026-10-23): the kernel opened a new file for
// writing twice before its first write, the data went to the newest handle,
// and the close of the OTHER descriptor popped that handle (PopForClose), so
// the next write reached the idle first stream at the wrong offset and failed
// EIO. Sami's copy-beside fallback died the same way on the live volume.
func (t *HandleTable) GetForWrite(itemID uint64, offset int64, n int) vfs.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.handles[itemID]
	var pick *handleEntry
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if !e.write {
			continue
		}
		if pick == nil {
			pick = e
		}
		if w, ok := e.h.(*vfs.WriteFileHandle); ok && w.Offset() == offset {
			pick = e
			break
		}
	}
	if pick == nil && len(entries) > 0 {
		pick = entries[len(entries)-1]
	}
	if pick == nil {
		return nil
	}
	pick.written += int64(n)
	return pick.h
}

// GetForRead returns any handle able to service a read.
func (t *HandleTable) GetForRead(itemID uint64) vfs.Handle {
	if e := t.getForRead(itemID); e != nil {
		return e.h
	}
	return nil
}

// getForRead returns the entry GetForRead would pick, with its read-ahead state.
//
// While an in-place edit (vfs.RangeFileHandle) is open on the item, reads go
// through it: it reads the live object on the remote, so a reader sees the
// bytes written so far. Any other read handle reads through its own buffer,
// which may hold bytes from before a patch.
func (t *HandleTable) getForRead(itemID uint64) *handleEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entries := t.handles[itemID]
	if len(entries) == 0 {
		return nil
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if _, ok := entries[i].h.(*vfs.RangeFileHandle); ok {
			return entries[i]
		}
	}
	return entries[len(entries)-1]
}

// PopForClose removes and returns the handle a close of the item refers to.
// Returns nil when none is left. write is whether that handle was opened for
// writing -- used by doClose to decide whether to drain the writeback queue.
// idleDuplicate is set when the handle carried no data and another write
// handle for the item stays open: the real stream continues elsewhere, so
// this one must be dropped without touching the remote (an O_TRUNC stream
// closed normally would upload an empty file over the copy in flight).
//
// FSKit's close names only the item, so with several handles open the bridge
// chooses: the newest handle FSKit opened that has carried no data goes
// first (a duplicate open the kernel made and is now dropping), then the
// newest handle FSKit opened, then the newest of all. A handle that has
// carried data is kept for the last close, so an in-flight stream is never
// finished early by the close of a descriptor that wrote nothing (see
// GetForWrite). The bridge's own lazy read handles are released last; doRead
// reopens one whenever a read needs it.
func (t *HandleTable) PopForClose(itemID uint64) (h vfs.Handle, write bool, idleDuplicate bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.handles[itemID]
	if len(entries) == 0 {
		return nil, false, false
	}
	pick := -1
	for i := len(entries) - 1; i >= 0 && pick < 0; i-- {
		if !entries[i].lazy && entries[i].written == 0 {
			pick = i
		}
	}
	for i := len(entries) - 1; i >= 0 && pick < 0; i-- {
		if !entries[i].lazy {
			pick = i
		}
	}
	if pick < 0 {
		pick = len(entries) - 1
	}
	e := entries[pick]
	if len(entries) == 1 {
		delete(t.handles, itemID)
	} else {
		t.handles[itemID] = append(entries[:pick:pick], entries[pick+1:]...)
	}
	if e.write && e.written == 0 {
		for _, other := range t.handles[itemID] {
			if other.write {
				idleDuplicate = true
				break
			}
		}
	}
	return e.h, e.write, idleDuplicate
}

// PopAll removes and returns every handle still open for an item.
//
// The safety net for the leak described on `HandleTable`: reclaim is the point
// at which FSKit guarantees nothing references the item any more, so anything
// still open here was never closed and never will be. Closing them at reclaim
// is what stops a missed close turning into an un-uploaded file or a rename
// that blocks forever.
func (t *HandleTable) PopAll(itemID uint64) []vfs.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.handles[itemID]
	delete(t.handles, itemID)
	out := make([]vfs.Handle, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.h)
	}
	return out
}

// PopEverything removes and returns every handle in the table, for every
// item. Used by Server.Stop, which must release open handles before it can
// drain uploads.
func (t *HandleTable) PopEverything() []vfs.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []vfs.Handle
	for id, entries := range t.handles {
		for _, e := range entries {
			out = append(out, e.h)
		}
		delete(t.handles, id)
	}
	return out
}

// SyncInPlace flushes every open in-place edit (vfs.RangeFileHandle) to its
// remote and returns the first error. Other handles are left alone: a
// streaming write handle has nothing to flush short of closing, and a cached
// one is flushed by the upload queue.
func (t *HandleTable) SyncInPlace() error {
	t.mu.RLock()
	var handles []*vfs.RangeFileHandle
	for _, entries := range t.handles {
		for _, e := range entries {
			if rh, ok := e.h.(*vfs.RangeFileHandle); ok {
				handles = append(handles, rh)
			}
		}
	}
	t.mu.RUnlock()
	var first error
	for _, rh := range handles {
		if err := rh.Sync(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// HasWriter reports whether any handle for this item was opened for writing.
func (t *HandleTable) HasWriter(itemID uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, e := range t.handles[itemID] {
		if e.write {
			return true
		}
	}
	return false
}

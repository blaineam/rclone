package vfsbridge

import (
	"sync"

	"github.com/rclone/rclone/vfs"
)

// handleEntry is one open file handle and whether it was opened for writing.
type handleEntry struct {
	h     vfs.Handle
	write bool
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
	handles map[uint64][]handleEntry
}

// NewHandleTable creates a new handle table.
func NewHandleTable() *HandleTable {
	return &HandleTable{
		handles: make(map[uint64][]handleEntry),
	}
}

// Put records a newly opened handle for the given item ID.
func (t *HandleTable) Put(itemID uint64, handle vfs.Handle, write bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handles[itemID] = append(t.handles[itemID], handleEntry{h: handle, write: write})
}

// GetForWrite returns a handle able to service a write, preferring one opened
// for writing. Falling back to any handle preserves the previous behaviour for
// callers that opened once and wrote through it.
func (t *HandleTable) GetForWrite(itemID uint64) vfs.Handle {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entries := t.handles[itemID]
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].write {
			return entries[i].h
		}
	}
	if len(entries) > 0 {
		return entries[len(entries)-1].h
	}
	return nil
}

// GetForRead returns any handle able to service a read.
func (t *HandleTable) GetForRead(itemID uint64) vfs.Handle {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entries := t.handles[itemID]
	if len(entries) == 0 {
		return nil
	}
	return entries[len(entries)-1].h
}

// PopLast removes and returns the most recently opened handle for an item,
// which is the one a matching close refers to. Returns nil when none is left.
func (t *HandleTable) PopLast(itemID uint64) vfs.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.handles[itemID]
	if len(entries) == 0 {
		return nil
	}
	last := entries[len(entries)-1]
	if len(entries) == 1 {
		delete(t.handles, itemID)
	} else {
		t.handles[itemID] = entries[:len(entries)-1]
	}
	return last.h
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

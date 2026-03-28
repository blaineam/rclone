package vfsbridge

import (
	"sync"

	"github.com/rclone/rclone/vfs"
)

// HandleTable manages open file handles keyed by item ID.
type HandleTable struct {
	mu      sync.RWMutex
	handles map[uint64]vfs.Handle
}

// NewHandleTable creates a new handle table.
func NewHandleTable() *HandleTable {
	return &HandleTable{
		handles: make(map[uint64]vfs.Handle),
	}
}

// Put stores a handle for the given item ID.
func (t *HandleTable) Put(itemID uint64, handle vfs.Handle) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handles[itemID] = handle
}

// Get returns the handle for the given item ID.
func (t *HandleTable) Get(itemID uint64) vfs.Handle {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.handles[itemID]
}

// Remove removes and returns the handle for the given item ID.
func (t *HandleTable) Remove(itemID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handles, itemID)
}

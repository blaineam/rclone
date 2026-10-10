package vfsbridge

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// promptClose is well under closeFlushTimeout (8s): a write-close that waited
// on readers took the whole 8s, one that waits only on its own upload takes
// milliseconds against a local backing directory.
const promptClose = 2 * time.Second

// A write-close must not wait for files that are merely open for reading.
// WaitForWriters used to count every open cache item, so with readers open
// anywhere on the volume -- explicit opens, or the read handles doRead opens
// lazily and keeps until reclaim -- every write-close stalled the full 8s, and
// so did every request queued behind it on that connection.
func TestWriteCloseWithOpenReadersIsPrompt(t *testing.T) {
	s, backingA, backingB := newTestServer(t)
	writeFile(t, backingA, "reader-explicit.txt", "read me")
	writeFile(t, backingA, "reader-lazy.txt", "read me lazily")
	writeFile(t, backingB, "reader-other-remote.txt", "elsewhere")
	dirA := remoteRootID(t, s, "rem_a")
	dirB := remoteRootID(t, s, "rem_b")

	// Readers on both remotes, explicit and lazy, all left open.
	explicitID := childID(t, s, dirA, "reader-explicit.txt")
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": explicitID, "write": false})), "open reader")
	mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": explicitID, "offset": 0, "length": 4})), "read")
	lazyID := childID(t, s, dirA, "reader-lazy.txt")
	mustOK(t, s.handleRequest(req(t, 3, "read", map[string]any{"itemId": lazyID, "offset": 0, "length": 4})), "lazy read")
	otherID := childID(t, s, dirB, "reader-other-remote.txt")
	mustOK(t, s.handleRequest(req(t, 4, "open", map[string]any{"itemId": otherID, "write": false})), "open reader B")
	mustOK(t, s.handleRequest(req(t, 5, "read", map[string]any{"itemId": otherID, "offset": 0, "length": 4})), "read B")

	for i, name := range []string{"one.txt", "two.txt", "three.txt"} {
		r := mustOK(t, s.handleRequest(req(t, 10, "create",
			map[string]any{"dirId": dirA, "name": name, "isDir": false})), "create")
		id := r.Result.(ItemInfo).ID
		mustOK(t, s.handleRequest(req(t, 11, "open", map[string]any{"itemId": id, "write": true})), "open w")
		mustOK(t, s.handleRequest(req(t, 12, "write",
			map[string]any{"itemId": id, "offset": 0, "data": []byte(name)})), "write")

		start := time.Now()
		mustOK(t, s.handleRequest(req(t, 13, "close", map[string]any{"itemId": id})), "close w")
		if d := time.Since(start); d > promptClose {
			t.Fatalf("write-close #%d with readers open took %v (closeFlushTimeout is %v)", i+1, d, closeFlushTimeout)
		}
		// Prompt AND durable: the bytes are on the backing store already.
		if got, err := os.ReadFile(filepath.Join(backingA, name)); err != nil || string(got) != name {
			t.Fatalf("after close backing store has %q, %v", got, err)
		}
	}

	// The durability points ignore readers too.
	start := time.Now()
	mustOK(t, s.handleRequest(rawReq(20, "sync", "")), "sync")
	if d := time.Since(start); d > promptClose {
		t.Fatalf("sync with only readers open took %v", d)
	}

	// The readers were left alone.
	if s.handles.GetForRead(explicitID) == nil || s.handles.GetForRead(otherID) == nil {
		t.Fatal("a write-close closed someone else's read handle")
	}
}

// rclone stores a dirty file only when its LAST handle closes. A lazy read
// handle on the very file being written must therefore not hold the write in
// the cache past the close that reported it durable.
func TestWriteCloseIsDurableDespiteLazyReaderOnSameFile(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "old contents")
	dirA := remoteRootID(t, s, "rem_a")
	id := childID(t, s, dirA, "f.txt")

	// A read with no open (the mmap paging path) leaves a lazy handle.
	mustOK(t, s.handleRequest(req(t, 1, "read", map[string]any{"itemId": id, "offset": 0, "length": 3})), "lazy read")
	if s.handles.GetForRead(id) == nil {
		t.Fatal("no lazy handle")
	}

	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open w")
	mustOK(t, s.handleRequest(req(t, 3, "write",
		map[string]any{"itemId": id, "offset": 0, "data": []byte("NEW")})), "write")
	start := time.Now()
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close w")
	if d := time.Since(start); d > promptClose {
		t.Fatalf("write-close took %v", d)
	}
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "NEW contents" {
		t.Fatalf("after close backing store has %q, want the write", got)
	}

	// Reads still work afterwards: a fresh lazy handle is opened.
	r := mustOK(t, s.handleRequest(req(t, 5, "read", map[string]any{"itemId": id, "offset": 0, "length": 12})), "read after")
	if got := string(r.Result.([]byte)); got != "NEW contents" {
		t.Fatalf("read after write = %q", got)
	}
}

// WaitForWriters still waits for a file open for WRITING, and gives up at
// its timeout rather than hanging.
func TestWaitForWritersWaitsOnWritersOnly(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "r.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	rid := childID(t, s, dirA, "r.txt")
	mustOK(t, s.handleRequest(req(t, 1, "read", map[string]any{"itemId": rid, "offset": 0, "length": 1})), "lazy read")

	start := time.Now()
	s.WaitForWriters(5 * time.Second)
	if d := time.Since(start); d > promptClose {
		t.Fatalf("WaitForWriters with only a reader open took %v", d)
	}

	r := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dirA, "name": "w.txt", "isDir": false})), "create")
	wid := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 3, "open", map[string]any{"itemId": wid, "write": true})), "open w")
	const budget = 300 * time.Millisecond
	start = time.Now()
	s.WaitForWriters(budget)
	if d := time.Since(start); d < budget {
		t.Fatalf("WaitForWriters returned after %v with a writer open, want it to wait %v", d, budget)
	}
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": wid})), "close w")
}

func TestHandleTablePopLazy(t *testing.T) {
	ht := NewHandleTable()
	if got := ht.PopLazy(1); len(got) != 0 {
		t.Fatalf("PopLazy on empty table = %v", got)
	}
	ht.PutLazy(1, nil)
	if got := ht.PopLazy(1); len(got) != 1 {
		t.Fatalf("PopLazy = %d handles, want 1", len(got))
	}
	if ht.GetForRead(1) != nil || len(ht.handles) != 0 {
		t.Fatal("PopLazy left the emptied item behind")
	}
}

package vfsbridge

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A remote rebuilt in place keeps every id the client holds, and no lookup
// under it answers not-found while the rebuild runs: the volume above never
// sees the remote go away.
func TestRefreshRemoteKeepsIdsAndNeverAnswersNotFound(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	if err := os.WriteFile(filepath.Join(backingA, "film.bin"), []byte("film"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(backingA, "Movies"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backingA, "Movies", "a.mp4"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	dirA := remoteRootID(t, s, "rem_a")
	filmID := childID(t, s, dirA, "film.bin")
	moviesID := childID(t, s, dirA, "Movies")
	aID := childID(t, s, moviesID, "a.mp4")
	s.mu.RLock()
	oldVFS := s.vfses["rem_a"]
	s.mu.RUnlock()

	// Hammer the paths the client already holds while the remote is rebuilt.
	var notFound, other, ok atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	hammer := func(req func() *Response) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r := req()
			switch r.Error {
			case 0:
				ok.Add(1)
			case cENOENT:
				notFound.Add(1)
			default:
				other.Add(1)
			}
		}
	}
	wg.Add(4)
	go hammer(func() *Response {
		return s.handleRequest(req(t, 10, "lookup", map[string]any{"dirId": dirA, "name": "Movies"}))
	})
	go hammer(func() *Response {
		return s.handleRequest(req(t, 11, "lookup", map[string]any{"dirId": moviesID, "name": "a.mp4"}))
	})
	go hammer(func() *Response {
		return s.handleRequest(req(t, 12, "getattr", map[string]any{"itemId": aID}))
	})
	go hammer(func() *Response {
		return s.handleRequest(req(t, 13, "readdir", map[string]any{"dirId": moviesID}))
	})
	time.Sleep(50 * time.Millisecond)

	// A file that appears on the backend before the rebuild shows up after it.
	if err := os.WriteFile(filepath.Join(backingA, "Movies", "b.mp4"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshRemote("rem_a"); err != nil {
		t.Fatalf("RefreshRemote: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := notFound.Load(); n != 0 {
		t.Fatalf("%d not-found answers during the rebuild (ok=%d other=%d)", n, ok.Load(), other.Load())
	}
	if n := other.Load(); n != 0 {
		t.Fatalf("%d other errors during the rebuild (ok=%d)", n, ok.Load())
	}
	s.mu.RLock()
	newVFS := s.vfses["rem_a"]
	s.mu.RUnlock()
	if newVFS == oldVFS {
		t.Fatal("the VFS was not replaced")
	}

	// The ids the client holds still resolve, on the new VFS.
	for _, c := range []struct {
		name string
		id   uint64
	}{{"film", filmID}, {"Movies", moviesID}, {"a.mp4", aID}} {
		r := mustOK(t, s.handleRequest(req(t, 20, "getattr", map[string]any{"itemId": c.id})), "getattr "+c.name)
		if r.Result.(ItemInfo).ID != c.id {
			t.Fatalf("%s: id %d answered as %d", c.name, c.id, r.Result.(ItemInfo).ID)
		}
	}
	if got := childID(t, s, moviesID, "a.mp4"); got != aID {
		t.Fatalf("a.mp4 resolved to a new id %d, want %d", got, aID)
	}
	if s.inodes.GetNode(aID).VFS() != newVFS {
		t.Fatal("a.mp4 still resolves on the old VFS")
	}
	// The new listing is the current one.
	childID(t, s, moviesID, "b.mp4")
	// And a reclaim of a stale id is clean.
	mustOK(t, s.handleRequest(req(t, 21, "reclaim", map[string]any{"itemId": filmID})), "reclaim")
}

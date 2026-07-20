package vfsbridge

// Regression tests for the handle-table leak.
//
// An item can be open more than once at a time. The table used to hold one
// handle per item ID, so a second open silently discarded the first with no
// reference left to close it. A leaked WRITE handle is never flushed, so the
// write is acknowledged, reads back through the cache show the new bytes, and
// the backing store still holds the old content. These tests assert against the
// backing store on disk, which is the only thing that catches that.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/alias"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
)

func newTestServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	tmp := t.TempDir()
	backingA := filepath.Join(tmp, "a")
	backingB := filepath.Join(tmp, "b")
	for _, d := range []string{backingA, backingB} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	confPath := filepath.Join(tmp, "rclone.conf")
	conf := "[rem_a]\ntype = alias\nremote = " + backingA + "\n\n" +
		"[rem_b]\ntype = alias\nremote = " + backingB + "\n"
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()

	s := NewServer()
	// Two remotes: the aggregate-root path is where every bug on this branch
	// has lived, so never test with one.
	for _, name := range []string{"rem_a", "rem_b"} {
		if err := s.AddRemote(name); err != nil {
			t.Fatalf("AddRemote %s: %v", name, err)
		}
	}
	t.Cleanup(s.Stop)
	return s, backingA, backingB
}

func req(t *testing.T, id uint64, op string, args any) *Request {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return &Request{ID: id, Op: op, Args: raw}
}

func mustOK(t *testing.T, r *Response, what string) *Response {
	t.Helper()
	if !r.Ok {
		t.Fatalf("%s failed: errno=%d", what, r.Error)
	}
	return r
}

// remoteRootID resolves a remote's directory id through the synthetic root.
func remoteRootID(t *testing.T, s *Server, name string) uint64 {
	t.Helper()
	r := mustOK(t, s.handleRequest(req(t, 1, "lookup",
		map[string]any{"dirId": RootInodeID, "name": name})), "lookup remote")
	info := r.Result.(ItemInfo)
	return info.ID
}

// TestDoubleOpenWriteIsNotLost is the data-loss regression: open an item twice
// for writing, write through it, close both, and require the bytes to be on the
// backing store. Before the fix the first handle was discarded on the second
// open and its data never left the cache.
func TestDoubleOpenWriteIsNotLost(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirID := remoteRootID(t, s, "rem_a")

	r := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dirID, "name": "f.txt", "isDir": false})), "create")
	fileID := r.Result.(ItemInfo).ID

	// Open TWICE for writing before either close — the shape a safe-save and a
	// create-write-read both produce.
	mustOK(t, s.handleRequest(req(t, 3, "open",
		map[string]any{"itemId": fileID, "write": true})), "open#1")
	mustOK(t, s.handleRequest(req(t, 4, "open",
		map[string]any{"itemId": fileID, "write": true})), "open#2")

	payload := []byte("durable-content")
	mustOK(t, s.handleRequest(req(t, 5, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": payload})), "write")

	mustOK(t, s.handleRequest(req(t, 6, "close", map[string]any{"itemId": fileID})), "close#1")
	mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": fileID})), "close#2")
	mustOK(t, s.handleRequest(req(t, 8, "reclaim", map[string]any{"itemId": fileID})), "reclaim")

	s.WaitForWriters(20 * time.Second)

	got, err := os.ReadFile(filepath.Join(backingA, "f.txt"))
	if err != nil {
		t.Fatalf("file never reached the backing store: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("backing store holds %q, want %q — the write was acknowledged but not stored",
			got, payload)
	}
}

// TestReclaimClosesLeakedHandle covers the safety net: if a close never arrives,
// reclaim must still close the handle. Otherwise the file stays open in the VFS
// and any later rename onto it blocks forever.
func TestReclaimClosesLeakedHandle(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirID := remoteRootID(t, s, "rem_a")

	r := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dirID, "name": "g.txt", "isDir": false})), "create")
	fileID := r.Result.(ItemInfo).ID

	mustOK(t, s.handleRequest(req(t, 3, "open",
		map[string]any{"itemId": fileID, "write": true})), "open")
	mustOK(t, s.handleRequest(req(t, 4, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": []byte("no-close")})), "write")

	// No close at all — straight to reclaim.
	mustOK(t, s.handleRequest(req(t, 5, "reclaim", map[string]any{"itemId": fileID})), "reclaim")
	s.WaitForWriters(20 * time.Second)

	got, err := os.ReadFile(filepath.Join(backingA, "g.txt"))
	if err != nil {
		t.Fatalf("reclaim did not flush the leaked handle: %v", err)
	}
	if string(got) != "no-close" {
		t.Fatalf("backing store holds %q, want %q", got, "no-close")
	}
}

// TestRenameOntoOpenDestination is the hang: renaming onto a destination that
// still has an open handle must not block indefinitely. Bounded so a regression
// fails the test instead of hanging the suite.
func TestRenameOntoOpenDestination(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirID := remoteRootID(t, s, "rem_a")

	for _, n := range []string{"src.txt", "dst.txt"} {
		mustOK(t, s.handleRequest(req(t, 2, "create",
			map[string]any{"dirId": dirID, "name": n, "isDir": false})), "create "+n)
	}
	r := mustOK(t, s.handleRequest(req(t, 3, "lookup",
		map[string]any{"dirId": dirID, "name": "dst.txt"})), "lookup dst")
	dstID := r.Result.(ItemInfo).ID

	// Leave the destination open, then rename over it.
	mustOK(t, s.handleRequest(req(t, 4, "open",
		map[string]any{"itemId": dstID, "write": true})), "open dst")
	mustOK(t, s.handleRequest(req(t, 5, "close", map[string]any{"itemId": dstID})), "close dst")

	done := make(chan *Response, 1)
	go func() {
		done <- s.handleRequest(req(t, 6, "rename", map[string]any{
			"srcDirId": dirID, "srcName": "src.txt",
			"dstDirId": dirID, "dstName": "dst.txt",
		}))
	}()
	select {
	case resp := <-done:
		mustOK(t, resp, "rename onto existing")
	case <-time.After(20 * time.Second):
		t.Fatal("rename onto an existing destination BLOCKED — this is the volume wedge")
	}

	if _, err := os.Stat(filepath.Join(backingA, "src.txt")); !os.IsNotExist(err) {
		t.Fatal("source still present after rename")
	}
}

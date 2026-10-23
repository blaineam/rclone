package vfsbridge

import (
	"os"
	"path/filepath"
	"testing"
)

func writeViaBridge(t *testing.T, s *Server, dir uint64, name string, data []byte) uint64 {
	t.Helper()
	id := childIDOf(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": name, "isDir": false})))
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
	mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": 0, "data": data})), "write")
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
	return id
}

// TestRenameOverKeepsTheRenamedItemsIdentity is the tag-writer's path:
// write a new file, rename it over the existing one, again and again. After
// each rename the destination name must resolve to the RENAMED item's id,
// the overwritten id must be dead, and FSKit reclaiming the overwritten item
// -- whenever that lands -- must not take the live file with it.
func TestRenameOverKeepsTheRenamedItemsIdentity(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dir := remoteRootID(t, s, "rem_a")
	writeViaBridge(t, s, dir, "target.bin", []byte("v0"))
	overwritten := childID(t, s, dir, "target.bin")
	for n := 1; n <= 3; n++ {
		data := []byte("version " + string(rune('0'+n)))
		renamed := writeViaBridge(t, s, dir, "incoming.bin", data)
		mustOK(t, s.handleRequest(req(t, 5, "rename", map[string]any{"srcDirId": dir, "srcName": "incoming.bin", "dstDirId": dir, "dstName": "target.bin"})), "rename")

		if got := childID(t, s, dir, "target.bin"); got != renamed {
			t.Fatalf("round %d: target resolves to id %d, want the renamed item's %d (overwritten was %d)", n, got, renamed, overwritten)
		}
		if r := s.handleRequest(req(t, 6, "getattr", map[string]any{"itemId": overwritten})); r.Ok {
			t.Fatalf("round %d: the overwritten item %d still answers", n, overwritten)
		}
		if r := s.handleRequest(req(t, 7, "lookup", map[string]any{"dirId": dir, "name": "incoming.bin"})); r.Ok {
			t.Fatalf("round %d: the source name still resolves after the rename", n)
		}
		// FSKit reclaims the overwritten item some time later.
		mustOK(t, s.handleRequest(req(t, 8, "reclaim", map[string]any{"itemId": overwritten})), "reclaim")
		r := s.handleRequest(req(t, 9, "getattr", map[string]any{"itemId": renamed}))
		if !r.Ok {
			t.Fatalf("round %d: reclaiming the overwritten item %d killed the renamed file %d (errno %d)", n, overwritten, renamed, r.Error)
		}
		rd := mustOK(t, s.handleRequest(req(t, 10, "read", map[string]any{"itemId": renamed, "offset": 0, "length": 32})), "read")
		if got := string(rd.Result.([]byte)); got != string(data) {
			t.Fatalf("round %d: target reads %q, want %q", n, got, data)
		}
		if got, _ := os.ReadFile(filepath.Join(backingA, "target.bin")); string(got) != string(data) {
			t.Fatalf("round %d: backing store %q, want %q", n, got, data)
		}
		overwritten = renamed
	}
}

// TestRenameMovesTheIdsOfADirectoryTree: renaming a directory carries every
// id below it to the new path, and a lookup under the new name finds them.
func TestRenameMovesTheIdsOfADirectoryTree(t *testing.T) {
	s, _, _ := newTestServer(t)
	dir := remoteRootID(t, s, "rem_a")
	sub := childIDOf(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": "sub", "isDir": true})))
	leaf := writeViaBridge(t, s, sub, "leaf.txt", []byte("leaf"))
	mustOK(t, s.handleRequest(req(t, 2, "rename", map[string]any{"srcDirId": dir, "srcName": "sub", "dstDirId": dir, "dstName": "moved"})), "rename dir")
	if got := childID(t, s, dir, "moved"); got != sub {
		t.Fatalf("moved directory resolves to %d, want %d", got, sub)
	}
	if got := childID(t, s, sub, "leaf.txt"); got != leaf {
		t.Fatalf("leaf under the moved directory resolves to %d, want %d", got, leaf)
	}
	if r := s.handleRequest(req(t, 3, "lookup", map[string]any{"dirId": dir, "name": "sub"})); r.Ok {
		t.Fatal("the old directory name still resolves")
	}
	if s.inodes.GetPath(leaf) != "rem_a/moved/leaf.txt" {
		t.Fatalf("leaf's path is %q after the rename", s.inodes.GetPath(leaf))
	}
}

package vfsbridge

// Error paths and the less-travelled ops of the bridge's RPC surface: malformed
// arguments, ids that resolve to nothing or to the wrong kind of node, POSIX
// errno mapping, the TCP framing, and server lifecycle. The happy paths live in
// the other test files; these pin what the FSKit side sees when things go
// wrong, since a wrong errno there turns into a hung or corrupted volume.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/vfs"
)

// rawReq builds a request whose args are the given raw bytes.
func rawReq(id uint64, op, args string) *Request {
	return &Request{ID: id, Op: op, Args: json.RawMessage(args)}
}

// wantErr asserts r failed with errno want.
func wantErr(t *testing.T, r *Response, want int32, what string) {
	t.Helper()
	if r.Ok {
		t.Fatalf("%s: succeeded, want errno %d", what, want)
	}
	if r.Error != want {
		t.Fatalf("%s: errno %d, want %d", what, r.Error, want)
	}
}

// writeFile puts a file straight onto a backing directory.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// childID looks name up under dirID and returns its id.
func childID(t *testing.T, s *Server, dirID uint64, name string) uint64 {
	t.Helper()
	r := mustOK(t, s.handleRequest(req(t, 1, "lookup",
		map[string]any{"dirId": dirID, "name": name})), "lookup "+name)
	return r.Result.(ItemInfo).ID
}

func TestMalformedArgsAreEINVAL(t *testing.T) {
	s, _, _ := newTestServer(t)
	for _, op := range []string{
		"getattr", "setattr", "lookup", "reclaim", "create", "remove",
		"rename", "readdir", "open", "close", "read", "write",
	} {
		for _, args := range []string{"{not json", `"a string"`, ""} {
			r := s.handleRequest(rawReq(7, op, args))
			wantErr(t, r, cEINVAL, fmt.Sprintf("%s with args %q", op, args))
			if r.ID != 7 {
				t.Fatalf("%s: reply id %d, want 7", op, r.ID)
			}
		}
	}
}

func TestUnknownOpIsENOSYS(t *testing.T) {
	s, _, _ := newTestServer(t)
	r := s.handleRequest(rawReq(9, "frobnicate", "{}"))
	wantErr(t, r, cENOSYS, "unknown op")
	if r.Gen != s.Generation() {
		t.Fatalf("error reply gen %d, want %d", r.Gen, s.Generation())
	}
}

func TestStaticOps(t *testing.T) {
	s, _, _ := newTestServer(t)

	r := mustOK(t, s.handleRequest(rawReq(1, "getResourceIdentifier", "")), "getResourceIdentifier")
	if m := r.Result.(map[string]string); m["name"] != "Enter Space" || m["containerId"] == "" {
		t.Fatalf("getResourceIdentifier = %v", m)
	}
	r = mustOK(t, s.handleRequest(rawReq(2, "getVolumeIdentifier", "")), "getVolumeIdentifier")
	if m := r.Result.(map[string]string); m["id"] != "enterspace-volume" {
		t.Fatalf("getVolumeIdentifier = %v", m)
	}
	r = mustOK(t, s.handleRequest(rawReq(3, "getVolumeBehavior", "")), "getVolumeBehavior")
	if m := r.Result.(map[string]bool); !m["xattrInhibited"] || !m["renameInhibited"] {
		t.Fatalf("getVolumeBehavior = %v", m)
	}
	r = mustOK(t, s.handleRequest(rawReq(4, "getPathConf", "")), "getPathConf")
	if m := r.Result.(map[string]int); m["maxNameLength"] != 255 || m["maxLinkCount"] != 1 {
		t.Fatalf("getPathConf = %v", m)
	}
	r = mustOK(t, s.handleRequest(rawReq(5, "getCapabilities", "")), "getCapabilities")
	if m := r.Result.(map[string]interface{}); m["persistentObjectIds"] != true || m["hardLinks"] != false {
		t.Fatalf("getCapabilities = %v", m)
	}
	mustOK(t, s.handleRequest(rawReq(6, "mount", "")), "mount")
	for _, op := range []string{"unmount", "sync", "deactivate"} {
		mustOK(t, s.handleRequest(rawReq(7, op, "")), op)
	}
	r = mustOK(t, s.handleRequest(rawReq(8, "activate", "")), "activate")
	if info := r.Result.(ItemInfo); info.ID != RootInodeID || !info.IsDir {
		t.Fatalf("activate = %+v", info)
	}
	r = mustOK(t, s.handleRequest(rawReq(9, "access", "")), "access")
	if r.Result != true {
		t.Fatalf("access = %v", r.Result)
	}
	r = mustOK(t, s.handleRequest(rawReq(10, "debugGoroutines", "")), "debugGoroutines")
	if dump, _ := r.Result.(string); !strings.Contains(dump, "goroutine") {
		t.Fatalf("debugGoroutines returned no stacks: %.80q", dump)
	}
}

// sync/unmount are durability points: a write closed just before must be on
// the backing store when they return.
func TestSyncFlushesClosedWrite(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirA := remoteRootID(t, s, "rem_a")
	r := mustOK(t, s.handleRequest(req(t, 1, "create",
		map[string]any{"dirId": dirA, "name": "synced.txt", "isDir": false})), "create")
	id := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
	mustOK(t, s.handleRequest(req(t, 3, "write",
		map[string]any{"itemId": id, "offset": 0, "data": []byte("durable")})), "write")
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
	mustOK(t, s.handleRequest(rawReq(5, "sync", "")), "sync")
	got, err := os.ReadFile(filepath.Join(backingA, "synced.txt"))
	if err != nil || string(got) != "durable" {
		t.Fatalf("after sync backing store has %q, %v", got, err)
	}
}

func TestStatFS(t *testing.T) {
	s, _, _ := newTestServer(t)
	r := mustOK(t, s.handleRequest(rawReq(1, "statfs", "")), "statfs")
	info := r.Result.(StatFSInfo)
	// Two local remotes on the same disk: both report real figures, summed.
	if info.TotalBytes == 0 || info.FreeBytes == 0 {
		t.Fatalf("statfs over local remotes reported %+v", info)
	}

	for _, name := range []string{"rem_a", "rem_b"} {
		if err := s.RemoveRemote(name); err != nil {
			t.Fatal(err)
		}
	}
	r = mustOK(t, s.handleRequest(rawReq(2, "statfs", "")), "statfs empty")
	if info := r.Result.(StatFSInfo); info != (StatFSInfo{}) {
		t.Fatalf("statfs with no remotes = %+v, want zeros", info)
	}
}

// A backend that cannot report usage (memory) must count as zero, not wrap
// -1 into a huge uint64.
func TestStatFSUnknownIsZero(t *testing.T) {
	s := newByteExactServer(t, "d")
	if err := s.RemoveRemote("rem_y"); err != nil {
		t.Fatal(err)
	}
	r := mustOK(t, s.handleRequest(rawReq(1, "statfs", "")), "statfs")
	info := r.Result.(StatFSInfo)
	const huge = uint64(1) << 62
	if info.TotalBytes > huge || info.FreeBytes > huge || info.UsedBytes > huge {
		t.Fatalf("unknown usage wrapped: %+v", info)
	}
}

func TestGetAttrAndSetAttr(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "0123456789")
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")

	wantErr(t, s.handleRequest(req(t, 1, "getattr", map[string]any{"itemId": 424242})), cENOENT, "getattr unknown")
	r := mustOK(t, s.handleRequest(req(t, 2, "getattr", map[string]any{"itemId": fileID})), "getattr file")
	if info := r.Result.(ItemInfo); info.Size != 10 || info.IsDir || info.Mode != 0644 || info.ParentID != dirA {
		t.Fatalf("getattr file = %+v", info)
	}
	r = mustOK(t, s.handleRequest(req(t, 3, "getattr", map[string]any{"itemId": dirA})), "getattr dir")
	if info := r.Result.(ItemInfo); !info.IsDir || info.Mode != 0755 {
		t.Fatalf("getattr dir = %+v", info)
	}

	wantErr(t, s.handleRequest(req(t, 4, "setattr", map[string]any{"itemId": RootInodeID})), cEPERM, "setattr root")
	wantErr(t, s.handleRequest(req(t, 5, "setattr", map[string]any{"itemId": 424242})), cENOENT, "setattr unknown")

	mtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).Unix()
	// Read the whole file first so the VFS cache holds all of it: see
	// TestSetAttrTruncateOfUncachedFile for why a truncate of an uncached
	// file does not stick. The reply's size can briefly trail a truncate (the
	// VFS reports the old object's size until the re-upload lands), so the
	// size is checked after sync.
	mustOK(t, s.handleRequest(req(t, 60, "read",
		map[string]any{"itemId": fileID, "offset": 0, "length": 10})), "warm read")
	mustOK(t, s.handleRequest(req(t, 61, "close", map[string]any{"itemId": fileID})), "close warm read")
	r = mustOK(t, s.handleRequest(req(t, 6, "setattr",
		map[string]any{"itemId": fileID, "size": 4, "modTime": mtime})), "setattr truncate+mtime")
	if info := r.Result.(ItemInfo); info.ID != fileID {
		t.Fatalf("setattr result = %+v", info)
	}
	mustOK(t, s.handleRequest(rawReq(7, "sync", "")), "sync")
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "0123" {
		t.Fatalf("truncate left %q on disk", got)
	}
	r = mustOK(t, s.handleRequest(req(t, 71, "getattr", map[string]any{"itemId": fileID})), "getattr after truncate")
	if info := r.Result.(ItemInfo); info.Size != 4 {
		t.Fatalf("size after truncate+sync = %d", info.Size)
	}
	// modTime alone.
	mustOK(t, s.handleRequest(req(t, 72, "setattr",
		map[string]any{"itemId": fileID, "modTime": mtime + 60})), "setattr mtime")

	// Truncating a directory is refused by the VFS and mapped, not swallowed.
	r = s.handleRequest(req(t, 8, "setattr", map[string]any{"itemId": dirA, "size": 0}))
	if r.Ok {
		t.Fatal("truncating a directory succeeded")
	}
}

// TestSetAttrTruncateOfUncachedFile documents an UPSTREAM rclone VFS bug
// (vfs/vfscache, unmodified by this fork) that the bridge's setattr(size)
// inherits: shrinking a file whose bytes are not in the VFS cache is undone on
// close. Item.Truncate clips the cached ranges to [0,size), then
// _actualClose's _ensure(0,size) starts a downloader that writes the remote's
// full content back over the truncated cache file, and that is what gets
// uploaded. Reproduces with plain vfs.New + File.Truncate, any ReadAhead.
// Skipped until fixed upstream (or worked around in doSetAttr); unskip to
// check.
func TestSetAttrTruncateOfUncachedFile(t *testing.T) {
	t.Skip("known upstream vfscache bug: truncate of an uncached file is re-extended on close")
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "0123456789")
	fileID := childID(t, s, remoteRootID(t, s, "rem_a"), "f.txt")
	mustOK(t, s.handleRequest(req(t, 1, "setattr", map[string]any{"itemId": fileID, "size": 4})), "setattr")
	mustOK(t, s.handleRequest(rawReq(2, "sync", "")), "sync")
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "0123" {
		t.Fatalf("truncate left %q on disk", got)
	}
}

func TestLookupErrors(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")

	wantErr(t, s.handleRequest(req(t, 1, "lookup",
		map[string]any{"dirId": RootInodeID, "name": "no_such_remote"})), cENOENT, "lookup unknown remote")
	wantErr(t, s.handleRequest(req(t, 2, "lookup",
		map[string]any{"dirId": 424242, "name": "x"})), cENOENT, "lookup in unknown dir")
	wantErr(t, s.handleRequest(req(t, 3, "lookup",
		map[string]any{"dirId": fileID, "name": "x"})), cENOTDIR, "lookup in a file")
	wantErr(t, s.handleRequest(req(t, 4, "lookup",
		map[string]any{"dirId": dirA, "name": "missing"})), cENOENT, "lookup missing child")
}

func TestCreateErrorsAndSuccess(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")

	create := func(dirID uint64, name string, isDir bool) *Response {
		return s.handleRequest(req(t, 1, "create",
			map[string]any{"dirId": dirID, "name": name, "isDir": isDir}))
	}
	wantErr(t, create(dirA, ".DS_Store", false), cEPERM, "create .DS_Store")
	wantErr(t, create(RootInodeID, "newremote", true), cEPERM, "create in root")
	wantErr(t, create(424242, "x", false), cENOENT, "create in unknown dir")
	wantErr(t, create(fileID, "x", false), cENOTDIR, "create in a file")

	// A directory called .DS_Store is not Finder scratch.
	mustOK(t, create(dirA, ".DS_Store", true), "mkdir .DS_Store")

	r := mustOK(t, create(dirA, "sub", true), "mkdir")
	if info := r.Result.(ItemInfo); !info.IsDir || info.Name != "sub" {
		t.Fatalf("mkdir result %+v", info)
	}
	if st, err := os.Stat(filepath.Join(backingA, "sub")); err != nil || !st.IsDir() {
		t.Fatalf("mkdir did not reach disk: %v", err)
	}
	// mkdir over an existing FILE is EEXIST. (Over an existing directory the
	// VFS returns that directory; the kernel's own lookup answers EEXIST
	// before a create of an existing name ever reaches the volume.)
	wantErr(t, create(dirA, "f.txt", true), cEEXIST, "mkdir over a file")

	r = mustOK(t, create(dirA, "new.txt", false), "create file")
	if info := r.Result.(ItemInfo); info.IsDir || info.Size != 0 {
		t.Fatalf("create file result %+v", info)
	}
}

func TestRemoveErrorsAndSuccess(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "full/inner.txt", "x")
	writeFile(t, backingA, "f.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	fullID := childID(t, s, dirA, "full")
	fileID := childID(t, s, dirA, "f.txt")

	wantErr(t, s.handleRequest(req(t, 1, "remove", map[string]any{"itemId": RootInodeID})), cEPERM, "remove root")
	wantErr(t, s.handleRequest(req(t, 2, "remove", map[string]any{"itemId": 424242})), cENOENT, "remove unknown")
	wantErr(t, s.handleRequest(req(t, 3, "remove", map[string]any{"itemId": fullID})), cENOTEMPTY, "remove non-empty dir")

	// Removing an item we hold open must not wait on our own handle.
	mustOK(t, s.handleRequest(req(t, 4, "open", map[string]any{"itemId": fileID})), "open")
	done := make(chan *Response, 1)
	go func() { done <- s.handleRequest(req(t, 5, "remove", map[string]any{"itemId": fileID})) }()
	select {
	case r := <-done:
		mustOK(t, r, "remove open file")
	case <-time.After(20 * time.Second):
		t.Fatal("remove of an open file blocked on its own handle")
	}
	if _, err := os.Stat(filepath.Join(backingA, "f.txt")); !os.IsNotExist(err) {
		t.Fatalf("removed file still on disk: %v", err)
	}
	// Its id is gone too.
	wantErr(t, s.handleRequest(req(t, 6, "getattr", map[string]any{"itemId": fileID})), cENOENT, "getattr removed")
}

func TestRenameErrorsAndSuccess(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "payload")
	dirA := remoteRootID(t, s, "rem_a")
	dirB := remoteRootID(t, s, "rem_b")
	fileID := childID(t, s, dirA, "f.txt")

	rename := func(src uint64, srcName string, dst uint64, dstName string) *Response {
		return s.handleRequest(req(t, 1, "rename", map[string]any{
			"srcDirId": src, "srcName": srcName, "dstDirId": dst, "dstName": dstName}))
	}
	wantErr(t, rename(RootInodeID, "rem_a", RootInodeID, "rem_z"), cEPERM, "rename a remote")
	wantErr(t, rename(dirA, "f.txt", RootInodeID, "f.txt"), cEPERM, "rename into root")
	wantErr(t, rename(424242, "f.txt", dirA, "g.txt"), cENOENT, "rename from unknown dir")
	wantErr(t, rename(dirA, "f.txt", 424242, "g.txt"), cENOENT, "rename to unknown dir")
	wantErr(t, rename(fileID, "x", dirA, "g.txt"), cENOTDIR, "rename from a file")
	wantErr(t, rename(dirA, "f.txt", dirB, "f.txt"), cEXDEV, "rename across remotes")
	wantErr(t, rename(dirA, "missing.txt", dirA, "g.txt"), cENOENT, "rename missing source")

	mustOK(t, rename(dirA, "f.txt", dirA, "g.txt"), "rename")
	if got, err := os.ReadFile(filepath.Join(backingA, "g.txt")); err != nil || string(got) != "payload" {
		t.Fatalf("renamed file: %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(backingA, "f.txt")); !os.IsNotExist(err) {
		t.Fatalf("rename left the source behind: %v", err)
	}
}

func TestReadDirErrorsAndPrefetch(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "x")
	// More subdirectories than the per-listing prefetch cap.
	for i := 0; i < prefetchMaxChildren+6; i++ {
		writeFile(t, backingA, fmt.Sprintf("d%02d/leaf.txt", i), "x")
	}
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")

	wantErr(t, s.handleRequest(req(t, 1, "readdir", map[string]any{"dirId": 424242})), cENOENT, "readdir unknown")
	wantErr(t, s.handleRequest(req(t, 2, "readdir", map[string]any{"dirId": fileID})), cENOTDIR, "readdir a file")

	names := dirNames(t, s, dirA)
	if len(names) != prefetchMaxChildren+7 {
		t.Fatalf("listed %d entries, want %d", len(names), prefetchMaxChildren+7)
	}
	// The listing's subdirectories must be resolvable and listable.
	sub := childID(t, s, dirA, "d00")
	if got := dirNames(t, s, sub); len(got) != 1 || got[0] != "leaf.txt" {
		t.Fatalf("subdir listed %v", got)
	}
	// Prefetch must release every slot it takes.
	deadline := time.Now().Add(10 * time.Second)
	for len(s.prefetchSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(s.prefetchSlots); n != 0 {
		t.Fatalf("%d prefetch slots still held", n)
	}
}

// A closed server and a full budget both drop prefetches instead of queueing.
func TestPrefetchDropsWhenClosedOrFull(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "sub/x", "x")
	v := s.vfses["rem_a"]
	root, err := v.Root()
	if err != nil {
		t.Fatal(err)
	}
	node, err := root.Stat("sub")
	if err != nil {
		t.Fatal(err)
	}
	d := node.(*vfs.Dir)

	for i := 0; i < prefetchConcurrency; i++ {
		s.prefetchSlots <- struct{}{}
	}
	s.prefetchChildren([]*vfs.Dir{d, d})
	if n := len(s.prefetchSlots); n != prefetchConcurrency {
		t.Fatalf("full budget: %d slots, want %d", n, prefetchConcurrency)
	}
	for i := 0; i < prefetchConcurrency; i++ {
		<-s.prefetchSlots
	}

	s.closed.Store(true)
	s.prefetchChildren([]*vfs.Dir{d})
	if n := len(s.prefetchSlots); n != 0 {
		t.Fatalf("closed server still prefetched (%d slots)", n)
	}
	s.closed.Store(false) // let Cleanup's Stop run its real teardown
}

func TestOpenCloseReadWriteErrors(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "hello world")
	writeFile(t, backingA, "gone.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")

	// open
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": RootInodeID})), "open root")
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": dirA})), "open dir")
	wantErr(t, s.handleRequest(req(t, 3, "open", map[string]any{"itemId": 424242})), cENOENT, "open unknown")

	// close with nothing open is fine
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": fileID})), "close unopened")

	// write with no handle
	wantErr(t, s.handleRequest(req(t, 5, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": []byte("x")})), cENOENT, "write without open")

	// read: unknown id, directory, then lazy open
	wantErr(t, s.handleRequest(req(t, 6, "read",
		map[string]any{"itemId": 424242, "offset": 0, "length": 4})), cENOENT, "read unknown")
	wantErr(t, s.handleRequest(req(t, 7, "read",
		map[string]any{"itemId": dirA, "offset": 0, "length": 4})), cEISDIR, "read a directory")

	r := mustOK(t, s.handleRequest(req(t, 8, "read",
		map[string]any{"itemId": fileID, "offset": 6, "length": 5})), "lazy read")
	if got := string(r.Result.([]byte)); got != "world" {
		t.Fatalf("lazy read = %q", got)
	}
	if s.handles.GetForRead(fileID) == nil {
		t.Fatal("lazy read did not keep its handle")
	}
	// Short read at EOF is not an error.
	r = mustOK(t, s.handleRequest(req(t, 9, "read",
		map[string]any{"itemId": fileID, "offset": 8, "length": 100})), "read past EOF")
	if got := string(r.Result.([]byte)); got != "rld" {
		t.Fatalf("read past EOF = %q", got)
	}
	mustOK(t, s.handleRequest(req(t, 10, "close", map[string]any{"itemId": fileID})), "close lazy")

	// A write through a read-only handle is refused, not silently dropped.
	mustOK(t, s.handleRequest(req(t, 11, "open", map[string]any{"itemId": fileID, "write": false})), "open ro")
	r = s.handleRequest(req(t, 12, "write", map[string]any{"itemId": fileID, "offset": 0, "data": []byte("x")}))
	if r.Ok {
		t.Fatal("write through a read-only handle succeeded")
	}
	mustOK(t, s.handleRequest(req(t, 13, "close", map[string]any{"itemId": fileID})), "close ro")

	// Write round trip.
	mustOK(t, s.handleRequest(req(t, 14, "open", map[string]any{"itemId": fileID, "write": true})), "open rw")
	r = mustOK(t, s.handleRequest(req(t, 15, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": []byte("HELLO")})), "write")
	if n := r.Result.(int); n != 5 {
		t.Fatalf("wrote %d bytes", n)
	}
	mustOK(t, s.handleRequest(req(t, 16, "close", map[string]any{"itemId": fileID})), "close rw")
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "HELLO world" {
		t.Fatalf("after write+close disk has %q", got)
	}

	// Reading a file whose backing copy has vanished fails to open lazily.
	goneID := childID(t, s, dirA, "gone.txt")
	if err := os.Remove(filepath.Join(backingA, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	r = s.handleRequest(req(t, 17, "read", map[string]any{"itemId": goneID, "offset": 0, "length": 1}))
	if r.Ok {
		t.Fatal("read of a file deleted behind the VFS succeeded")
	}
}

func TestReclaimForgetsItem(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "x")
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "f.txt")
	mustOK(t, s.handleRequest(req(t, 1, "reclaim", map[string]any{"itemId": fileID})), "reclaim")
	wantErr(t, s.handleRequest(req(t, 2, "getattr", map[string]any{"itemId": fileID})), cENOENT, "getattr reclaimed")
	// Reclaiming something unknown is harmless.
	mustOK(t, s.handleRequest(req(t, 3, "reclaim", map[string]any{"itemId": 424242})), "reclaim unknown")
}

func TestMapVFSErr(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int32
	}{
		{os.ErrNotExist, cENOENT},
		{&os.PathError{Op: "open", Path: "x", Err: os.ErrNotExist}, cENOENT},
		{os.ErrExist, cEEXIST},
		{os.ErrPermission, cEPERM},
		{vfs.ENOTEMPTY, cENOTEMPTY},
		{vfs.EROFS, cEROFS},
		{vfs.ENOSYS, cENOSYS},
		{errors.New("backend exploded"), cEIO},
	} {
		if got := mapVFSErr(tc.err); got != tc.want {
			t.Errorf("mapVFSErr(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestAddRemoteErrorsAndIdempotence(t *testing.T) {
	s, _, _ := newTestServer(t)
	gen := s.Generation()
	if err := s.AddRemote("rem_a"); err != nil {
		t.Fatalf("re-adding an exposed remote: %v", err)
	}
	if s.Generation() != gen {
		t.Fatal("re-adding an exposed remote moved the generation")
	}
	if err := s.AddRemote("not_configured"); err == nil {
		t.Fatal("AddRemote of an unconfigured remote succeeded")
	}
	if s.Generation() != gen {
		t.Fatal("a failed AddRemote moved the generation")
	}
}

// RemoveRemote closes handles still open inside the removed remote.
func TestRemoveRemoteClosesOpenHandles(t *testing.T) {
	s, _, backingB := newTestServer(t)
	writeFile(t, backingB, "open.txt", "x")
	dirB := remoteRootID(t, s, "rem_b")
	fileID := childID(t, s, dirB, "open.txt")
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": fileID})), "open ro")
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": fileID, "write": true})), "open rw")

	done := make(chan error, 1)
	go func() { done <- s.RemoveRemote("rem_b") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("RemoveRemote waited on handles only it could close")
	}
	if s.handles.GetForRead(fileID) != nil {
		t.Fatal("handles of the removed remote survived")
	}
}

// --- TCP transport ---

func sendFrame(t *testing.T, c net.Conn, payload []byte) {
	t.Helper()
	if err := writeLengthPrefixed(c, payload); err != nil {
		t.Fatal(err)
	}
}

func recvResponse(t *testing.T, c net.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	data, err := readLengthPrefixed(c)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("response not JSON: %v (%q)", err, data)
	}
	return m
}

func startBridge(t *testing.T) (*Server, string) {
	t.Helper()
	s, _, _ := newTestServer(t)
	if got := s.ListenAddr(); got != "" {
		t.Fatalf("ListenAddr before Start = %q", got)
	}
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if s.ListenAddr() != addr {
		t.Fatalf("ListenAddr %q, Start returned %q", s.ListenAddr(), addr)
	}
	return s, addr
}

func TestTCPRoundTripAndBadFrames(t *testing.T) {
	_, addr := startBridge(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Undecodable and empty frames are skipped without a reply; the
	// connection keeps serving the next valid one.
	sendFrame(t, c, []byte("{garbage"))
	sendFrame(t, c, nil)
	sendFrame(t, c, []byte(`{"id":42,"op":"getattr","args":{"itemId":2}}`))
	m := recvResponse(t, c)
	if m["id"] != float64(42) || m["ok"] != true {
		t.Fatalf("response %v", m)
	}
	if gen, _ := m["gen"].(float64); gen == 0 {
		t.Fatalf("response carried no generation: %v", m)
	}

	sendFrame(t, c, []byte(`{"id":43,"op":"nope"}`))
	m = recvResponse(t, c)
	if m["ok"] != false || m["error"] != float64(cENOSYS) {
		t.Fatalf("unknown op over TCP: %v", m)
	}
}

// An oversized length prefix drops the connection rather than allocating it.
func TestTCPOversizedFrameClosesConnection(t *testing.T) {
	_, addr := startBridge(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 64*1024*1024+1)
	if _, err := c.Write(lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("server kept the connection open after an oversized frame")
	}
}

func TestReadLengthPrefixed(t *testing.T) {
	if _, err := readLengthPrefixed(bytes.NewReader([]byte{0, 0})); err == nil {
		t.Fatal("short prefix accepted")
	}
	if _, err := readLengthPrefixed(bytes.NewReader([]byte{0, 0, 0, 5, 'a'})); err == nil {
		t.Fatal("truncated body accepted")
	}
	got, err := readLengthPrefixed(bytes.NewReader([]byte{0, 0, 0, 0}))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty frame: %q, %v", got, err)
	}
	if _, err := readLengthPrefixed(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff})); err == nil ||
		!strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized frame: %v", err)
	}
}

type failWriter struct{ after int }

func (w *failWriter) Write(p []byte) (int, error) {
	if w.after <= 0 {
		return 0, io.ErrClosedPipe
	}
	w.after--
	return len(p), nil
}

func TestWriteLengthPrefixedErrors(t *testing.T) {
	if err := writeLengthPrefixed(&failWriter{after: 0}, []byte("x")); err == nil {
		t.Fatal("prefix write error swallowed")
	}
	if err := writeLengthPrefixed(&failWriter{after: 1}, []byte("x")); err == nil {
		t.Fatal("body write error swallowed")
	}
}

func TestStartOnBusyAddressFails(t *testing.T) {
	_, addr := startBridge(t)
	other := NewServer()
	if _, err := other.Start(addr); err == nil {
		other.Stop()
		t.Fatal("second Start on a bound address succeeded")
	}
}

// Stop closes the listener, empties the remote set, and is idempotent.
func TestStopIsIdempotentAndRefusesConnections(t *testing.T) {
	s, addr := startBridge(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	sendFrame(t, c, []byte(`{"id":1,"op":"mount"}`))
	recvResponse(t, c)

	s.Stop()
	s.Stop()
	if names := s.remoteNames(); len(names) != 0 {
		t.Fatalf("remotes after Stop: %v", names)
	}
	if c2, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c2.Close()
		t.Fatal("listener still accepting after Stop")
	}
	// The open connection is dropped after at most the request it was
	// already blocked reading.
	sendFrame(t, c, []byte(`{"id":2,"op":"mount"}`))
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, _ = readLengthPrefixed(c)
	if _, err := readLengthPrefixed(c); err == nil {
		t.Fatal("stopped server kept the connection open")
	}
}

// Stop with handles still open must release them itself and flush what they
// wrote. It used to drain FIRST, and the drain counts every open cache item --
// read handles included -- so any handle left in the table (a lazily-opened
// read handle lives there until reclaim) stalled shutdown for the full 30s.
func TestStopClosesOpenHandlesAndFlushes(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "reading.txt", "r")
	dirA := remoteRootID(t, s, "rem_a")
	readID := childID(t, s, dirA, "reading.txt")
	// A lazily-opened read handle, never closed.
	mustOK(t, s.handleRequest(req(t, 1, "read",
		map[string]any{"itemId": readID, "offset": 0, "length": 1})), "lazy read")

	r := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dirA, "name": "unclosed.txt", "isDir": false})), "create")
	wID := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 3, "open", map[string]any{"itemId": wID, "write": true})), "open")
	mustOK(t, s.handleRequest(req(t, 4, "write",
		map[string]any{"itemId": wID, "offset": 0, "data": []byte("flushed at stop")})), "write")

	start := time.Now()
	s.Stop()
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("Stop took %v with handles open — it waited on handles only it could close", d)
	}
	got, err := os.ReadFile(filepath.Join(backingA, "unclosed.txt"))
	if err != nil || string(got) != "flushed at stop" {
		t.Fatalf("Stop discarded an unclosed write: %q, %v", got, err)
	}
}

func TestHandleTableHasWriter(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	writeFile(t, backingA, "f.txt", "x")
	fileID := childID(t, s, remoteRootID(t, s, "rem_a"), "f.txt")
	if s.handles.HasWriter(fileID) {
		t.Fatal("HasWriter with nothing open")
	}
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": fileID})), "open ro")
	if s.handles.HasWriter(fileID) {
		t.Fatal("HasWriter with only a reader open")
	}
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": fileID, "write": true})), "open rw")
	if !s.handles.HasWriter(fileID) {
		t.Fatal("HasWriter missed an open writer")
	}
	// Close the writer (last opened) then the reader.
	mustOK(t, s.handleRequest(req(t, 3, "close", map[string]any{"itemId": fileID})), "close rw")
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": fileID})), "close ro")
}

// With --no-unicode-normalization the caller's spelling is stored verbatim.
func TestStoredNameWithoutNormalization(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	ci := fs.GetConfig(context.Background())
	old := ci.NoUnicodeNormalization
	ci.NoUnicodeNormalization = true
	t.Cleanup(func() { ci.NoUnicodeNormalization = old })

	const nfd = "café.txt"
	dirA := remoteRootID(t, s, "rem_a")
	mustOK(t, s.handleRequest(req(t, 1, "create",
		map[string]any{"dirId": dirA, "name": nfd, "isDir": false})), "create")
	if _, err := os.Stat(filepath.Join(backingA, nfd)); err != nil {
		t.Fatalf("NFD name not stored verbatim: %v", err)
	}
}

// An unreadable subtree of the cache is skipped, not fatal to the purge.
func TestPurgeSkipsUnreadableSubtree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read anything")
	}
	prev := config.GetCacheDir()
	t.Cleanup(func() { _ = config.SetCacheDir(prev) })
	dir := t.TempDir()
	if err := config.SetCacheDir(dir); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(dir, "vfs", "locked")
	writeFile(t, locked, ".DS_Store", "x")
	writeFile(t, filepath.Join(dir, "vfs"), ".DS_Store", "x")
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	purgeCachedDesktopServicesStores() // must skip the unreadable subtree, not abort
	if _, err := os.Stat(filepath.Join(dir, "vfs", ".DS_Store")); !os.IsNotExist(err) {
		t.Fatalf("readable .DS_Store not purged: %v", err)
	}
}

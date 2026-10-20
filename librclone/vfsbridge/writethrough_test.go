package vfsbridge

// Tests for streaming ("write-through") writes: a remote that streams holds the writer to
// the upload's pace, and its close means the bytes are on the remote. A remote that cannot
// stream keeps the normal mode and says so.

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// streamingServer is newTestServer with rem_a switched to streaming writes.
func streamingServer(t *testing.T) (*Server, string) {
	t.Helper()
	s, backingA, _ := newTestServer(t)
	oldBuf := throughBufferSize
	throughBufferSize = 0 // no read-ahead, so the pacing is exact in a small test
	t.Cleanup(func() { throughBufferSize = oldBuf })

	s.SetWriteThrough("rem_a", true)
	if err := s.RemoveRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	return s, backingA
}

func modeOf(s *Server, remote string) string {
	for _, r := range s.UploadStatus().Remotes {
		if r.Remote == remote {
			return r.WriteMode
		}
	}
	return ""
}

func TestStreamingModeIsReportedPerRemote(t *testing.T) {
	s, _ := streamingServer(t)
	if got := modeOf(s, "rem_a"); got != "through" {
		t.Fatalf("rem_a mode %q, want through", got)
	}
	if got := modeOf(s, "rem_b"); got != "cached" {
		t.Fatalf("rem_b mode %q, want cached: the setting is per remote", got)
	}
}

// The point of streaming: the writer is held to the upload's pace, so when the last write
// returns the data is (nearly) there, and close only finalises. The same copy to a buffered
// remote writes instantly and makes close wait for everything.
func TestStreamingHoldsTheWriterToTheUploadsPace(t *testing.T) {
	slowUploads := func(t *testing.T) { slowUploads(t, 100<<10) } // 100 KiB/s

	copyTimes := func(t *testing.T, s *Server, remote string) (writes, closing time.Duration) {
		dir := remoteRootID(t, s, remote)
		r := mustOK(t, s.handleRequest(req(t, 1, "create",
			map[string]any{"dirId": dir, "name": "copy.bin", "isDir": false})), "create")
		id := r.Result.(ItemInfo).ID
		mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
		start := time.Now()
		for i := 0; i < 6; i++ { // 6 x 50 KiB = 300 KiB, about 3s at 100 KiB/s
			mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{
				"itemId": id, "offset": i * (50 << 10), "data": payloadOf(50<<10, byte('a'+i))})), "write")
		}
		writes = time.Since(start)
		start = time.Now()
		mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
		return writes, time.Since(start)
	}

	t.Run("streaming", func(t *testing.T) {
		s, backingA := streamingServer(t)
		slowUploads(t)
		writes, closing := copyTimes(t, s, "rem_a")
		if writes < 1500*time.Millisecond {
			t.Fatalf("writes took %v: the writer was not held to the upload's pace", writes)
		}
		if closing > 2*time.Second {
			t.Fatalf("close took %v after the writes: most of the upload should already be done", closing)
		}
		got, err := os.ReadFile(filepath.Join(backingA, "copy.bin"))
		if err != nil || len(got) != 300<<10 {
			t.Fatalf("after close the remote holds %d bytes (err %v), want %d", len(got), err, 300<<10)
		}
	})

	t.Run("buffered", func(t *testing.T) {
		s, _, _ := newTestServer(t)
		slowUploads(t)
		writes, closing := copyTimes(t, s, "rem_b")
		if writes > 700*time.Millisecond {
			t.Fatalf("buffered writes took %v: they should land in the cache at once", writes)
		}
		if closing < time.Second {
			t.Fatalf("buffered close took %v: it should wait for the upload", closing)
		}
	})
}

// A new file, an overwrite (truncated to 0 first, then opened) and a plain append of a new
// version all work; the content on the remote is exactly the last write.
func TestStreamingOverwriteReplacesTheFile(t *testing.T) {
	s, backingA := streamingServer(t)
	dir := remoteRootID(t, s, "rem_a")

	write := func(name string, id uint64, data []byte) {
		mustOK(t, s.handleRequest(req(t, 5, "open", map[string]any{"itemId": id, "write": true})), "open "+name)
		mustOK(t, s.handleRequest(req(t, 6, "write", map[string]any{"itemId": id, "offset": 0, "data": data})), "write "+name)
		mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": id})), "close "+name)
	}

	r := mustOK(t, s.handleRequest(req(t, 1, "create",
		map[string]any{"dirId": dir, "name": "doc.txt", "isDir": false})), "create")
	id := r.Result.(ItemInfo).ID
	write("first", id, []byte("version one, a good deal longer"))
	if got, _ := os.ReadFile(filepath.Join(backingA, "doc.txt")); string(got) != "version one, a good deal longer" {
		t.Fatalf("first write: remote holds %q", got)
	}

	// The overwrite as FSKit sends it: truncate to 0, then open and write.
	mustOK(t, s.handleRequest(req(t, 8, "setattr", map[string]any{"itemId": id, "size": 0})), "truncate")
	write("second", id, []byte("v2"))
	if got, _ := os.ReadFile(filepath.Join(backingA, "doc.txt")); string(got) != "v2" {
		t.Fatalf("overwrite: remote holds %q, want v2", got)
	}
}

// What streaming cannot do must fail loudly, not corrupt: an in-place edit of a file that has
// content (refused at the first write, the remote file untouched), and a write that does not
// continue where the last one ended.
func TestStreamingRefusesInPlaceEditsAndNonSequentialWrites(t *testing.T) {
	s, backingA := streamingServer(t)
	dir := remoteRootID(t, s, "rem_a")
	if err := os.WriteFile(filepath.Join(backingA, "existing.txt"), []byte("already here"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Re-list so the bridge sees it.
	existing := childID(t, s, dir, "existing.txt")

	// rclone opens a streaming handle lazily, so the open itself succeeds; the refusal comes at
	// the first write. Either way the file on the remote must be left exactly as it was.
	if r := s.handleRequest(req(t, 1, "open", map[string]any{"itemId": existing, "write": true})); r.Ok {
		if w := s.handleRequest(req(t, 9, "write", map[string]any{"itemId": existing, "offset": 0, "data": []byte("clobbered")})); w.Ok {
			t.Fatal("an in-place write to a file that has content succeeded on a streaming remote")
		}
		_ = s.handleRequest(req(t, 10, "close", map[string]any{"itemId": existing}))
	}
	if got, _ := os.ReadFile(filepath.Join(backingA, "existing.txt")); string(got) != "already here" {
		t.Fatalf("a refused in-place write changed the file: %q", got)
	}

	c := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dir, "name": "seq.bin", "isDir": false})), "create")
	id := c.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 3, "open", map[string]any{"itemId": id, "write": true})), "open")
	mustOK(t, s.handleRequest(req(t, 4, "write", map[string]any{"itemId": id, "offset": 0, "data": payloadOf(100, 'a')})), "first write")
	if r := s.handleRequest(req(t, 5, "write", map[string]any{"itemId": id, "offset": 5000, "data": payloadOf(100, 'b')})); r.Ok {
		t.Fatal("a write that skipped ahead succeeded: the file would have a hole")
	}
	_ = s.handleRequest(req(t, 6, "close", map[string]any{"itemId": id}))
}

// Switching the preference off and re-adding the remote returns it to the normal mode.
func TestStreamingCanBeSwitchedOff(t *testing.T) {
	s, _ := streamingServer(t)
	s.SetWriteThrough("rem_a", false)
	if err := s.RemoveRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	if got := modeOf(s, "rem_a"); got != "cached" {
		t.Fatalf("mode %q after switching streaming off, want cached", got)
	}
}

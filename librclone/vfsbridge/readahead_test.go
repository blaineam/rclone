package vfsbridge

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
)

// TestReadAheadGrowsWithSequentialReadsAndResetsOnSeek pins the policy: a
// fresh handle and a seek get the minimum, sequential runs grow it by four
// every readAheadStep reads, and the ceiling holds.
func TestReadAheadGrowsWithSequentialReadsAndResetsOnSeek(t *testing.T) {
	var p readAhead
	const n = 64 * 1024
	want := func(got, want int64, what string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: read-ahead %d, want %d", what, got, want)
		}
	}
	// First read on the handle.
	want(p.next(0, n), int64(readAheadMin), "first read")
	// Reads 2..4 of the run stay at the minimum; the 5th (seq == 4) grows.
	off := int64(n)
	for i := 1; i < readAheadStep; i++ {
		want(p.next(off, n), int64(readAheadMin), "early sequential read")
		off += n
	}
	want(p.next(off, n), 4*int64(readAheadMin), "after one step")
	off += n
	for i := 1; i < readAheadStep; i++ {
		p.next(off, n)
		off += n
	}
	want(p.next(off, n), 16*int64(readAheadMin), "after two steps")
	off += n
	for i := 0; i < 20*readAheadStep; i++ {
		got := p.next(off, n)
		if got > int64(readAheadMax) {
			t.Fatalf("read-ahead %d exceeded the ceiling %d", got, readAheadMax)
		}
		off += n
	}
	want(p.next(off, n), int64(readAheadMax), "long run reaches the ceiling")
	// A seek far away resets to the minimum.
	want(p.next(off+int64(100*fs.Mebi), n), int64(readAheadMin), "after a seek")
	// A page re-read just behind the run's end is not a seek.
	off = off + int64(100*fs.Mebi) + n
	for i := 0; i < readAheadStep; i++ {
		p.next(off, n)
		off += n
	}
	want(p.next(off-2*n, n), 4*int64(readAheadMin), "re-read behind the run")
	// A read within readAheadNear ahead continues the run too.
	want(p.next(off+readAheadNear/2, n), 4*int64(readAheadMin), "small forward gap")
}

// cacheBlocksOf returns how many bytes of the one file under the VFS cache
// are actually allocated. The cache file is sparse, so its apparent size is
// the whole object's; the allocated blocks are what the downloader fetched.
func cacheBlocksOf(t *testing.T, name string) int64 {
	t.Helper()
	var found string
	_ = filepath.WalkDir(filepath.Join(config.GetCacheDir(), "vfs"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == name {
			found = p
		}
		return nil
	})
	if found == "" {
		return 0
	}
	st, err := os.Stat(found)
	if err != nil {
		t.Fatal(err)
	}
	return st.Sys().(*syscall.Stat_t).Blocks * 512
}

// TestHeaderProbeFetchesLittleAndStreamGrows is the end-to-end behaviour on a
// cached remote: a small read at the start of a big file leaves the cache
// holding about readAheadMin, not the whole file; a sequential run through
// the same file is then served with a growing window.
func TestHeaderProbeFetchesLittleAndStreamGrows(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	const size = 48 * fs.Mebi
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(backingA, "film.bin"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	dirA := remoteRootID(t, s, "rem_a")
	fileID := childID(t, s, dirA, "film.bin")

	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": fileID, "write": false})), "open")
	r := mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": fileID, "offset": 0, "length": 16})), "read")
	if got := r.Result.([]byte); len(got) != 16 || got[1] != 1 {
		t.Fatalf("header read returned %d bytes (%v)", len(got), got)
	}

	// Let the downloader run to its window and stop. It writes to the cache
	// file in asyncreader-sized pieces, so allow one extra piece on top of
	// the read-ahead.
	deadline := time.Now().Add(10 * time.Second)
	var have int64
	for time.Now().Before(deadline) {
		have = cacheBlocksOf(t, "film.bin")
		if have >= int64(readAheadMin) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // anything still in flight lands
	have = cacheBlocksOf(t, "film.bin")
	if have < int64(readAheadMin) || have > int64(readAheadMin)+int64(4*fs.Mebi) {
		t.Fatalf("after a 16-byte probe the cache holds %d bytes; want about readAheadMin (%d), not the %d-byte file",
			have, readAheadMin, size)
	}

	// Now stream: sequential 1MiB reads from the start. The window grows, so
	// well before the end the cache is ahead of the reader by far more than
	// readAheadMin.
	const chunk = int64(1 * fs.Mebi)
	var off int64
	for off < int64(24*fs.Mebi) {
		mustOK(t, s.handleRequest(req(t, 3, "read", map[string]any{"itemId": fileID, "offset": off, "length": chunk})), "stream read")
		off += chunk
	}
	time.Sleep(300 * time.Millisecond)
	have = cacheBlocksOf(t, "film.bin")
	if have < off+int64(8*fs.Mebi) {
		t.Fatalf("after %d bytes of sequential reads the cache holds %d; want the window to have grown past readAheadMin", off, have)
	}
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": fileID})), "close")
}

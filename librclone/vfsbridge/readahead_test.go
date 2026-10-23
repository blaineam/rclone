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

func TestReadAheadFollowsBytesConsumed(t *testing.T) {
	const n = 64 * 1024
	mib := int64(fs.Mebi)
	want := func(got, want int64, what string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: read-ahead %d, want %d", what, got, want)
		}
	}

	// A probe: one small read costs the floor.
	var probe readAhead
	want(probe.next(0, n), 2*mib, "first read")

	// A metadata parse: 273 reads of 64KiB with jumps in both directions
	// (the recorded AVFoundation pattern's shape) never earns more than half
	// of what it consumed, so it cannot look like a stream and fetch the
	// ceiling.
	var parse readAhead
	var last int64
	offs := []int64{0, 512 * 1024, 1536 * 1024, 3 * mib, 128 * 1024}
	for i := 0; i < 273; i++ {
		last = parse.next(offs[i%len(offs)]+int64(i)*n, n)
	}
	consumed := int64(273 * n)
	want(last, max(consumed/2, 2*mib), "parse window is half of what it read")
	if last > 16*mib {
		t.Fatalf("a 17 MiB parse got a %d window", last)
	}

	// A stream: 1MiB reads double the window with their consumption and
	// reach the ceiling at 256 MiB read, never above it.
	var stream readAhead
	var off int64
	for i := 1; i <= 300; i++ {
		got := stream.next(off, mib)
		off += mib
		switch {
		case int64(i) < 4:
			want(got, 2*mib, "early stream read stays at the floor")
		case int64(i) <= 256:
			want(got, int64(i)*mib/2, "stream window is half of what it read")
		default:
			want(got, 128*mib, "stream holds the ceiling")
		}
	}

	// A bulk reader that seeks: the window depends on consumption only, so
	// a jump far away changes nothing.
	want(stream.next(5*1024*mib, n), 128*mib, "seek after 300 MiB keeps the ceiling")
	var seeky readAhead
	for i := 0; i < 64; i++ {
		last = seeky.next(int64(i)*32*mib, n)
	}
	want(last, 2*mib, "seeking reader stays at the floor while it has read little")
	for i := 64; i < 64+512; i++ {
		last = seeky.next(int64(i)*32*mib, n)
	}
	want(last, 18*mib, "seeking reader that has read 36 MiB gets 18 MiB regardless of seeks")
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

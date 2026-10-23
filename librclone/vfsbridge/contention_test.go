package vfsbridge

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
)

// The latency bug behind "25ms per operation on a warm walk": the VFS cache
// fingerprinted a file on its first read with a hash the backend computes,
// holding the item's lock, and every other file's getattr queued behind that
// lock through the cache. These tests use the local backend, whose hash is a
// full read of the file, as a stand-in for SFTP's remote md5sum.

func writeBigFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1<<20)
	for i := range buf {
		buf[i] = byte(i * 13)
	}
	for written := int64(0); written < size; written += int64(len(buf)) {
		if _, err := f.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	_ = f.Close()
}

// firstReadAndConcurrentGetattr opens big and reads its first 64KiB. While
// that read is in flight it issues a getattr of big itself (which queues on
// big's item lock, as on the live volume where Sami stats what it opens) and,
// 50ms later, times a getattr of small, a file that already has a cache item.
// The production chain was: getattr(big) waits on big's item lock while
// holding the cache lock, so getattr(small) waits on the cache lock, so every
// metadata operation on the remote waits for big's open.
//
// The read is of the file's LAST 64KiB. The first read is what triggers the
// open (and the hash), wherever it lands; reading at the end lets the
// downloader reach EOF and finish, instead of idling with a buffer in hand
// after the test ends and spending the next test's global bandwidth limit.
func firstReadAndConcurrentGetattr(t *testing.T, s *Server, bigID, smallID uint64, bigSize int64) (firstRead, getattr time.Duration) {
	t.Helper()
	// Give small a cache item, as every file that was ever read has one.
	mustOK(t, s.handleRequest(req(t, 10, "open", map[string]any{"itemId": smallID, "write": false})), "open small")
	mustOK(t, s.handleRequest(req(t, 11, "read", map[string]any{"itemId": smallID, "offset": 0, "length": 1})), "read small")
	mustOK(t, s.handleRequest(req(t, 12, "close", map[string]any{"itemId": smallID})), "close small")

	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": bigID, "write": false})), "open")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		t0 := time.Now()
		mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": bigID, "offset": bigSize - 64*1024, "length": 64 * 1024})), "read")
		firstRead = time.Since(t0)
	}()
	time.Sleep(50 * time.Millisecond) // let the read reach Item.Open
	go func() {
		defer wg.Done()
		mustOK(t, s.handleRequest(req(t, 5, "getattr", map[string]any{"itemId": bigID})), "getattr big")
	}()
	time.Sleep(50 * time.Millisecond) // let that getattr queue on big's item lock
	t0 := time.Now()
	mustOK(t, s.handleRequest(req(t, 3, "getattr", map[string]any{"itemId": smallID})), "getattr")
	getattr = time.Since(t0)
	wg.Wait()
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": bigID})), "close")
	return firstRead, getattr
}

func TestFirstReadDoesNotHashTheFileAndDoesNotStallOthers(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	const size = 768 * fs.Mebi
	writeBigFile(t, filepath.Join(backingA, "film.bin"), int64(size))
	if err := os.WriteFile(filepath.Join(backingA, "small.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirA := remoteRootID(t, s, "rem_a")
	bigID := childID(t, s, dirA, "film.bin")
	smallID := childID(t, s, dirA, "small.txt")

	firstRead, getattr := firstReadAndConcurrentGetattr(t, s, bigID, smallID, int64(size))
	t.Logf("fast fingerprint: first 64KiB read %v, concurrent getattr of another file %v", firstRead, getattr)
	if firstRead > 500*time.Millisecond {
		t.Fatalf("first read of a %v file took %v; the cache is still hashing the whole file on open", size, firstRead)
	}
	if getattr > 200*time.Millisecond {
		t.Fatalf("getattr of an unrelated file took %v while another file was being opened", getattr)
	}
}

// TestSlowOpenNoLongerStallsOtherFiles shows the lock chain the DirtyItem
// change broke: with the hashing fingerprint back on, the first read of the
// big file is slow (it hashes 768MiB), but a getattr of another file on the
// same remote no longer waits for it.
func TestSlowOpenNoLongerStallsOtherFiles(t *testing.T) {
	fastFingerprint = false
	t.Cleanup(func() { fastFingerprint = true })
	s, backingA, _ := newTestServer(t)
	const size = 768 * fs.Mebi
	writeBigFile(t, filepath.Join(backingA, "film.bin"), int64(size))
	if err := os.WriteFile(filepath.Join(backingA, "small.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirA := remoteRootID(t, s, "rem_a")
	bigID := childID(t, s, dirA, "film.bin")
	smallID := childID(t, s, dirA, "small.txt")

	firstRead, getattr := firstReadAndConcurrentGetattr(t, s, bigID, smallID, int64(size))
	t.Logf("hashing fingerprint: first 64KiB read %v, concurrent getattr of another file %v", firstRead, getattr)
	if firstRead < 100*time.Millisecond {
		t.Skipf("first read took only %v; this machine hashes 768MiB too fast for the scenario to be visible", firstRead)
	}
	if getattr > firstRead/4 {
		t.Fatalf("getattr of an unrelated file took %v while a %v open took %v: one slow open still stalls the whole remote", getattr, size, firstRead)
	}
}

// TestStatFSNeverWaitsOnTheBackends: after the first computation, statfs is
// answered from the last result and refreshes off the request path.
func TestStatFSServesCachedFiguresAndRefreshesInBackground(t *testing.T) {
	s, _, _ := newTestServer(t)
	r := mustOK(t, s.handleRequest(req(t, 1, "statfs", nil)), "statfs")
	first := r.Result.(StatFSInfo)
	if first.TotalBytes == 0 {
		t.Fatal("first statfs computed nothing")
	}
	c := s.statfs.Load()
	if c == nil {
		t.Fatal("first statfs did not record its result")
	}
	// Age the record past the refresh interval: the next statfs must answer
	// at once with the old figures and start a refresh.
	c.at = time.Now().Add(-2 * statfsRefresh)
	t0 := time.Now()
	r = mustOK(t, s.handleRequest(req(t, 2, "statfs", nil)), "statfs")
	if d := time.Since(t0); d > 50*time.Millisecond {
		t.Fatalf("stale statfs took %v; it should answer from the cache", d)
	}
	if r.Result.(StatFSInfo) != first {
		t.Fatal("stale statfs did not serve the last computed figures")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && s.statfs.Load() == c {
		time.Sleep(10 * time.Millisecond)
	}
	if s.statfs.Load() == c {
		t.Fatal("no background refresh happened")
	}
	// A remote-set change also triggers a refresh.
	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatal(err)
	}
	mustOK(t, s.handleRequest(req(t, 3, "statfs", nil)), "statfs")
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && s.statfs.Load().gen != s.generation.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if s.statfs.Load().gen != s.generation.Load() {
		t.Fatal("statfs did not refresh after the remote set changed")
	}
}

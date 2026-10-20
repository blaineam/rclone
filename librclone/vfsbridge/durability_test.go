package vfsbridge

// Tests for upload durability: a copy is finished when the bytes are on the remote, a
// backlog holds writers back, what has not uploaded is visible, and an upload that cannot
// finish can be rescued with its folder structure.
//
// "On the remote" is a local directory behind an alias. To make an upload slow the tests
// limit rclone's bandwidth; to make one fail they make that directory read-only.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
)

// slowUploads limits upload bandwidth for the rest of the test.
func slowUploads(t *testing.T, bytesPerSecond int) {
	t.Helper()
	accounting.TokenBucket.SetBwLimit(fs.BwPair{Tx: fs.SizeSuffix(bytesPerSecond), Rx: fs.SizeSuffix(bytesPerSecond)})
	t.Cleanup(func() { accounting.TokenBucket.SetBwLimit(fs.BwPair{}) })
}

// breakRemote makes uploads into dir fail until the returned func is called.
func breakRemote(t *testing.T, dir string) (repair func()) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	repair = func() { _ = os.Chmod(dir, 0o755) }
	t.Cleanup(repair)
	return repair
}

// createAndWrite creates name in dirID, opens it, writes payload and returns the item id,
// leaving it open.
func createAndWrite(t *testing.T, s *Server, dirID uint64, name string, payload []byte) uint64 {
	t.Helper()
	r := mustOK(t, s.handleRequest(req(t, 1, "create",
		map[string]any{"dirId": dirID, "name": name, "isDir": false})), "create "+name)
	id := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open "+name)
	mustOK(t, s.handleRequest(req(t, 3, "write",
		map[string]any{"itemId": id, "offset": 0, "data": payload})), "write "+name)
	return id
}

func payloadOf(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

// A close that returns success must mean the bytes are on the remote, however long the
// upload takes. The old close gave up after 8 seconds and reported success anyway.
func TestCloseReturnsOnlyOnceTheBytesAreOnTheRemote(t *testing.T) {
	if testing.Short() {
		t.Skip("takes ~10s: the upload has to outlast the legacy 8s close wait")
	}
	s, backingA, _ := newTestServer(t)
	slowUploads(t, 40<<10) // 400 KiB at 40 KiB/s: about 10s, longer than closeFlushTimeout
	dirA := remoteRootID(t, s, "rem_a")
	payload := payloadOf(400<<10, 'x')
	id := createAndWrite(t, s, dirA, "big.bin", payload)

	start := time.Now()
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
	took := time.Since(start)

	if took <= closeFlushTimeout {
		t.Fatalf("close returned after %v, inside the old %v wait: it did not wait for the upload", took, closeFlushTimeout)
	}
	got, err := os.ReadFile(filepath.Join(backingA, "big.bin"))
	if err != nil {
		t.Fatalf("close reported success but the file is not on the remote: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("close reported success with %d of %d bytes on the remote", len(got), len(payload))
	}
}

// With confirmation off the legacy bounded wait applies, so the switch really switches.
func TestConfirmUploadsCanBeSwitchedOff(t *testing.T) {
	s, _, _ := newTestServer(t)
	s.SetConfirmUploads(false)
	dirA := remoteRootID(t, s, "rem_a")
	id := createAndWrite(t, s, dirA, "quick.txt", []byte("hi"))
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
}

// A file that keeps failing to upload fails its close with EIO, stays queued, shows as
// failing, and uploads by itself once the remote is back.
func TestFailingUploadFailsCloseThenRecovers(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirA := remoteRootID(t, s, "rem_a")
	payload := []byte("must not be lost")
	id := createAndWrite(t, s, dirA, "stuck.txt", payload)
	repair := breakRemote(t, backingA) // after the write: the upload is what fails

	r := s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id}))
	if r.Ok || r.Error != cEIO {
		t.Fatalf("close of a file that cannot upload: ok=%v errno=%d, want EIO", r.Ok, r.Error)
	}

	st := s.UploadStatus()
	if st.QueuedFiles != 1 || st.FailingFiles != 1 {
		t.Fatalf("status after failures: %+v, want one queued failing file", st)
	}
	it := st.Remotes[0].Items[0]
	if it.Path != "stuck.txt" || !it.Failing || it.Tries < confirmFailAfterTries || it.Size != int64(len(payload)) {
		t.Fatalf("failing item reported as %+v", it)
	}

	repair()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if got, err := os.ReadFile(filepath.Join(backingA, "stuck.txt")); err == nil && string(got) == string(payload) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the file never uploaded after the remote came back: it was not retried")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if left := s.UploadStatus().QueuedFiles; left != 0 {
		// The queue entry clears just after the upload lands.
		time.Sleep(500 * time.Millisecond)
		if left = s.UploadStatus().QueuedFiles; left != 0 {
			t.Fatalf("%d files still queued after a successful upload", left)
		}
	}
}

// Failed uploads must back off, not hammer the remote in a hot loop.
func TestFailedUploadsBackOff(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirA := remoteRootID(t, s, "rem_a")
	id := createAndWrite(t, s, dirA, "retry.txt", []byte("x"))
	breakRemote(t, backingA)
	s.SetConfirmUploads(false)
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")

	time.Sleep(2500 * time.Millisecond)
	tries := s.UploadStatus().Remotes[0].Items[0].Tries
	// With a 1s then 2s backoff there are about 2-3 attempts in 2.5s. A hot loop makes
	// thousands.
	if tries > 6 {
		t.Fatalf("%d upload attempts in 2.5s: failed uploads are not backing off", tries)
	}
	if tries < 2 {
		t.Fatalf("only %d attempt(s) in 2.5s: failed uploads are not being retried", tries)
	}
}

// Rescue copies pending files out with their folder structure, never overwrites, keeps
// the files queued, and skips nothing that is complete.
func TestRescuePendingKeepsFolderStructure(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirA := remoteRootID(t, s, "rem_a")
	// Build the folder structure on the remote first, then break it.
	mustOK(t, s.handleRequest(req(t, 1, "create",
		map[string]any{"dirId": dirA, "name": "docs", "isDir": true})), "mkdir docs")
	docs := childID(t, s, dirA, "docs")
	mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": docs, "name": "sub", "isDir": true})), "mkdir sub")
	sub := childID(t, s, docs, "sub")

	payload := []byte("rescue me, folders and all")
	id := createAndWrite(t, s, sub, "report.txt", payload)
	breakRemote(t, filepath.Join(backingA, "docs", "sub"))
	if r := s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})); r.Ok {
		t.Fatal("close of an unuploadable file succeeded")
	}

	dest := t.TempDir()
	res := s.RescuePending(dest, true)
	if len(res.Rescued) != 1 || len(res.Skipped) != 0 {
		t.Fatalf("rescue: %+v, want one rescued and none skipped", res)
	}
	want := filepath.Join(dest, "rem_a", "docs", "sub", "report.txt")
	if res.Rescued[0].Dest != want {
		t.Fatalf("rescued to %q, want %q", res.Rescued[0].Dest, want)
	}
	got, err := os.ReadFile(want)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("rescued file is %q (err %v), want %q", got, err, payload)
	}

	// Rescuing again must not overwrite the first copy.
	res2 := s.RescuePending(dest, true)
	if len(res2.Rescued) != 1 || !strings.Contains(res2.Rescued[0].Dest, "(recovered 1)") {
		t.Fatalf("second rescue: %+v, want a numbered copy", res2.Rescued)
	}
	if again, _ := os.ReadFile(want); string(again) != string(payload) {
		t.Fatal("a second rescue overwrote the first copy")
	}

	// Copy only: the file is still queued and still retrying.
	if s.UploadStatus().QueuedFiles != 1 {
		t.Fatal("rescue removed the file from the upload queue")
	}
}

// onlyFailing leaves files alone that are still on their first attempts.
func TestRescueOnlyFailingSkipsHealthyUploads(t *testing.T) {
	s, _, _ := newTestServer(t)
	slowUploads(t, 30<<10)
	dirA := remoteRootID(t, s, "rem_a")
	id := createAndWrite(t, s, dirA, "healthy.bin", payloadOf(100<<10, 'h'))
	done := make(chan struct{})
	go func() {
		s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id}))
		close(done)
	}()
	time.Sleep(500 * time.Millisecond)
	res := s.RescuePending(t.TempDir(), true)
	if len(res.Rescued) != 0 || len(res.Skipped) != 0 {
		t.Fatalf("rescue of failing-only touched a healthy upload: %+v", res)
	}
	<-done
}

// The queue is visible while uploads run, and empty afterwards.
func TestUploadStatusShowsTheQueue(t *testing.T) {
	s, _, _ := newTestServer(t)
	slowUploads(t, 30<<10)
	dirA := remoteRootID(t, s, "rem_a")
	id := createAndWrite(t, s, dirA, "one.bin", payloadOf(90<<10, '1'))
	done := make(chan struct{})
	go func() {
		s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id}))
		close(done)
	}()
	time.Sleep(700 * time.Millisecond)
	st := s.UploadStatus()
	if st.QueuedFiles != 1 || st.QueuedBytes != 90<<10 || st.FailingFiles != 0 {
		t.Fatalf("status during an upload: %+v", st)
	}
	if !st.Remotes[0].Items[0].Uploading {
		t.Fatalf("item not marked uploading: %+v", st.Remotes[0].Items[0])
	}
	<-done
	if left := s.UploadStatus().QueuedFiles; left != 0 {
		t.Fatalf("%d files queued after the close returned", left)
	}
}

// While too much waits to upload, a writer is held back (back pressure); with flow
// control off it is not.
func TestWritesAreHeldBackWhileUploadsBacklog(t *testing.T) {
	for _, tc := range []struct {
		name    string
		control bool
		held    bool
	}{{"on", true, true}, {"off", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := newTestServer(t)
			s.SetFlowControl(tc.control)
			oldMark, oldStall := writeHighWater, writeStallMax
			writeHighWater, writeStallMax = 50<<10, 5*time.Second
			t.Cleanup(func() { writeHighWater, writeStallMax = oldMark, oldStall })
			slowUploads(t, 30<<10) // 90 KiB takes about 3s

			dirA := remoteRootID(t, s, "rem_a")
			first := createAndWrite(t, s, dirA, "first.bin", payloadOf(90<<10, 'a'))
			done := make(chan struct{})
			go func() {
				s.handleRequest(req(t, 4, "close", map[string]any{"itemId": first}))
				close(done)
			}()
			time.Sleep(400 * time.Millisecond) // first.bin is now queued and uploading

			r := mustOK(t, s.handleRequest(req(t, 5, "create",
				map[string]any{"dirId": dirA, "name": "second.bin", "isDir": false})), "create second")
			id := r.Result.(ItemInfo).ID
			mustOK(t, s.handleRequest(req(t, 6, "open", map[string]any{"itemId": id, "write": true})), "open second")
			start := time.Now()
			mustOK(t, s.handleRequest(req(t, 7, "write",
				map[string]any{"itemId": id, "offset": 0, "data": payloadOf(1024, 'b')})), "write second")
			waited := time.Since(start)

			if tc.held && waited < time.Second {
				t.Fatalf("the writer was held only %v while 90 KiB waited to upload (limit 50 KiB)", waited)
			}
			if tc.held && waited > 5500*time.Millisecond {
				t.Fatalf("the writer was held %v, past the stall cap", waited)
			}
			if !tc.held && waited > 500*time.Millisecond {
				t.Fatalf("flow control off but the writer was held %v", waited)
			}
			<-done
			mustOK(t, s.handleRequest(req(t, 8, "close", map[string]any{"itemId": id})), "close second")
		})
	}
}

// A file that is dirty only because ANOTHER handle is open has nothing to wait for.
func TestCloseOfOneOfTwoWritersDoesNotWait(t *testing.T) {
	s, _, _ := newTestServer(t)
	slowUploads(t, 20<<10)
	dirA := remoteRootID(t, s, "rem_a")
	id := createAndWrite(t, s, dirA, "shared.bin", payloadOf(60<<10, 's'))
	mustOK(t, s.handleRequest(req(t, 5, "open", map[string]any{"itemId": id, "write": true})), "second open")

	start := time.Now()
	mustOK(t, s.handleRequest(req(t, 6, "close", map[string]any{"itemId": id})), "first close")
	if took := time.Since(start); took > time.Second {
		t.Fatalf("closing one of two handles took %v: it waited for an upload that cannot start yet", took)
	}
	// The last close does store, and waits.
	mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": id})), "last close")
}

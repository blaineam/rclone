package vfsbridge

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
)

// TestFirstWriteDoesNotWaitOutTheCancelledEmptyUpload covers the write side
// of a copy onto the volume. Creating a file opens and closes it, which queues
// an upload of it empty; the caller's first write then cancels that upload
// (vfscache Item._dirty) and waits for it to finish. The WebDAV backend slept
// a flat second after any failed PUT, cancellation included, so every new
// file's first write took a second once another upload was in flight.
// Measured on a loopback server: 1.01s, now ~20ms.
//
// Needs `rclone` on PATH (or ES_RCLONE_BIN) to serve the loopback remote.
// newWebdavServer serves a temp directory over loopback WebDAV with the rclone
// binary on PATH (or ES_RCLONE_BIN), exposes it on a fresh bridge as "meas",
// and returns the bridge, the served directory and the remote's root id. Skips
// the test when there is no rclone binary.
func newWebdavServer(t *testing.T) (*Server, string, uint64) {
	t.Helper()
	rcloneBin := os.Getenv("ES_RCLONE_BIN")
	if rcloneBin == "" {
		var err error
		if rcloneBin, err = exec.LookPath("rclone"); err != nil {
			t.Skip("no rclone binary to serve a loopback WebDAV remote")
		}
	}
	served := t.TempDir()
	port := freePort(t)
	serve := exec.Command(rcloneBin, "serve", "webdav", served,
		"--addr", fmt.Sprintf("127.0.0.1:%d", port), "--vfs-cache-mode", "off", "--log-level", "ERROR")
	serve.Env = append(os.Environ(), "RCLONE_CONFIG=/notfound")
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serve.Process.Kill(); _ = serve.Wait() })
	for i := 0; ; i++ {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			c.Close()
			break
		}
		if i > 50 {
			t.Fatal("rclone serve never listened")
		}
		time.Sleep(100 * time.Millisecond)
	}
	confPath := filepath.Join(t.TempDir(), "rclone.conf")
	conf := fmt.Sprintf("[meas]\ntype = webdav\nurl = http://127.0.0.1:%d/\nvendor = other\n", port)
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	useTempCacheDir(t)
	s := NewServer()
	if err := s.AddRemote("meas"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s, served, remoteRootID(t, s, "meas")
}

func TestFirstWriteDoesNotWaitOutTheCancelledEmptyUpload(t *testing.T) {
	s, _, dir := newWebdavServer(t)

	// A 1.3MB file, as cp(1) writes it: create, open, write, close. Its
	// upload is what the next file's cancelled empty upload queues behind.
	big := make([]byte, 1300*1024)
	bid := childIDOf(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": "incoming.bin", "isDir": false})))
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": bid, "write": true})), "open")
	t0 := time.Now()
	mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": bid, "offset": 0, "data": big})), "write")
	firstWrite := time.Since(t0)
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": bid})), "close")

	// The sidecar cp(1) creates alongside it, created and written while the
	// big upload is in flight.
	sid := childIDOf(t, s.handleRequest(req(t, 5, "create", map[string]any{"dirId": dir, "name": "._incoming.bin", "isDir": false})))
	mustOK(t, s.handleRequest(req(t, 6, "open", map[string]any{"itemId": sid, "write": true})), "open")
	t0 = time.Now()
	mustOK(t, s.handleRequest(req(t, 7, "write", map[string]any{"itemId": sid, "offset": 0, "data": make([]byte, 4096)})), "write")
	sidecarWrite := time.Since(t0)
	mustOK(t, s.handleRequest(req(t, 8, "close", map[string]any{"itemId": sid})), "close")

	t.Logf("first write of a new 1.3MB file %v; of a 4KiB sidecar while that uploads %v", firstWrite.Round(time.Millisecond), sidecarWrite.Round(time.Millisecond))
	for _, d := range []time.Duration{firstWrite, sidecarWrite} {
		if d > 500*time.Millisecond {
			t.Fatalf("a first write took %v: it is waiting out the backend's post-error sleep for the cancelled empty upload", d)
		}
	}
}

func childIDOf(t *testing.T, r *Response) uint64 {
	t.Helper()
	mustOK(t, r, "create")
	return r.Result.(ItemInfo).ID
}

// TestRenameOntoExistingWhileBothUploadsArePending is the harness's
// rename-onto-existing-destination test at bridge level: write the old
// destination, write the new source, rename source over destination at once,
// and the volume must serve the new bytes and the backing store must end up
// holding them.
func TestRenameOntoExistingWhileBothUploadsArePending(t *testing.T) {
	s, served, dir := newWebdavServer(t)
	writeFile := func(name string, data []byte) uint64 {
		id := childIDOf(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": name, "isDir": false})))
		mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
		mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": 0, "data": data})), "write")
		mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
		return id
	}
	oldData, newData := []byte("the old content\n"), []byte("the new content\n")
	writeFile("ov-dst.txt", oldData)
	writeFile("ov-src.txt", newData)
	mustOK(t, s.handleRequest(req(t, 5, "rename", map[string]any{"srcDirId": dir, "srcName": "ov-src.txt", "dstDirId": dir, "dstName": "ov-dst.txt"})), "rename")
	dstID := childID(t, s, dir, "ov-dst.txt")
	r := mustOK(t, s.handleRequest(req(t, 6, "read", map[string]any{"itemId": dstID, "offset": 0, "length": 64})), "read")
	if got := r.Result.([]byte); string(got) != string(newData) {
		t.Fatalf("after rename the volume serves %q, want %q", got, newData)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		got, _ := os.ReadFile(filepath.Join(served, "ov-dst.txt"))
		if string(got) == string(newData) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backing store holds %q after rename, want %q", got, newData)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(served, "ov-src.txt")); err == nil {
		t.Fatal("source survived the rename on the backing store")
	}
}

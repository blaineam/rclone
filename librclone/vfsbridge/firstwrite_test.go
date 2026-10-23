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
func TestFirstWriteDoesNotWaitOutTheCancelledEmptyUpload(t *testing.T) {
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
	dir := remoteRootID(t, s, "meas")

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

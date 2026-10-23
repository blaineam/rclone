package vfsbridge

// Measurement rig against a real SFTP server, not a pass/fail test.
//
// Run with:
//
//	ES_VFSBRIDGE_NAS="host=10.0.0.20,port=722,user=blaine,key_file=$HOME/.ssh/blaine" \
//	ES_VFSBRIDGE_NAS_PATH="FORTRESS/Glacier/Paid Media/Movies/9.mp4" \
//	RCLONE_CONFIG=/notfound go test -run TestMeasureNASRead -v ./librclone/vfsbridge/
//
// It mounts the server as a cached remote on a fresh bridge, exactly as the
// app does, reads the first ES_VFSBRIDGE_NAS_MIB (default 256) MiB of the
// file through the bridge's read handler in 1 MiB pieces, and reports the
// per-read latency distribution, the throughput, and where the CPU went
// (a pprof profile written next to the test binary's temp dir).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/sftp"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
)

func TestMeasureNASRead(t *testing.T) {
	spec := os.Getenv("ES_VFSBRIDGE_NAS")
	if spec == "" {
		t.Skip("set ES_VFSBRIDGE_NAS=host=...,port=...,user=...,key_file=... to run the NAS read measurement")
	}
	relPath := os.Getenv("ES_VFSBRIDGE_NAS_PATH")
	if relPath == "" {
		t.Fatal("set ES_VFSBRIDGE_NAS_PATH to a file path relative to the SFTP root")
	}
	mib := int64(256)
	if v := os.Getenv("ES_VFSBRIDGE_NAS_MIB"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mib = n
	}
	readSize := int64(fs.Mebi)
	if v := os.Getenv("ES_VFSBRIDGE_NAS_READ"); v != "" {
		var ss fs.SizeSuffix
		if err := ss.Set(v); err != nil {
			t.Fatal(err)
		}
		readSize = int64(ss)
	}
	if os.Getenv("ES_VFSBRIDGE_MEASURE_DEBUG") != "" {
		fs.GetConfig(context.Background()).LogLevel = fs.LogLevelDebug
	}

	tmp := t.TempDir()
	confPath := filepath.Join(tmp, "rclone.conf")
	var conf strings.Builder
	conf.WriteString("[nas]\ntype = sftp\nknown_hosts_file = none\nshell_type = unix\n")
	for _, kv := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("bad ES_VFSBRIDGE_NAS entry %q", kv)
		}
		fmt.Fprintf(&conf, "%s = %s\n", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	for _, extra := range strings.Split(os.Getenv("ES_VFSBRIDGE_NAS_EXTRA"), ";") {
		if extra = strings.TrimSpace(extra); extra != "" {
			conf.WriteString(extra + "\n")
		}
	}
	if err := os.WriteFile(confPath, []byte(conf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	useTempCacheDir(t)

	s := NewServer()
	if err := s.AddRemote("nas"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)

	// Walk to the file.
	dir := remoteRootID(t, s, "nas")
	parts := strings.Split(relPath, "/")
	for _, p := range parts[:len(parts)-1] {
		dir = childID(t, s, dir, p)
	}
	fileID := childID(t, s, dir, parts[len(parts)-1])

	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": fileID, "write": false})), "open")

	profPath := filepath.Join(tmp, "nas-read.prof")
	if os.Getenv("ES_VFSBRIDGE_NAS_PPROF") != "" {
		profPath = os.Getenv("ES_VFSBRIDGE_NAS_PPROF")
	}
	pf, err := os.Create(profPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(pf); err != nil {
		t.Fatal(err)
	}

	total := mib * int64(fs.Mebi)
	var lat []time.Duration
	t0 := time.Now()
	var off int64
	for off < total {
		r0 := time.Now()
		r := mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": fileID, "offset": off, "length": readSize})), "read")
		lat = append(lat, time.Since(r0))
		n := len(r.Result.([]byte))
		if n == 0 {
			t.Fatalf("short read at %d", off)
		}
		off += int64(n)
	}
	elapsed := time.Since(t0)
	pprof.StopCPUProfile()
	pf.Close()
	mustOK(t, s.handleRequest(req(t, 3, "close", map[string]any{"itemId": fileID})), "close")

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }
	t.Logf("bridge sequential read of %d MiB in %s pieces: %v = %.1f MB/s; %d reads, latency p50 %v p90 %v p99 %v max %v; profile %s",
		mib, fs.SizeSuffix(readSize), elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds()/1e6,
		len(lat), pct(0.5).Round(time.Millisecond), pct(0.9).Round(time.Millisecond), pct(0.99).Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond), profPath)
}

// TestMeasureNASReaddir times a directory listing through the bridge against
// the server named by ES_VFSBRIDGE_NAS: cold, then warm, then after the
// cached listing has gone stale (DirCacheTime forced to a millisecond), which
// is the case every lookup and readdir hits once a directory is five minutes
// old. ES_VFSBRIDGE_NAS_DIR names the directory relative to the SFTP root.
func TestMeasureNASReaddir(t *testing.T) {
	spec := os.Getenv("ES_VFSBRIDGE_NAS")
	if spec == "" {
		t.Skip("set ES_VFSBRIDGE_NAS to run the NAS readdir measurement")
	}
	relDir := os.Getenv("ES_VFSBRIDGE_NAS_DIR")
	if relDir == "" {
		t.Fatal("set ES_VFSBRIDGE_NAS_DIR to a directory relative to the SFTP root")
	}
	tmp := t.TempDir()
	confPath := filepath.Join(tmp, "rclone.conf")
	var conf strings.Builder
	conf.WriteString("[nas]\ntype = sftp\nknown_hosts_file = none\nshell_type = unix\n")
	for _, kv := range strings.Split(spec, ",") {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&conf, "%s = %s\n", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	if err := os.WriteFile(confPath, []byte(conf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	useTempCacheDir(t)
	s := NewServer()
	if err := s.AddRemote("nas"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	dir := remoteRootID(t, s, "nas")
	for _, p := range strings.Split(relDir, "/") {
		dir = childID(t, s, dir, p)
	}
	timed := func(label string) int {
		t0 := time.Now()
		r := mustOK(t, s.handleRequest(req(t, 1, "readdir", map[string]any{"dirId": dir})), "readdir")
		n := len(r.Result.([]DirEntry))
		t.Logf("%s: %d entries in %v", label, n, time.Since(t0).Round(time.Millisecond))
		return n
	}
	timed("cold readdir")
	timed("warm readdir")
	// Force the cached listing stale, as five minutes would. The cache's
	// cleanup timer drops an unvisited directory at twice DirCacheTime, so
	// the stale window is kept wide enough for the refresh to land inside it.
	s.mu.RLock()
	v := s.vfses["nas"]
	s.mu.RUnlock()
	v.Opt.DirCacheTime = fs.Duration(3 * time.Second)
	time.Sleep(3500 * time.Millisecond)
	timed("stale readdir (served from cache, refresh in background)")
	timed("stale readdir again")
	time.Sleep(3 * time.Second)
	timed("after the refresh landed")
}

// TestMeasureNASWrite times writing a file through the bridge to the server
// named by ES_VFSBRIDGE_NAS, in streaming (write-through) mode and in cached
// mode: the 1 MiB writes, the close, and for cached mode the upload that
// follows. ES_VFSBRIDGE_NAS_WRITE_DIR names the directory to write into,
// relative to the SFTP root; the file is removed afterwards.
func TestMeasureNASWrite(t *testing.T) {
	spec := os.Getenv("ES_VFSBRIDGE_NAS")
	if spec == "" {
		t.Skip("set ES_VFSBRIDGE_NAS to run the NAS write measurement")
	}
	relDir := os.Getenv("ES_VFSBRIDGE_NAS_WRITE_DIR")
	if relDir == "" {
		t.Fatal("set ES_VFSBRIDGE_NAS_WRITE_DIR to a directory relative to the SFTP root")
	}
	mib := int64(64)
	if v := os.Getenv("ES_VFSBRIDGE_NAS_MIB"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mib = n
	}
	tmp := t.TempDir()
	confPath := filepath.Join(tmp, "rclone.conf")
	var conf strings.Builder
	conf.WriteString("[nas]\ntype = sftp\nknown_hosts_file = none\nshell_type = unix\n")
	for _, kv := range strings.Split(spec, ",") {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&conf, "%s = %s\n", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	for _, extra := range strings.Split(os.Getenv("ES_VFSBRIDGE_NAS_EXTRA"), ";") {
		if extra = strings.TrimSpace(extra); extra != "" {
			conf.WriteString(extra + "\n")
		}
	}
	if err := os.WriteFile(confPath, []byte(conf.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	useTempCacheDir(t)
	buf := make([]byte, fs.Mebi)
	for i := range buf {
		buf[i] = byte(i * 13)
	}
	for _, mode := range []struct {
		name    string
		through bool
	}{{"streaming", true}, {"cached", false}} {
		t.Run(mode.name, func(t *testing.T) {
			s := NewServer()
			s.SetWriteThrough("nas", mode.through)
			if err := s.AddRemote("nas"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Stop)
			dir := remoteRootID(t, s, "nas")
			for _, p := range strings.Split(relDir, "/") {
				dir = childID(t, s, dir, p)
			}
			name := fmt.Sprintf("es-write-probe-%s.bin", mode.name)
			r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": name, "isDir": false})), "create")
			id := r.Result.(ItemInfo).ID
			mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
			t0 := time.Now()
			for i := int64(0); i < mib; i++ {
				mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": i * int64(fs.Mebi), "data": buf})), "write")
			}
			writes := time.Since(t0)
			t0 = time.Now()
			mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
			closeTook := time.Since(t0)
			// Cached mode uploads after the close; wait for the queue to drain.
			t0 = time.Now()
			deadline := time.Now().Add(10 * time.Minute)
			for {
				s.mu.RLock()
				v := s.vfses["nas"]
				s.mu.RUnlock()
				if b, _ := v.QueuedUploadBytes(); b == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("upload never drained")
				}
				time.Sleep(200 * time.Millisecond)
			}
			drained := time.Since(t0)
			total := float64(mib*int64(fs.Mebi)) / 1e6
			t.Logf("%s: %d MiB in %d writes: writes %v (%.1f MB/s), close %v, upload drained %v after close; end to end %.1f MB/s",
				mode.name, mib, mib, writes.Round(time.Millisecond), total/writes.Seconds(), closeTook.Round(time.Millisecond),
				drained.Round(time.Millisecond), total/(writes+closeTook+drained).Seconds())
			mustOK(t, s.handleRequest(req(t, 5, "remove", map[string]any{"dirId": dir, "itemId": id, "name": name})), "remove")
		})
	}
}

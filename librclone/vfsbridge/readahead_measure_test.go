package vfsbridge

// Measurement rig for the read-ahead policy, not a pass/fail test.
//
// Run with:
//
//	ES_VFSBRIDGE_MEASURE=1 go test -run TestMeasureReadAhead -v ./librclone/vfsbridge/
//
// It serves two 300MiB files over a loopback `rclone serve webdav` (the
// rclone binary on PATH, or ES_RCLONE_BIN) through a byte-counting TCP proxy,
// mounts that as a cached remote on a fresh bridge, and reports for both the
// adaptive policy and the old fixed 128MiB:
//
//   - probe: a 64KiB read at offset 0 of a cold file, then how many bytes the
//     backend actually sent once the downloader went idle;
//   - stream: a sequential read of the whole other file in 1MiB pieces, its
//     wall time, throughput and bytes sent.

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
)

const measureFileSize = int64(300 * fs.Mebi)

// countingProxy forwards one loopback port to another and counts the bytes
// the backend sends to its clients.
type countingProxy struct {
	ln       net.Listener
	fromBack atomic.Int64
}

func newCountingProxy(t *testing.T, backend string) *countingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &countingProxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				b, err := net.Dial("tcp", backend)
				if err != nil {
					return
				}
				defer b.Close()
				go func() { _, _ = io.Copy(b, c); _ = b.(*net.TCPConn).CloseWrite() }()
				buf := make([]byte, 256*1024)
				for {
					n, err := b.Read(buf)
					if n > 0 {
						p.fromBack.Add(int64(n))
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

// settled waits until the byte count has not moved for quiet, or until max.
func (p *countingProxy) settled(quiet, max time.Duration) int64 {
	deadline := time.Now().Add(max)
	last, since := p.fromBack.Load(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		if now := p.fromBack.Load(); now != last {
			last, since = now, time.Now()
		} else if time.Since(since) >= quiet {
			break
		}
	}
	return last
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func writePatternFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	for i := range buf {
		buf[i] = byte(i * 7)
	}
	for written := int64(0); written < size; written += int64(len(buf)) {
		if _, err := f.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMeasureReadAhead(t *testing.T) {
	if os.Getenv("ES_VFSBRIDGE_MEASURE") == "" {
		t.Skip("set ES_VFSBRIDGE_MEASURE=1 to run the read-ahead measurement")
	}
	if os.Getenv("ES_VFSBRIDGE_MEASURE_DEBUG") != "" {
		fs.GetConfig(context.Background()).LogLevel = fs.LogLevelDebug
	}
	rcloneBin := os.Getenv("ES_RCLONE_BIN")
	if rcloneBin == "" {
		rcloneBin = "rclone"
	}
	served := t.TempDir()
	writePatternFile(t, filepath.Join(served, "probe.bin"), measureFileSize)
	writePatternFile(t, filepath.Join(served, "stream.bin"), measureFileSize)

	port := freePort(t)
	serve := exec.Command(rcloneBin, "serve", "webdav", served,
		"--addr", fmt.Sprintf("127.0.0.1:%d", port), "--vfs-cache-mode", "off", "--log-level", "ERROR")
	serve.Env = append(os.Environ(), "RCLONE_CONFIG=/notfound")
	serve.Stdout, serve.Stderr = os.Stderr, os.Stderr
	if err := serve.Start(); err != nil {
		t.Fatalf("starting %s: %v", rcloneBin, err)
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
	proxy := newCountingProxy(t, fmt.Sprintf("127.0.0.1:%d", port))

	for _, mode := range []struct {
		name     string
		adaptive bool
	}{{"adaptive", true}, {"fixed-128MiB", false}} {
		t.Run(mode.name, func(t *testing.T) {
			readAheadAdaptive = mode.adaptive
			oldBuf := cachedBufferSize
			if !mode.adaptive {
				cachedBufferSize = fs.SizeSuffix(16 * fs.Mebi) // rclone's default, what shipped before
			}
			t.Cleanup(func() { readAheadAdaptive = true; cachedBufferSize = oldBuf })

			tmp := t.TempDir()
			confPath := filepath.Join(tmp, "rclone.conf")
			conf := fmt.Sprintf("[meas]\ntype = webdav\nurl = http://%s/\nvendor = other\n", proxy.ln.Addr())
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

			// Probe: 64KiB at offset 0, cold.
			probeID := childID(t, s, dir, "probe.bin")
			mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": probeID, "write": false})), "open")
			proxy.fromBack.Store(0)
			t0 := time.Now()
			r := mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": probeID, "offset": 0, "length": 64 * 1024})), "read")
			probeLatency := time.Since(t0)
			if len(r.Result.([]byte)) != 64*1024 {
				t.Fatalf("probe read returned %d bytes", len(r.Result.([]byte)))
			}
			probeBytes := proxy.settled(2*time.Second, 20*time.Second)
			mustOK(t, s.handleRequest(req(t, 3, "close", map[string]any{"itemId": probeID})), "close")
			t.Logf("%s probe: 64KiB read at 0 answered in %v; backend sent %.1f MiB before going idle; cache holds %.1f MiB",
				mode.name, probeLatency.Round(time.Millisecond), float64(probeBytes)/float64(fs.Mebi),
				float64(cacheBlocksOf(t, "probe.bin"))/float64(fs.Mebi))

			// Stream: whole file, sequential 1MiB reads, cold.
			streamID := childID(t, s, dir, "stream.bin")
			mustOK(t, s.handleRequest(req(t, 4, "open", map[string]any{"itemId": streamID, "write": false})), "open")
			proxy.fromBack.Store(0)
			t0 = time.Now()
			var off int64
			for off < measureFileSize {
				r := mustOK(t, s.handleRequest(req(t, 5, "read", map[string]any{"itemId": streamID, "offset": off, "length": int64(fs.Mebi)})), "stream read")
				n := len(r.Result.([]byte))
				if n == 0 {
					t.Fatalf("short read at %d", off)
				}
				off += int64(n)
			}
			elapsed := time.Since(t0)
			streamBytes := proxy.fromBack.Load()
			mustOK(t, s.handleRequest(req(t, 6, "close", map[string]any{"itemId": streamID})), "close")
			t.Logf("%s stream: %d MiB in %v = %.1f MB/s; backend sent %.1f MiB",
				mode.name, measureFileSize/int64(fs.Mebi), elapsed.Round(time.Millisecond),
				float64(measureFileSize)/elapsed.Seconds()/1e6, float64(streamBytes)/float64(fs.Mebi))
		})
	}
}

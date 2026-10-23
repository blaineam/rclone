package vfsbridge

// Measurement rig for the read-ahead policy, not a pass/fail test.
//
// Run with:
//
//	ES_VFSBRIDGE_MEASURE=1 go test -run TestMeasureReadAhead -v ./librclone/vfsbridge/
//
// It serves pattern files over a loopback `rclone serve webdav` (the rclone
// binary on PATH, or ES_RCLONE_BIN) through a TCP proxy that counts the bytes
// and requests the backend serves and can emulate a slow link
// (ES_VFSBRIDGE_MEASURE_RATE_MBPS caps backend-to-client bytes,
// ES_VFSBRIDGE_MEASURE_LATENCY_MS delays the first bytes of every response),
// mounts that as a cached remote on a fresh bridge, and reports for both the
// adaptive policy and the old fixed 128MiB:
//
//   - probe: a 64KiB read at offset 0 of a cold file, then how many bytes the
//     backend actually sent once the downloader went idle;
//   - moov: the shape of AVFoundation parsing a film's metadata — hundreds of
//     small reads walking forward through the first ~13 MiB with a few
//     re-reads behind the cursor;
//   - seeky: 64KiB reads covering the first 128 MiB of a cold file from four
//     cursors 32 MiB apart, round robin — a bulk reader that never looks
//     sequential to a seek detector;
//   - stream: a sequential read of the first streamMiB in 1MiB pieces, its
//     wall time, throughput and bytes sent.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
)

const (
	measureFileSize = int64(300 * fs.Mebi)
	seekyFileSize   = int64(160 * fs.Mebi)
	seekyRegion     = int64(128 * fs.Mebi)
	streamMiB       = int64(96)
)

// countingProxy forwards one loopback port to another, counts the bytes and
// the requests the backend serves, and optionally emulates a slow link.
type countingProxy struct {
	ln        net.Listener
	fromBack  atomic.Int64
	toBack    atomic.Int64 // bytes the client sent to the backend
	requests  atomic.Int64
	rateBytes int64         // bytes per second from the backend, 0 = unlimited
	latency   time.Duration // added before the first bytes of each response
	rateMu    sync.Mutex    // the token bucket below is shared by every connection
	rateStart time.Time
	rateSent  int64
}

// throttle delays the caller so that, across all connections, no more than
// rateBytes per second have passed since the first throttled byte.
func (p *countingProxy) throttle(n int) {
	if p.rateBytes <= 0 {
		return
	}
	p.rateMu.Lock()
	if p.rateStart.IsZero() {
		p.rateStart = time.Now()
	}
	p.rateSent += int64(n)
	ahead := time.Duration(float64(p.rateSent)/float64(p.rateBytes)*float64(time.Second)) - time.Since(p.rateStart)
	p.rateMu.Unlock()
	if ahead > 0 {
		time.Sleep(ahead)
	}
}

func newCountingProxy(t *testing.T, backend string) *countingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &countingProxy{ln: ln}
	if v := os.Getenv("ES_VFSBRIDGE_MEASURE_RATE_MBPS"); v != "" {
		mbps, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatal(err)
		}
		p.rateBytes = int64(mbps * 1e6)
	}
	if v := os.Getenv("ES_VFSBRIDGE_MEASURE_LATENCY_MS"); v != "" {
		ms, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatal(err)
		}
		p.latency = time.Duration(ms * float64(time.Millisecond))
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c, backend)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *countingProxy) serve(c net.Conn, backend string) {
	defer c.Close()
	b, err := net.Dial("tcp", backend)
	if err != nil {
		return
	}
	defer b.Close()
	// Requests seen on this connection and responses started, so the latency
	// is charged once per request.
	var seen, started atomic.Int64
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				seen.Add(1)
				p.requests.Add(1)
				p.toBack.Add(int64(n))
				if _, werr := b.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		_ = b.(*net.TCPConn).CloseWrite()
	}()
	buf := make([]byte, 256*1024)
	for {
		n, err := b.Read(buf)
		if n > 0 {
			if s := seen.Load(); s > started.Load() {
				started.Store(s)
				if p.latency > 0 {
					time.Sleep(p.latency)
				}
			}
			p.throttle(n)
			p.fromBack.Add(int64(n))
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
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

func (p *countingProxy) reset() {
	p.fromBack.Store(0)
	p.toBack.Store(0)
	p.requests.Store(0)
	p.rateMu.Lock()
	p.rateStart, p.rateSent = time.Time{}, 0
	p.rateMu.Unlock()
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
	writePatternFile(t, filepath.Join(served, "moov.bin"), 64*int64(fs.Mebi))
	writePatternFile(t, filepath.Join(served, "avf.bin"), 256*int64(fs.Mebi))
	writePatternFile(t, filepath.Join(served, "seeky.bin"), seekyFileSize)
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
	t.Logf("link: rate %s, latency %v per request", func() string {
		if proxy.rateBytes == 0 {
			return "unlimited"
		}
		return fmt.Sprintf("%.1f MB/s", float64(proxy.rateBytes)/1e6)
	}(), proxy.latency)

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

			read := func(id uint64, off, n int64) int {
				r := mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": id, "offset": off, "length": n})), "read")
				return len(r.Result.([]byte))
			}
			open := func(name string) uint64 {
				id := childID(t, s, dir, name)
				mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": id, "write": false})), "open")
				return id
			}
			closeF := func(id uint64) {
				mustOK(t, s.handleRequest(req(t, 3, "close", map[string]any{"itemId": id})), "close")
			}

			// Probe: 64KiB at offset 0, cold.
			probeID := open("probe.bin")
			proxy.reset()
			t0 := time.Now()
			if n := read(probeID, 0, 64*1024); n != 64*1024 {
				t.Fatalf("probe read returned %d bytes", n)
			}
			probeLatency := time.Since(t0)
			probeBytes := proxy.settled(2*time.Second, 20*time.Second)
			closeF(probeID)
			t.Logf("%s probe: 64KiB read at 0 answered in %v; backend sent %.1f MiB in %d requests before going idle; cache holds %.1f MiB",
				mode.name, probeLatency.Round(time.Millisecond), float64(probeBytes)/float64(fs.Mebi), proxy.requests.Load(),
				float64(cacheBlocksOf(t, "probe.bin"))/float64(fs.Mebi))

			// Moov: AVFoundation's metadata parse. 4KiB at 0 (ftyp), then 48KiB
			// reads walking from 512KiB to ~12.5MiB (a moov that holds a
			// 10.9MB cover), with a 16KiB re-read behind the cursor every 64
			// reads, then three 64KiB reads just past the end.
			moovID := open("moov.bin")
			proxy.reset()
			t0 = time.Now()
			read(moovID, 0, 4*1024)
			off := int64(512 * fs.Kibi)
			runStart := off
			reads := 1
			for off < int64(12*fs.Mebi)+int64(512*fs.Kibi) {
				read(moovID, off, 48*1024)
				reads++
				off += 48 * 1024
				if reads%64 == 0 {
					read(moovID, runStart+int64(reads/64)*64*1024, 16*1024)
					reads++
				}
			}
			for i := 0; i < 3; i++ {
				read(moovID, off, 64*1024)
				reads++
				off += 64 * 1024
			}
			moovElapsed := time.Since(t0)
			moovBytes := proxy.settled(2*time.Second, 30*time.Second)
			moovReqs := proxy.requests.Load()
			closeF(moovID)
			t.Logf("%s moov: %d reads over the first %.1f MiB in %v; backend sent %.1f MiB in %d requests",
				mode.name, reads, float64(off)/float64(fs.Mebi), moovElapsed.Round(time.Millisecond),
				float64(moovBytes)/float64(fs.Mebi), moovReqs)

			// AVFoundation replay: the byte ranges a real metadata load asked
			// for (see docs/FSKIT.md §25), replayed in order against a cold
			// file. One read per line of ES_VFSBRIDGE_MEASURE_PATTERN.
			if pattern := os.Getenv("ES_VFSBRIDGE_MEASURE_PATTERN"); pattern != "" {
				avfID := open("avf.bin")
				proxy.reset()
				t0 = time.Now()
				var avfReads, avfBytes int64
				var maxOff int64
				for _, p := range loadPattern(t, pattern) {
					read(avfID, p[0], p[1])
					avfReads++
					avfBytes += p[1]
					maxOff = max(maxOff, p[0]+p[1])
				}
				avfElapsed := time.Since(t0)
				avfSent := proxy.settled(2*time.Second, 30*time.Second)
				avfReqs := proxy.requests.Load()
				closeF(avfID)
				t.Logf("%s avf: %d reads, %.1f MiB consumed, furthest byte %.1f MiB, in %v; backend sent %.1f MiB in %d requests",
					mode.name, avfReads, float64(avfBytes)/float64(fs.Mebi), float64(maxOff)/float64(fs.Mebi),
					avfElapsed.Round(time.Millisecond), float64(avfSent)/float64(fs.Mebi), avfReqs)
			}

			// Seeky bulk: 64KiB reads covering the first 128MiB from four
			// cursors 32MiB apart, four reads per cursor per round.
			seekyID := open("seeky.bin")
			proxy.reset()
			t0 = time.Now()
			const piece = int64(64 * fs.Kibi)
			cursors := [4]int64{0, 32 * int64(fs.Mebi), 64 * int64(fs.Mebi), 96 * int64(fs.Mebi)}
			ends := [4]int64{32 * int64(fs.Mebi), 64 * int64(fs.Mebi), 96 * int64(fs.Mebi), seekyRegion}
			var seekyReads int
			for done := false; !done; {
				done = true
				for c := range cursors {
					for i := 0; i < 4 && cursors[c] < ends[c]; i++ {
						read(seekyID, cursors[c], piece)
						seekyReads++
						cursors[c] += piece
					}
					if cursors[c] < ends[c] {
						done = false
					}
				}
			}
			seekyElapsed := time.Since(t0)
			seekyBytes := proxy.fromBack.Load()
			// The cursors stop mid-file, so their downloaders run on to the end
			// of their windows after the last read; wait that out so it is
			// counted here and not against the stream that follows.
			seekySettled := proxy.settled(2*time.Second, 120*time.Second)
			seekyReqs := proxy.requests.Load()
			closeF(seekyID)
			t.Logf("%s seeky: %d MiB in %d reads of 64KiB from 4 cursors in %v = %.1f MB/s; backend sent %.1f MiB by the last read, %.1f MiB once idle, in %d requests",
				mode.name, seekyRegion/int64(fs.Mebi), seekyReads, seekyElapsed.Round(time.Millisecond),
				float64(seekyRegion)/seekyElapsed.Seconds()/1e6, float64(seekyBytes)/float64(fs.Mebi), float64(seekySettled)/float64(fs.Mebi), seekyReqs)

			// Stream: the first streamMiB sequentially, 1MiB reads, cold.
			streamID := open("stream.bin")
			proxy.reset()
			t0 = time.Now()
			total := streamMiB * int64(fs.Mebi)
			for off = 0; off < total; {
				n := read(streamID, off, int64(fs.Mebi))
				if n == 0 {
					t.Fatalf("short read at %d", off)
				}
				off += int64(n)
			}
			elapsed := time.Since(t0)
			streamBytes := proxy.fromBack.Load()
			closeF(streamID)
			t.Logf("%s stream: %d MiB in %v = %.1f MB/s; backend sent %.1f MiB in %d requests",
				mode.name, streamMiB, elapsed.Round(time.Millisecond),
				float64(total)/elapsed.Seconds()/1e6, float64(streamBytes)/float64(fs.Mebi), proxy.requests.Load())
		})
	}
}

// loadPattern reads "offset length" pairs, one per line, skipping blanks and
// lines starting with #.
func loadPattern(t *testing.T, path string) [][2]int64 {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]int64
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var off, n int64
		if _, err := fmt.Sscanf(line, "%d %d", &off, &n); err != nil {
			t.Fatalf("pattern line %q: %v", line, err)
		}
		out = append(out, [2]int64{off, n})
	}
	return out
}

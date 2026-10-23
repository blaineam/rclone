package vfsbridge

// Tests for in-place edits on a streaming remote: an existing file opened for
// writing is patched with ranged writes that go straight to the remote, so a
// patch costs the bytes patched, fsync comes back when the remote has them,
// and the file's size follows appends and truncates. Where the remote cannot
// write at an offset the open is refused with EPERM and the remote untouched.
//
// The sequence driven here is exactly Lathe's in-place moov rewrite (append a
// block at EOF, fsync, write 8 bytes at a mid-file offset, fsync, truncate to
// a new length, read back), which failed on the Enter Space volume at the
// first fsync with EIO (2026-10-23).

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/crypt"
	_ "github.com/rclone/rclone/backend/sftp"
	_ "github.com/rclone/rclone/cmd/serve/sftp"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/rc"
)

// latheSequence drives Lathe's commit sequence against item id through the
// bridge and checks what the bridge itself reports along the way. size is the
// file's size before; block is appended, patch is written at patchAt, and the
// file is cut to newSize. Every step must succeed.
func latheSequence(t *testing.T, s *Server, id uint64, size int64, block, patch []byte, patchAt, newSize int64) {
	t.Helper()
	mustOK(t, s.handleRequest(req(t, 10, "open", map[string]any{"itemId": id, "write": true})), "open for writing an existing file")

	// append a block at EOF
	r := mustOK(t, s.handleRequest(req(t, 11, "write", map[string]any{"itemId": id, "offset": size, "data": block})), "append")
	if n := int(r.Result.(int)); n != len(block) {
		t.Fatalf("append wrote %d of %d", n, len(block))
	}
	start := time.Now()
	mustOK(t, s.handleRequest(req(t, 12, "sync", nil)), "fsync after the append")
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("fsync took %v with the file still open: it waited for the handle to close", took)
	}
	r = mustOK(t, s.handleRequest(req(t, 13, "getattr", map[string]any{"itemId": id})), "getattr")
	if got := r.Result.(ItemInfo).Size; got != size+int64(len(block)) {
		t.Fatalf("size after append %d, want %d", got, size+int64(len(block)))
	}

	// patch bytes in the middle
	mustOK(t, s.handleRequest(req(t, 14, "write", map[string]any{"itemId": id, "offset": patchAt, "data": patch})), "patch")
	mustOK(t, s.handleRequest(req(t, 15, "sync", nil)), "fsync after the patch")

	// read back through the volume while the handle is open
	r = mustOK(t, s.handleRequest(req(t, 16, "read", map[string]any{"itemId": id, "offset": patchAt, "length": len(patch)})), "read the patch back")
	if got := r.Result.([]byte); !bytes.Equal(got, patch) {
		t.Fatalf("read back %q, want %q", got, patch)
	}

	// truncate to the new length
	r = mustOK(t, s.handleRequest(req(t, 17, "setattr", map[string]any{"itemId": id, "size": newSize})), "truncate")
	if got := r.Result.(ItemInfo).Size; got != newSize {
		t.Fatalf("size after truncate %d, want %d", got, newSize)
	}
	mustOK(t, s.handleRequest(req(t, 18, "close", map[string]any{"itemId": id})), "close")
	r = mustOK(t, s.handleRequest(req(t, 19, "getattr", map[string]any{"itemId": id})), "getattr after close")
	if got := r.Result.(ItemInfo).Size; got != newSize {
		t.Fatalf("size after close %d, want %d", got, newSize)
	}
}

func inPlaceOf(s *Server, remote string) bool {
	for _, r := range s.UploadStatus().Remotes {
		if r.Remote == remote {
			return r.WriteInPlace
		}
	}
	return false
}

// A local-backed streaming remote: the edit lands on disk, byte for byte,
// and nothing else about the file changes.
func TestInPlaceEditOnStreamingRemote(t *testing.T) {
	s, backingA := streamingServer(t)
	if !inPlaceOf(s, "rem_a") {
		t.Fatal("rem_a should report in-place edits: the local backend can write at an offset")
	}
	if inPlaceOf(s, "rem_b") {
		t.Fatal("rem_b is cached, not streaming; in-place is a streaming-mode property")
	}

	// An existing file with content, placed on the remote behind the volume's back.
	original := payloadOf(1<<20, 'o')
	for i := range original {
		original[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(backingA, "film.bin"), original, 0o644); err != nil {
		t.Fatal(err)
	}
	dir := remoteRootID(t, s, "rem_a")
	id := childID(t, s, dir, "film.bin")

	block := payloadOf(64<<10, 'B')
	patch := []byte("freemoov")
	patchAt := int64(512 << 10)
	newSize := int64(1<<20) + 32<<10
	latheSequence(t, s, id, int64(len(original)), block, patch, patchAt, newSize)

	got, err := os.ReadFile(filepath.Join(backingA, "film.bin"))
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, original...), block...)[:newSize]
	copy(want[patchAt:], patch)
	if !bytes.Equal(got, want) {
		t.Fatalf("the file on disk is not the original with the block appended, the patch applied and the tail cut: %d bytes, first difference at %d", len(got), firstDiff(got, want))
	}

	// A second edit of the same file, after the first closed.
	mustOK(t, s.handleRequest(req(t, 20, "open", map[string]any{"itemId": id, "write": true})), "re-open")
	mustOK(t, s.handleRequest(req(t, 21, "write", map[string]any{"itemId": id, "offset": 0, "data": []byte("again")})), "write")
	mustOK(t, s.handleRequest(req(t, 22, "close", map[string]any{"itemId": id})), "close")
	got, _ = os.ReadFile(filepath.Join(backingA, "film.bin"))
	if len(got) != int(newSize) || string(got[:5]) != "again" {
		t.Fatalf("second edit: %d bytes, head %q", len(got), got[:5])
	}

	// A truncate with no handle open works too.
	mustOK(t, s.handleRequest(req(t, 23, "setattr", map[string]any{"itemId": id, "size": 100})), "truncate without a handle")
	if st, err := os.Stat(filepath.Join(backingA, "film.bin")); err != nil || st.Size() != 100 {
		t.Fatalf("after truncate without a handle: %v %d", err, st.Size())
	}
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

// A streaming remote whose backend cannot write at an offset -- a crypt here,
// where a byte patch would break the cipher stream -- refuses the open with
// EPERM and leaves the file alone. New files and overwrites still work.
func TestInPlaceEditRefusedWhereTheBackendCannot(t *testing.T) {
	tmp := t.TempDir()
	vault := filepath.Join(tmp, "vault")
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	conf := "[rem_a]\ntype = alias\nremote = " + filepath.Join(tmp, "plain") + "\n\n" +
		"[enc]\ntype = crypt\nremote = " + vault + "\npassword = " + obscure.MustObscure(hex.EncodeToString(raw)) + "\n"
	confPath := filepath.Join(tmp, "rclone.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "plain"), 0o755); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	cache.Clear()
	useTempCacheDir(t)

	s := NewServer()
	s.SetWriteThrough("enc", true)
	for _, name := range []string{"rem_a", "enc"} {
		if err := s.AddRemote(name); err != nil {
			t.Fatalf("AddRemote %s: %v", name, err)
		}
	}
	t.Cleanup(s.Stop)
	if modeOf(s, "enc") != "through" {
		t.Fatalf("enc mode %q, want through: crypt over local can stream", modeOf(s, "enc"))
	}
	if inPlaceOf(s, "enc") {
		t.Fatal("a crypt remote must not offer in-place edits: a byte patch breaks the cipher stream")
	}

	dir := remoteRootID(t, s, "enc")
	r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": "doc.txt", "isDir": false})), "create")
	id := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open new")
	mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": 0, "data": []byte("version one")})), "write")
	mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")

	entries, err := os.ReadDir(vault)
	if err != nil || len(entries) != 1 {
		t.Fatalf("vault holds %d entries (%v), want the one encrypted file", len(entries), err)
	}
	before, _ := os.Stat(filepath.Join(vault, entries[0].Name()))

	// The in-place edit: refused at open, cleanly.
	resp := s.handleRequest(req(t, 5, "open", map[string]any{"itemId": id, "write": true}))
	if resp.Ok || resp.Error != cEPERM {
		t.Fatalf("open for writing of an existing file on a crypt streaming remote: ok=%v errno=%d, want EPERM", resp.Ok, resp.Error)
	}
	// and a positioned write with no handle is ENOENT, as before
	resp = s.handleRequest(req(t, 6, "write", map[string]any{"itemId": id, "offset": 4, "data": []byte("X")}))
	if resp.Ok || resp.Error != cENOENT {
		t.Fatalf("write with no handle: ok=%v errno=%d", resp.Ok, resp.Error)
	}
	after, _ := os.Stat(filepath.Join(vault, entries[0].Name()))
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("the refused edit touched the encrypted file")
	}

	// An overwrite (truncate to 0, then write) still replaces it.
	mustOK(t, s.handleRequest(req(t, 7, "setattr", map[string]any{"itemId": id, "size": 0})), "truncate to 0")
	mustOK(t, s.handleRequest(req(t, 8, "open", map[string]any{"itemId": id, "write": true})), "open after truncate")
	mustOK(t, s.handleRequest(req(t, 9, "write", map[string]any{"itemId": id, "offset": 0, "data": []byte("version two")})), "write")
	mustOK(t, s.handleRequest(req(t, 10, "close", map[string]any{"itemId": id})), "close")
	r = mustOK(t, s.handleRequest(req(t, 11, "read", map[string]any{"itemId": id, "offset": 0, "length": 32})), "read")
	if got := string(r.Result.([]byte)); got != "version two" {
		t.Fatalf("after the overwrite the file reads %q", got)
	}
}

// newSFTPServer serves dir over an in-process `rclone serve sftp` (this
// fork's, with its VFS in cache mode off and range writes on, so the server
// itself writes at an offset into the local file) behind a byte-counting
// proxy, and returns the proxy and the rclone.conf text of a remote that
// reaches it through the proxy.
func newSFTPServer(t *testing.T, dir string) (*countingProxy, string) {
	t.Helper()
	const user, pass = "lathe", "moov"
	call := rc.Calls.Get("serve/start")
	if call == nil {
		t.Fatal("serve/start is not registered")
	}
	out, err := call.Fn(context.Background(), rc.Params{
		"type": "sftp", "fs": dir, "addr": "127.0.0.1:0",
		"user": user, "pass": pass,
		"vfs_cache_mode": "off", "vfs_range_writes": true,
	})
	if err != nil {
		t.Fatalf("serve sftp: %v", err)
	}
	t.Cleanup(func() {
		_, _ = rc.Calls.Get("serve/stop").Fn(context.Background(), rc.Params{"id": out["id"]})
	})
	proxy := newCountingProxy(t, out["addr"].(string))
	_, port, _ := netSplitHostPort(proxy.ln.Addr().String())
	conf := fmt.Sprintf("[sftp_a]\ntype = sftp\nhost = 127.0.0.1\nport = %s\nuser = %s\npass = %s\nknown_hosts_file = none\nshell_type = none\n",
		port, user, obscure.MustObscure(pass))
	return proxy, conf
}

func netSplitHostPort(addr string) (host, port string, err error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("no port in %q", addr)
}

// The point of the feature, measured: Lathe's sequence on a multi-GB film over
// SFTP moves about as many bytes as it patches. The film is a sparse fixture,
// so the test costs no disk; the proxy counts what crosses the wire.
func TestInPlaceEditOverSFTPCostsOnlyThePatch(t *testing.T) {
	served := t.TempDir()
	const filmSize = int64(3 << 30) // 3 GiB, sparse
	filmPath := filepath.Join(served, "film.mp4")
	f, err := os.Create(filmPath)
	if err != nil {
		t.Fatal(err)
	}
	// A recognisable stripe near the patch and at the end, so a wrong offset shows.
	if _, err := f.WriteAt(payloadOf(4096, 'm'), filmSize/2-2048); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(filmSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	proxy, conf := newSFTPServer(t, served)
	confPath := filepath.Join(t.TempDir(), "rclone.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	cache.Clear()
	useTempCacheDir(t)

	s := NewServer()
	s.SetWriteThrough("sftp_a", true)
	if err := s.AddRemote("sftp_a"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	if modeOf(s, "sftp_a") != "through" || !inPlaceOf(s, "sftp_a") {
		t.Fatalf("sftp_a: mode %q in-place %v, want through + in-place", modeOf(s, "sftp_a"), inPlaceOf(s, "sftp_a"))
	}

	dir := remoteRootID(t, s, "sftp_a")
	id := childID(t, s, dir, "film.mp4")
	r := mustOK(t, s.handleRequest(req(t, 1, "getattr", map[string]any{"itemId": id})), "getattr")
	if got := r.Result.(ItemInfo).Size; got != filmSize {
		t.Fatalf("film is %d bytes over sftp, want %d", got, filmSize)
	}

	// Lathe: a 2.5 MB moov appended, 8 bytes relabelled, the tail cut.
	block := payloadOf(2500<<10, 'M')
	patch := []byte{0, 0, 0, 8, 'f', 'r', 'e', 'e'}
	patchAt := filmSize / 2
	newSize := filmSize + int64(len(block)) - 300<<10
	proxy.reset()
	start := time.Now()
	latheSequence(t, s, id, filmSize, block, patch, patchAt, newSize)
	took := time.Since(start)
	sent, received := proxy.toBack.Load(), proxy.fromBack.Load()
	t.Logf("3 GiB film over loopback sftp: appended %d + patched %d bytes, truncated; %d bytes sent, %d received, %v",
		len(block), len(patch), sent, received, took.Round(time.Millisecond))

	patched := int64(len(block) + len(patch))
	if sent < patched {
		t.Fatalf("sent %d bytes, fewer than the %d patched: the writes did not go to the server", sent, patched)
	}
	// SFTP framing is a few dozen bytes per 32 KiB-255 KiB chunk, plus the
	// open/stat/close exchanges and the SSH handshake. Anything near the film's
	// size would mean a download or a re-upload.
	if overhead := sent - patched; overhead > 512<<10 {
		t.Fatalf("sent %d bytes for %d patched: %d of overhead, which is more than ranged writes cost", sent, patched, overhead)
	}
	if received > 512<<10 {
		t.Fatalf("received %d bytes from the server: the edit should not read the film", received)
	}
	if took > 20*time.Second {
		t.Fatalf("the edit took %v", took)
	}

	// The file on disk: size, the appended block, the patch, the stripe beside it.
	st, err := os.Stat(filmPath)
	if err != nil || st.Size() != newSize {
		t.Fatalf("film on disk: %v, %d bytes, want %d", err, st.Size(), newSize)
	}
	g, err := os.Open(filmPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	check := func(off int64, want []byte, what string) {
		got := make([]byte, len(want))
		if _, err := g.ReadAt(got, off); err != nil {
			t.Fatalf("%s: read at %d: %v", what, off, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: bytes at %d differ, first at +%d", what, off, firstDiff(got, want))
		}
	}
	check(patchAt, patch, "the patch")
	check(patchAt-2048, payloadOf(2048, 'm'), "the stripe before the patch")
	check(patchAt+8, payloadOf(2040, 'm'), "the stripe after the patch")
	check(filmSize, block[:newSize-filmSize], "the appended block up to the cut")
	check(filmSize-4096, make([]byte, 4096), "the zeros before the old end")
}

var _ = fs.Debugf

// A new file copied onto a streaming remote the way the kernel does it --
// create, open for writing, sequential writes, close, then renamed over an
// existing film -- must keep streaming with range writes available, including
// when a second open for writing arrives mid-copy and when the copier sets the
// size first. Sami's copy-beside fallback did exactly this and failed EIO on
// the first build with range writes (2026-10-23).
func TestStreamingCopyStillStreamsWithRangeWrites(t *testing.T) {
	s, backingA := streamingServer(t)
	dir := remoteRootID(t, s, "rem_a")
	if err := os.WriteFile(filepath.Join(backingA, "film.mp4"), []byte("the old film"), 0o644); err != nil {
		t.Fatal(err)
	}
	filmID := childID(t, s, dir, "film.mp4")

	part := payloadOf(1<<20, 'p')
	copyIn := func(t *testing.T, name string, midCopy func(id uint64)) uint64 {
		t.Helper()
		r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": name, "isDir": false})), "create")
		id := r.Result.(ItemInfo).ID
		mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
		for i := 0; i < 4; i++ {
			mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": i * len(part), "data": part})), fmt.Sprintf("write %d", i))
			if i == 1 && midCopy != nil {
				midCopy(id)
			}
		}
		mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
		got, err := os.ReadFile(filepath.Join(backingA, name))
		if err != nil || len(got) != 4<<20 || !bytes.Equal(got, bytes.Repeat(part, 4)) {
			t.Fatalf("%s on the remote: %d bytes, err %v", name, len(got), err)
		}
		return id
	}

	t.Run("plain", func(t *testing.T) {
		id := copyIn(t, ".film.mp4.sami-tags", nil)
		mustOK(t, s.handleRequest(req(t, 5, "rename", map[string]any{
			"srcDirId": dir, "srcName": ".film.mp4.sami-tags", "srcItemId": id,
			"dstDirId": dir, "dstName": "film.mp4", "dstItemId": filmID})), "rename over the film")
		got, _ := os.ReadFile(filepath.Join(backingA, "film.mp4"))
		if len(got) != 4<<20 {
			t.Fatalf("after the rename the film is %d bytes", len(got))
		}
		if _, err := os.Stat(filepath.Join(backingA, ".film.mp4.sami-tags")); !os.IsNotExist(err) {
			t.Fatalf("the sibling is still there: %v", err)
		}
	})

	t.Run("second open for writing mid-copy", func(t *testing.T) {
		copyIn(t, "second-open.bin", func(id uint64) {
			// The kernel opens a file being copied a second time; whatever it
			// gets must not take over the copy's writes. A write through the
			// second handle fails as it always did (EPERM); the copy's own
			// writes keep streaming.
			mustOK(t, s.handleRequest(req(t, 6, "open", map[string]any{"itemId": id, "write": true})), "second open")
			mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": id})), "second close")
		})
	})

	t.Run("size set before the data arrives", func(t *testing.T) {
		// copyfile may preallocate: a truncate to the final size right after
		// the create, before any write. An empty file has nothing for a range
		// write to preserve, so this is refused exactly as before (EPERM) and
		// the copy that follows streams; the data must land and the size be right.
		r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": "prealloc.bin", "isDir": false})), "create")
		id := r.Result.(ItemInfo).ID
		resp := s.handleRequest(req(t, 8, "setattr", map[string]any{"itemId": id, "size": 4 << 20}))
		if resp.Ok || resp.Error != cEPERM {
			t.Fatalf("preallocating an empty file on a streaming remote: ok=%v errno=%d, want EPERM as before", resp.Ok, resp.Error)
		}
		mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
		for i := 0; i < 4; i++ {
			mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": i * len(part), "data": part})), fmt.Sprintf("write %d", i))
		}
		mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")
		got, err := os.ReadFile(filepath.Join(backingA, "prealloc.bin"))
		if err != nil || !bytes.Equal(got, bytes.Repeat(part, 4)) {
			t.Fatalf("preallocated copy on the remote: %d bytes, err %v (setattr ok=%v errno=%d)", len(got), err, resp.Ok, resp.Error)
		}
	})
}

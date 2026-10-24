package vfsbridge

// Which remotes can edit a file in place: every backend whose storage writes
// at an offset, through the wrappers that pass bytes through untouched (a
// union forwards to the one upstream holding the file), and not through the
// wrappers whose stored form a byte patch would break. All on top of a local
// directory, so this runs anywhere.

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/rclone/rclone/backend/archive"
	_ "github.com/rclone/rclone/backend/chunker"
	_ "github.com/rclone/rclone/backend/combine"
	_ "github.com/rclone/rclone/backend/compress"
	_ "github.com/rclone/rclone/backend/hasher"
	_ "github.com/rclone/rclone/backend/union"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/obscure"
)

func TestInPlaceEditsThroughWrappers(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "base")
	base2 := filepath.Join(tmp, "base2")
	for _, d := range []string{filepath.Join(base, "sub"), base2} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	conf := "[base]\ntype = alias\nremote = " + base + "\n\n" +
		"[base2]\ntype = alias\nremote = " + base2 + "\n\n" +
		"[w_combine]\ntype = combine\nupstreams = \"dir=base:sub\"\n\n" +
		"[w_hasher]\ntype = hasher\nremote = base:\n\n" +
		"[w_archive]\ntype = archive\nremote = base:\n\n" +
		"[w_union]\ntype = union\nupstreams = base: base2:\n\n" +
		"[w_chunker]\ntype = chunker\nremote = base:\nchunk_size = 1k\n\n" +
		"[w_compress]\ntype = compress\nremote = base:\n\n" +
		"[w_crypt]\ntype = crypt\nremote = base:\npassword = " + obscure.MustObscure(hex.EncodeToString(raw)) + "\n"
	confPath := filepath.Join(tmp, "rclone.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	cache.Clear()
	useTempCacheDir(t)

	cases := []struct {
		remote  string
		inPlace bool
		why     string
	}{
		{"w_combine", true, "a combine maps paths to an upstream and passes bytes through"},
		{"w_hasher", true, "a hasher passes bytes through and drops the stale hashes"},
		{"w_archive", true, "an archive passes files outside archives through"},
		{"w_union", true, "a union forwards to the one upstream that holds the file"},
		{"w_chunker", false, "a chunker splits the file into parts"},
		{"w_compress", false, "a byte patch inside a compressed stream corrupts it"},
		{"w_crypt", false, "a byte patch inside a cipher stream corrupts it"},
	}
	s := NewServer()
	for _, c := range cases {
		s.SetWriteThrough(c.remote, true)
		if err := s.AddRemote(c.remote); err != nil {
			t.Fatalf("AddRemote %s: %v", c.remote, err)
		}
	}
	t.Cleanup(s.Stop)

	for _, c := range cases {
		t.Run(c.remote, func(t *testing.T) {
			if modeOf(s, c.remote) != "through" {
				t.Fatalf("%s mode %q, want through", c.remote, modeOf(s, c.remote))
			}
			if got := inPlaceOf(s, c.remote); got != c.inPlace {
				t.Fatalf("%s reports in-place %v, want %v: %s", c.remote, got, c.inPlace, c.why)
			}
			dir := remoteRootID(t, s, c.remote)
			if c.remote == "w_combine" {
				dir = childID(t, s, dir, "dir")
			}
			// A file written through the remote, so the wrapper's own format is
			// what is on disk; then the in-place edit.
			r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": "doc.bin", "isDir": false})), "create")
			id := r.Result.(ItemInfo).ID
			mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
			mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": 0, "data": payloadOf(4096, 'o')})), "write")
			mustOK(t, s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id})), "close")

			resp := s.handleRequest(req(t, 5, "open", map[string]any{"itemId": id, "write": true}))
			if !c.inPlace {
				if resp.Ok || resp.Error != cEPERM {
					t.Fatalf("open for an in-place edit on %s: ok=%v errno=%d, want EPERM (%s)", c.remote, resp.Ok, resp.Error, c.why)
				}
				r = mustOK(t, s.handleRequest(req(t, 6, "read", map[string]any{"itemId": id, "offset": 0, "length": 4096})), "read")
				if got := r.Result.([]byte); string(got) != string(payloadOf(4096, 'o')) {
					t.Fatalf("the refused edit changed the file")
				}
				return
			}
			mustOK(t, resp, "open for an in-place edit on "+c.remote)
			mustOK(t, s.handleRequest(req(t, 6, "write", map[string]any{"itemId": id, "offset": 2048, "data": []byte("PATCH")})), "patch")
			mustOK(t, s.handleRequest(req(t, 7, "write", map[string]any{"itemId": id, "offset": 4096, "data": []byte("TAIL")})), "append")
			mustOK(t, s.handleRequest(req(t, 8, "close", map[string]any{"itemId": id})), "close")
			r = mustOK(t, s.handleRequest(req(t, 9, "read", map[string]any{"itemId": id, "offset": 2040, "length": 2060})), "read back")
			got := r.Result.([]byte)
			want := append(append(append([]byte{}, payloadOf(8, 'o')...), []byte("PATCH")...), payloadOf(2043, 'o')...)
			want = append(want, []byte("TAIL")...)
			if string(got) != string(want) {
				t.Fatalf("after the edit on %s the file reads %q…", c.remote, got[:16])
			}
		})
	}
}

// The person can switch in-place edits off per remote; the remote then behaves
// as before (EPERM on the open), and on again.
func TestInPlaceEditsCanBeSwitchedOffPerRemote(t *testing.T) {
	s, backingA := streamingServer(t)
	if err := os.WriteFile(filepath.Join(backingA, "f.txt"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.SetInPlaceEdits("rem_a", false)
	if err := s.RefreshRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	if inPlaceOf(s, "rem_a") {
		t.Fatal("rem_a still reports in-place edits after opting out")
	}
	dir := remoteRootID(t, s, "rem_a")
	id := childID(t, s, dir, "f.txt")
	// As before the feature: the open is accepted (rclone opens a streaming
	// handle lazily) and the first write into the existing content is EPERM;
	// the file on the remote is untouched.
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": id, "write": true})), "open while opted out")
	if resp := s.handleRequest(req(t, 2, "write", map[string]any{"itemId": id, "offset": 0, "data": []byte("X")})); resp.Ok || resp.Error != cEPERM {
		t.Fatalf("opted out: write ok=%v errno=%d, want EPERM", resp.Ok, resp.Error)
	}
	_ = s.handleRequest(req(t, 3, "close", map[string]any{"itemId": id}))
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "content" {
		t.Fatalf("opted out: the refused edit changed the file to %q", got)
	}
	s.SetInPlaceEdits("rem_a", true)
	if err := s.RefreshRemote("rem_a"); err != nil {
		t.Fatal(err)
	}
	if !inPlaceOf(s, "rem_a") {
		t.Fatal("rem_a does not report in-place edits after opting back in")
	}
	mustOK(t, s.handleRequest(req(t, 4, "open", map[string]any{"itemId": id, "write": true})), "open after opting back in")
	mustOK(t, s.handleRequest(req(t, 5, "write", map[string]any{"itemId": id, "offset": 0, "data": []byte("C")})), "write after opting back in")
	mustOK(t, s.handleRequest(req(t, 6, "close", map[string]any{"itemId": id})), "close")
	if got, _ := os.ReadFile(filepath.Join(backingA, "f.txt")); string(got) != "Content" {
		t.Fatalf("opted back in: the file reads %q", got)
	}
}

// A reader with its own handle open sees a patch while the editor's handle is
// still open: reads of the item go through the editing handle to the live object.
func TestReadersSeeAnInPlaceEditWhileItIsOpen(t *testing.T) {
	s, backingA := streamingServer(t)
	if err := os.WriteFile(filepath.Join(backingA, "f.bin"), payloadOf(64<<10, 'a'), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := remoteRootID(t, s, "rem_a")
	id := childID(t, s, dir, "f.bin")
	// A reader opens first and reads the head (its buffer now holds old bytes).
	mustOK(t, s.handleRequest(req(t, 1, "open", map[string]any{"itemId": id, "write": false})), "reader open")
	mustOK(t, s.handleRequest(req(t, 2, "read", map[string]any{"itemId": id, "offset": 0, "length": 4096})), "reader read")
	// The editor opens and patches.
	mustOK(t, s.handleRequest(req(t, 3, "open", map[string]any{"itemId": id, "write": true})), "editor open")
	mustOK(t, s.handleRequest(req(t, 4, "write", map[string]any{"itemId": id, "offset": 100, "data": []byte("NEW")})), "patch")
	r := mustOK(t, s.handleRequest(req(t, 5, "read", map[string]any{"itemId": id, "offset": 98, "length": 7})), "read during the edit")
	if got := string(r.Result.([]byte)); got != "aaNEWaa" {
		t.Fatalf("a read while the edit is open gave %q, want aaNEWaa", got)
	}
	mustOK(t, s.handleRequest(req(t, 6, "close", map[string]any{"itemId": id})), "editor close")
	mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": id})), "reader close")
	r = mustOK(t, s.handleRequest(req(t, 8, "read", map[string]any{"itemId": id, "offset": 98, "length": 7})), "read after")
	if got := string(r.Result.([]byte)); got != "aaNEWaa" {
		t.Fatalf("a read after the edit gave %q", got)
	}
}

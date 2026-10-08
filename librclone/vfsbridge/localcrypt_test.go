package vfsbridge

// A crypt remote whose base is a LOCAL path must mount like any other remote.
//
// Enter Space users keep encrypted vaults on the Mac itself or on an external
// drive: a `crypt` over a bare path, or a `crypt` over the device-scoped `alias`
// that a Local Folder creates. The File Provider extension can never serve
// those (it runs in its own sandbox and cannot open the app's folder
// bookmarks), so on macOS 27 the app routes them to this bridge instead. These
// pin that the bridge serves both shapes end to end: a write through the
// volume lands on disk ENCRYPTED (name and bytes), and decrypts back to what
// was written.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/alias"
	_ "github.com/rclone/rclone/backend/crypt"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/obscure"
)

// newLocalCryptServer serves two crypt remotes over local folders:
//
//	lc_bare   crypt → /tmp/…/bare/vault             (bare local path)
//	lc_alias  crypt → lc_folder:vault → /tmp/…/aliased  (Local Folder shape)
//
// It returns the server and the two directories the ciphertext lands in.
func newLocalCryptServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	tmp := t.TempDir()
	bare := filepath.Join(tmp, "bare")
	aliased := filepath.Join(tmp, "aliased")
	for _, d := range []string{bare, aliased} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// A throwaway password per run, never a fixed one.
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	password := obscure.MustObscure(hex.EncodeToString(raw))

	conf := "[lc_bare]\ntype = crypt\nremote = " + filepath.Join(bare, "vault") +
		"\npassword = " + password + "\n\n" +
		"[lc_folder]\ntype = alias\nremote = " + aliased + "\n\n" +
		"[lc_alias]\ntype = crypt\nremote = lc_folder:vault\npassword = " + password + "\n"
	confPath := filepath.Join(tmp, "rclone.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()

	s := NewServer()
	for _, name := range []string{"lc_bare", "lc_alias"} {
		if err := s.AddRemote(name); err != nil {
			t.Fatalf("AddRemote %s: %v — a crypt over a local path must be mountable", name, err)
		}
	}
	t.Cleanup(s.Stop)
	return s, filepath.Join(bare, "vault"), filepath.Join(aliased, "vault")
}

// writeThroughBridge creates name under the remote's root and writes payload.
func writeThroughBridge(t *testing.T, s *Server, remote, name string, payload []byte) {
	t.Helper()
	dirID := remoteRootID(t, s, remote)
	r := mustOK(t, s.handleRequest(req(t, 2, "create",
		map[string]any{"dirId": dirID, "name": name, "isDir": false})), remote+": create")
	fileID := r.Result.(ItemInfo).ID
	mustOK(t, s.handleRequest(req(t, 3, "open",
		map[string]any{"itemId": fileID, "write": true})), remote+": open")
	mustOK(t, s.handleRequest(req(t, 4, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": payload})), remote+": write")
	mustOK(t, s.handleRequest(req(t, 5, "close", map[string]any{"itemId": fileID})), remote+": close")
}

func TestCryptOverLocalPathMounts(t *testing.T) {
	s, bareVault, aliasVault := newLocalCryptServer(t)

	// Both remotes are listed at the root, like any cloud remote.
	names := dirNames(t, s, RootInodeID)
	if strings.Join(names, ",") != "lc_alias,lc_bare" {
		t.Fatalf("root listed %v, want [lc_alias lc_bare]", names)
	}

	const plainName = "diary.txt"
	payloads := map[string][]byte{
		"lc_bare":  []byte("bare-path vault contents"),
		"lc_alias": []byte("local-folder vault contents"),
	}
	for remote, payload := range payloads {
		writeThroughBridge(t, s, remote, plainName, payload)
	}
	s.WaitForWriters(20 * time.Second)

	ctx := context.Background()
	for remote, vault := range map[string]string{"lc_bare": bareVault, "lc_alias": aliasVault} {
		want := payloads[remote]

		// On disk: exactly one file, with an encrypted name and encrypted bytes.
		entries, err := os.ReadDir(vault)
		if err != nil {
			t.Fatalf("%s: nothing reached the local folder: %v", remote, err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s: local folder holds %d entries, want 1", remote, len(entries))
		}
		if entries[0].Name() == plainName {
			t.Fatalf("%s: file stored under its PLAINTEXT name — crypt was bypassed", remote)
		}
		onDisk, err := os.ReadFile(filepath.Join(vault, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(onDisk), string(want)) {
			t.Fatalf("%s: plaintext bytes found on disk — crypt was bypassed", remote)
		}

		// And it decrypts back to what was written, read straight off the
		// backing folder rather than out of the bridge's VFS cache.
		f, err := fs.NewFs(ctx, remote+":")
		if err != nil {
			t.Fatalf("%s: NewFs: %v", remote, err)
		}
		obj, err := f.NewObject(ctx, plainName)
		if err != nil {
			t.Fatalf("%s: decrypted listing has no %s: %v", remote, plainName, err)
		}
		rc, err := obj.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s: decrypted %q, want %q", remote, got, want)
		}
	}
}

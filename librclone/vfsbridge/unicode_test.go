package vfsbridge

// Unicode normalization through the bridge.
//
// macOS hands a filesystem decomposed (NFD) names — Finder and every Cocoa
// path API run names through fileSystemRepresentation — while most cloud
// backends store and return precomposed (NFC) names byte-exactly. User report
// (Enter Space 7.0.2, macOS 27, Internxt): a folder named "Università" listed
// fine in Finder, but opening it showed nothing.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"golang.org/x/text/unicode/norm"
)

// newByteExactServer exposes a memory remote, which — like Internxt — matches
// names byte for byte, so an NFD lookup of an NFC name only succeeds if the
// bridge normalizes.
func newByteExactServer(t *testing.T, nfcDir string, files ...string) *Server {
	t.Helper()
	tmp := t.TempDir()
	confPath := filepath.Join(tmp, "rclone.conf")
	// Distinct bucket per test: the memory backend is process-global.
	bucket := "b" + norm.NFC.String(t.Name())
	conf := "[mem]\ntype = memory\n\n[rem_x]\ntype = alias\nremote = mem:" + bucket + "\n\n" +
		"[rem_y]\ntype = alias\nremote = " + tmp + "\n"
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()

	ctx := context.Background()
	f, err := fs.NewFs(ctx, "rem_x:")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Mkdir(ctx, nfcDir); err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		src := filepath.Join(tmp, "src")
		if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		lf, err := fs.NewFs(ctx, tmp)
		if err != nil {
			t.Fatal(err)
		}
		o, err := lf.NewObject(ctx, "src")
		if err != nil {
			t.Fatal(err)
		}
		in, err := o.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Put(ctx, in, fs.NewOverrideRemote(o, nfcDir+"/"+name)); err != nil {
			t.Fatal(err)
		}
		_ = in.Close()
	}

	s := NewServer()
	for _, name := range []string{"rem_x", "rem_y"} {
		if err := s.AddRemote(name); err != nil {
			t.Fatalf("AddRemote %s: %v", name, err)
		}
	}
	t.Cleanup(s.Stop)
	return s
}

func lookupID(t *testing.T, s *Server, dirID uint64, name string) ItemInfo {
	t.Helper()
	r := mustOK(t, s.handleRequest(req(t, 10, "lookup",
		map[string]any{"dirId": dirID, "name": name})), "lookup "+name)
	return r.Result.(ItemInfo)
}

// TestNFDLookupOfNFCFolderListsItsContents is the user report, in the order
// Finder drives it: list the parent (NFC names come back), then look the
// folder up by its NFD spelling and list what that lookup returned.
func TestNFDLookupOfNFCFolderListsItsContents(t *testing.T) {
	nfc := norm.NFC.String("Università")
	nfd := norm.NFD.String(nfc)
	if nfc == nfd {
		t.Fatal("test name has no decomposable characters")
	}
	s := newByteExactServer(t, nfc, "lezione.pdf", norm.NFC.String("città.txt"))
	remoteID := remoteRootID(t, s, "rem_x")

	// Finder lists the parent first.
	listed := dirNames(t, s, remoteID)
	if len(listed) != 1 || listed[0] != nfc {
		t.Fatalf("parent listed %q, want [%q]", listed, nfc)
	}
	var listedID uint64
	{
		r := mustOK(t, s.handleRequest(req(t, 11, "readdir",
			map[string]any{"dirId": remoteID})), "readdir")
		listedID = r.Result.([]DirEntry)[0].Item.ID
	}

	// Then looks it up by the decomposed name.
	got := lookupID(t, s, remoteID, nfd)
	if got.ID != listedID {
		t.Errorf("NFD lookup returned inode %d, readdir advertised %d for the same folder", got.ID, listedID)
	}
	if got.Name != nfc {
		t.Errorf("NFD lookup reported name %q, want the stored %q", got.Name, nfc)
	}

	children := dirNames(t, s, got.ID)
	if len(children) != 2 {
		t.Fatalf("folder looked up by NFD name listed %q, want 2 files", children)
	}
	// And a child looked up by ITS NFD spelling resolves too.
	child := lookupID(t, s, got.ID, norm.NFD.String("città.txt"))
	if child.ParentID != got.ID {
		t.Errorf("child parent = %d, want %d", child.ParentID, got.ID)
	}
}

// TestNFDLookupBeforeListing covers the cold path: a lookup by NFD name with no
// prior readdir of the parent (Terminal `cd`, a restored Finder window).
func TestNFDLookupBeforeListing(t *testing.T) {
	nfc := norm.NFC.String("Università")
	s := newByteExactServer(t, nfc, "a.txt")
	remoteID := remoteRootID(t, s, "rem_x")

	got := lookupID(t, s, remoteID, norm.NFD.String(nfc))
	if children := dirNames(t, s, got.ID); len(children) != 1 {
		t.Fatalf("listed %q, want [a.txt]", children)
	}
	// A later NFC listing must hand out the same identity.
	r := mustOK(t, s.handleRequest(req(t, 12, "readdir",
		map[string]any{"dirId": remoteID})), "readdir")
	if id := r.Result.([]DirEntry)[0].Item.ID; id != got.ID {
		t.Errorf("readdir advertised %d after NFD lookup returned %d", id, got.ID)
	}
}

// remoteNames lists dir on the backing remote directly, bypassing the VFS.
func remoteNames(t *testing.T, dir string) []string {
	t.Helper()
	f, err := fs.NewFs(context.Background(), "rem_x:")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := f.List(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, filepath.Base(e.Remote()))
	}
	return names
}

// TestNFDCreateAndRenameStoreNFC: a name typed or copied in Finder arrives
// decomposed and must reach the remote precomposed, and must not make a twin
// of an entry that already exists under the other spelling.
func TestNFDCreateAndRenameStoreNFC(t *testing.T) {
	nfc := norm.NFC.String("Università")
	existing := norm.NFC.String("città.txt")
	s := newByteExactServer(t, nfc, existing)
	remoteID := remoteRootID(t, s, "rem_x")
	dirID := lookupID(t, s, remoteID, norm.NFD.String(nfc)).ID

	// Overwrite the existing file by its NFD spelling.
	mustOK(t, s.handleRequest(req(t, 20, "create", map[string]any{
		"dirId": dirID, "name": norm.NFD.String(existing), "isDir": false})), "create existing")
	// Create new entries by NFD spelling.
	created := norm.NFC.String("perché.txt")
	mustOK(t, s.handleRequest(req(t, 21, "create", map[string]any{
		"dirId": dirID, "name": norm.NFD.String(created), "isDir": false})), "create new")
	sub := norm.NFC.String("Esercitazioni è")
	r := mustOK(t, s.handleRequest(req(t, 22, "create", map[string]any{
		"dirId": dirID, "name": norm.NFD.String(sub), "isDir": true})), "mkdir")
	if name := r.Result.(ItemInfo).Name; name != sub {
		t.Errorf("mkdir reported %q, want %q", name, sub)
	}
	renamed := norm.NFC.String("perché-2.txt")
	mustOK(t, s.handleRequest(req(t, 23, "rename", map[string]any{
		"srcDirId": dirID, "srcName": norm.NFD.String(created),
		"dstDirId": dirID, "dstName": norm.NFD.String(renamed)})), "rename")
	s.WaitForWriters(20 * time.Second)

	got := remoteNames(t, nfc)
	// The memory backend keeps no empty directories, so only files are listed.
	want := map[string]bool{existing: true, renamed: true}
	if len(got) != len(want) {
		t.Fatalf("remote holds %q, want %d entries", got, len(want))
	}
	for _, name := range got {
		if !want[name] {
			t.Errorf("remote holds %q (NFC? %v), not one of the expected NFC names",
				name, norm.NFC.IsNormalString(name))
		}
	}
}

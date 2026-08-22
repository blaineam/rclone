package vfsbridge

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs/config"
)

// Finder writes .DS_Store into every directory it displays, including a mounted
// volume's root. Persisting that to the remote produced "Update Create failed:
// permission denied" against non-writable remote roots, plus a failed
// partial-copy cleanup on top — a stack of errors about a file the user never
// created and does not want stored.
func TestDesktopServicesStoreIsRecognised(t *testing.T) {
	if !isDesktopServicesStore(".DS_Store") {
		t.Fatal(".DS_Store must be recognised")
	}
}

// Matched exactly. A user's own file that merely resembles the name is their
// data and must still be creatable — silently refusing to store someone's file
// is far worse than the noise this suppresses.
func TestSimilarNamesAreNotSuppressed(t *testing.T) {
	for _, name := range []string{
		"DS_Store",         // no leading dot
		".DS_Store.txt",    // extension
		".DS_Store_backup", // suffixed
		"my.DS_Store",      // embedded
		".ds_store",        // case differs; APFS is case-insensitive but the
		// remote may not be, and Finder only ever writes
		// the canonical spelling
		"", // defensive
	} {
		if isDesktopServicesStore(name) {
			t.Errorf("%q must not be treated as Finder scratch", name)
		}
	}
}

// The guard in doCreate only stops NEW .DS_Store writes. Ones cached before it
// existed are already queued for upload and the VFS retries them on every
// launch — forever, against a remote root that will never accept them. Ten were
// found on one machine, reappearing seconds after each reinstall.
func TestPurgeRemovesCachedDesktopServicesStores(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("RCLONE_CACHE_DIR", cacheDir)
	if err := config.SetCacheDir(cacheDir); err != nil {
		t.Fatalf("SetCacheDir: %v", err)
	}

	// Both trees: vfs/ holds the data, vfsMeta/ the write-back bookkeeping.
	// Leaving either behind keeps the pending upload alive.
	junk := []string{
		filepath.Join(cacheDir, "vfs", "FORTRESS", ".DS_Store"),
		filepath.Join(cacheDir, "vfsMeta", "FORTRESS", ".DS_Store"),
		filepath.Join(cacheDir, "vfs", "R", "nested", "deep", ".DS_Store"),
	}
	// Real user data living alongside it must survive untouched.
	keep := []string{
		filepath.Join(cacheDir, "vfs", "FORTRESS", "holiday.jpg"),
		filepath.Join(cacheDir, "vfs", "FORTRESS", ".DS_Store.txt"),
		filepath.Join(cacheDir, "vfsMeta", "FORTRESS", "DS_Store"),
	}
	for _, p := range append(append([]string{}, junk...), keep...) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	purgeCachedDesktopServicesStores()

	for _, p := range junk {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("should have been purged: %s", p)
		}
	}
	for _, p := range keep {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("must NOT have been touched: %s (%v)", p, err)
		}
	}
}

// A cache directory that does not exist is normal on first run and must not
// stop the bridge from starting.
func TestPurgeToleratesMissingCacheDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	t.Setenv("RCLONE_CACHE_DIR", dir)
	_ = config.SetCacheDir(dir)
	purgeCachedDesktopServicesStores() // must not panic
}

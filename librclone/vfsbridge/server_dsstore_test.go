package vfsbridge

import "testing"

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

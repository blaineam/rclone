package vfsbridge

// Tests for changing the exposed remote set on a LIVE volume.
//
// Dropping a remote used to mean stopping the whole bridge and rebuilding it,
// which takes every other remote's VFS and cache down too. These cover the
// pieces that make an in-place add/remove safe: the root keeps its shape no
// matter how many remotes are exposed, a removal flushes before it releases,
// and the generation moves so the FSKit side can tell the listing changed.

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// dirNames lists a directory through the bridge.
func dirNames(t *testing.T, s *Server, dirID uint64) []string {
	t.Helper()
	r := mustOK(t, s.handleRequest(req(t, 100, "readdir",
		map[string]any{"dirId": dirID})), "readdir")
	entries := r.Result.([]DirEntry)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Item.Name)
	}
	return names
}

// TestRootIsSyntheticAtEveryRemoteCount is the invariant that makes a live
// toggle possible at all. The root used to be the single remote's own root when
// exactly one was exposed, and the synthetic list-of-remotes above that — so
// crossing one in either direction changed what inode 2 MEANT while the kernel
// held a vnode for it. The root must list remote names at every count.
func TestRootIsSyntheticAtEveryRemoteCount(t *testing.T) {
	s, _, _ := newTestServer(t)

	if got := dirNames(t, s, RootInodeID); len(got) != 2 {
		t.Fatalf("two remotes: root listed %v, want both remote names", got)
	}

	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}
	got := dirNames(t, s, RootInodeID)
	if len(got) != 1 || got[0] != "rem_a" {
		t.Fatalf("one remote: root listed %v, want [rem_a] — the root changed shape", got)
	}
	if !s.isAggregateRoot(RootInodeID) {
		t.Fatal("one remote: root stopped being synthetic")
	}

	if err := s.RemoveRemote("rem_a"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}
	if got := dirNames(t, s, RootInodeID); len(got) != 0 {
		t.Fatalf("no remotes: root listed %v, want empty", got)
	}
	// An empty volume still has to answer for its own root, or the mount wedges
	// at fetchAndSetTypeForItem before anything user-visible runs.
	mustOK(t, s.handleRequest(req(t, 200, "getattr",
		map[string]any{"itemId": RootInodeID})), "getattr root with no remotes")
}

// TestRootListingIsStablyOrdered guards against Go's randomised map iteration
// reaching the user: a root whose entries reshuffle between listings is not a
// listing.
func TestRootListingIsStablyOrdered(t *testing.T) {
	s, _, _ := newTestServer(t)
	first := dirNames(t, s, RootInodeID)
	for i := 0; i < 20; i++ {
		got := dirNames(t, s, RootInodeID)
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("listing %d was %v, first was %v — root order is not stable", i, got, first)
			}
		}
	}
}

// TestRemoveRemoteLeavesOthersAlone is the whole point of RemoveRemote: the
// remote being dropped goes away and every other one keeps working, including
// the inodes already handed out for it.
func TestRemoveRemoteLeavesOthersAlone(t *testing.T) {
	s, backingA, _ := newTestServer(t)

	if err := os.WriteFile(filepath.Join(backingA, "keep.txt"), []byte("still here"), 0o600); err != nil {
		t.Fatal(err)
	}
	dirA := remoteRootID(t, s, "rem_a")

	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}

	// rem_a's directory id, resolved BEFORE the removal, must still work.
	if got := dirNames(t, s, dirA); len(got) != 1 || got[0] != "keep.txt" {
		t.Fatalf("surviving remote listed %v, want [keep.txt]", got)
	}
	// And rem_b must be gone from the root.
	if r := s.handleRequest(req(t, 300, "lookup",
		map[string]any{"dirId": RootInodeID, "name": "rem_b"})); r.Ok {
		t.Fatal("removed remote is still resolvable from the root")
	}
}

// TestRemoveRemoteFlushesPendingWrite is the durability requirement. With
// CacheModeFull a write is acknowledged from the local cache and uploaded
// asynchronously, so releasing the VFS without flushing discards it silently:
// the file was written, readable through the mount, and never stored.
func TestRemoveRemoteFlushesPendingWrite(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirA := remoteRootID(t, s, "rem_a")

	r := mustOK(t, s.handleRequest(req(t, 400, "create",
		map[string]any{"dirId": dirA, "name": "pending.txt", "isDir": false})), "create")
	fileID := r.Result.(ItemInfo).ID

	mustOK(t, s.handleRequest(req(t, 401, "open",
		map[string]any{"itemId": fileID, "write": true})), "open")
	payload := []byte("must survive the removal")
	mustOK(t, s.handleRequest(req(t, 402, "write",
		map[string]any{"itemId": fileID, "offset": 0, "data": payload})), "write")

	// Deliberately no close: removal has to flush what is still outstanding.
	if err := s.RemoveRemote("rem_a"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(backingA, "pending.txt"))
	if err != nil {
		t.Fatalf("removing the remote discarded a pending write: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("backing store holds %q, want %q", got, payload)
	}
}

// TestRemoveRemoteDropsItsInodes checks the subtree sweep: ids belonging to the
// removed remote are forgotten, so they cannot resolve to a VFS that has been
// shut down.
func TestRemoveRemoteDropsItsInodes(t *testing.T) {
	s, _, backingB := newTestServer(t)
	if err := os.WriteFile(filepath.Join(backingB, "gone.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	dirB := remoteRootID(t, s, "rem_b")
	r := mustOK(t, s.handleRequest(req(t, 500, "lookup",
		map[string]any{"dirId": dirB, "name": "gone.txt"})), "lookup child")
	childID := r.Result.(ItemInfo).ID

	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}

	for _, id := range []uint64{dirB, childID} {
		if resp := s.handleRequest(req(t, 501, "getattr",
			map[string]any{"itemId": id})); resp.Ok {
			t.Fatalf("item %d of the removed remote still resolves", id)
		}
	}
}

// TestGenerationMovesWithTheRemoteSet covers the change signal. The FSKit side
// uses this as the root's directory verifier, so it has to move on both add and
// remove — and it must never be zero, which FSKit reserves for "no verifier
// yet" (FSDirectoryVerifierInitial).
func TestGenerationMovesWithTheRemoteSet(t *testing.T) {
	s, _, _ := newTestServer(t)

	start := s.Generation()
	if start == 0 {
		t.Fatal("generation is 0, which FSKit reads as FSDirectoryVerifierInitial")
	}

	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}
	afterRemove := s.Generation()
	if afterRemove == start {
		t.Fatal("generation did not move on remove — the listing change is invisible")
	}

	if err := s.AddRemote("rem_b"); err != nil {
		t.Fatalf("AddRemote: %v", err)
	}
	if s.Generation() == afterRemove {
		t.Fatal("generation did not move on add")
	}

	// A no-op removal must not move it: a verifier that changes when nothing
	// changed makes every listing look stale.
	settled := s.Generation()
	if err := s.RemoveRemote("not_a_remote"); err != nil {
		t.Fatalf("RemoveRemote of an absent remote should succeed: %v", err)
	}
	if s.Generation() != settled {
		t.Fatal("generation moved for a removal that removed nothing")
	}
}

// TestRootModTimeIsStableAndMovesOnlyWithTheRemoteSet is the property a polling
// client needs. The root used to report time.Now() on every call, so it looked
// modified on every getattr -- which carries exactly as much information as
// never being modified at all, and left Finder unable to notice a remote that
// had just been added.
func TestRootModTimeIsStableAndMovesOnlyWithTheRemoteSet(t *testing.T) {
	s, _, _ := newTestServer(t)

	rootMod := func() int64 {
		r := mustOK(t, s.handleRequest(req(t, 900, "getattr",
			map[string]any{"itemId": RootInodeID})), "getattr root")
		return r.Result.(ItemInfo).ModTime
	}

	first := rootMod()
	for i := 0; i < 5; i++ {
		if got := rootMod(); got != first {
			t.Fatalf("root mtime changed with no remote-set change: %d then %d", first, got)
		}
	}

	// Unix-second resolution: without this the change would land in the same
	// second and the assertion could not tell a real bump from a stuck value.
	time.Sleep(1100 * time.Millisecond)
	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}
	afterRemove := rootMod()
	if afterRemove <= first {
		t.Fatalf("root mtime did not advance on remove: %d then %d", first, afterRemove)
	}

	time.Sleep(1100 * time.Millisecond)
	if err := s.AddRemote("rem_b"); err != nil {
		t.Fatalf("AddRemote: %v", err)
	}
	if got := rootMod(); got <= afterRemove {
		t.Fatalf("root mtime did not advance on add: %d then %d", afterRemove, got)
	}
}

// TestEveryResponseCarriesTheGeneration checks the transport contract: the
// value rides on all traffic so the FSKit side never has to poll for it.
func TestEveryResponseCarriesTheGeneration(t *testing.T) {
	s, _, _ := newTestServer(t)
	want := s.Generation()

	ok := s.handleRequest(req(t, 600, "readdir", map[string]any{"dirId": RootInodeID}))
	if ok.Gen != want {
		t.Fatalf("success reply carried gen=%d, want %d", ok.Gen, want)
	}
	// Failures too — a stale item id is exactly when the caller most needs to
	// learn the remote set moved underneath it.
	bad := s.handleRequest(req(t, 601, "getattr", map[string]any{"itemId": uint64(999999)}))
	if bad.Ok {
		t.Fatal("expected the bogus id to fail")
	}
	if bad.Gen != want {
		t.Fatalf("error reply carried gen=%d, want %d", bad.Gen, want)
	}
}

// TestAddRemoteOnLiveVolumeIsVisible is the toggle-on half: a remote added
// while the volume is mounted shows up in the root without a remount.
func TestAddRemoteOnLiveVolumeIsVisible(t *testing.T) {
	s, _, _ := newTestServer(t)
	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote: %v", err)
	}
	if got := dirNames(t, s, RootInodeID); len(got) != 1 {
		t.Fatalf("after remove root listed %v", got)
	}

	if err := s.AddRemote("rem_b"); err != nil {
		t.Fatalf("AddRemote: %v", err)
	}
	got := dirNames(t, s, RootInodeID)
	if len(got) != 2 {
		t.Fatalf("re-added remote is not in the root: %v", got)
	}
	// It must be usable, not just listed.
	dirB := remoteRootID(t, s, "rem_b")
	mustOK(t, s.handleRequest(req(t, 700, "readdir",
		map[string]any{"dirId": dirB})), "readdir re-added remote")
}

// TestRemoveThenReAddSurvivesTraffic exercises the toggle the way a user would
// hit it: off and on repeatedly, with the volume answering throughout.
func TestRemoveThenReAddSurvivesTraffic(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	if err := os.WriteFile(filepath.Join(backingA, "anchor.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := s.RemoveRemote("rem_b"); err != nil {
			t.Fatalf("round %d remove: %v", i, err)
		}
		// rem_a keeps serving across the churn.
		dirA := remoteRootID(t, s, "rem_a")
		if got := dirNames(t, s, dirA); len(got) != 1 {
			t.Fatalf("round %d: surviving remote listed %v", i, got)
		}
		if err := s.AddRemote("rem_b"); err != nil {
			t.Fatalf("round %d add: %v", i, err)
		}
	}

	if got := dirNames(t, s, RootInodeID); len(got) != 2 {
		t.Fatalf("after churn root listed %v, want both remotes", got)
	}
}

// TestRemoveRemoteIsSafeUnderConcurrentTraffic removes a remote while requests
// are in flight against another one. The removal path drops s.mu to flush, so
// this is where a lock mistake would show up as a race or a deadlock.
func TestRemoveRemoteIsSafeUnderConcurrentTraffic(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	if err := os.WriteFile(filepath.Join(backingA, "busy.txt"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	dirA := remoteRootID(t, s, "rem_a")

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			s.handleRequest(req(t, 800, "readdir", map[string]any{"dirId": dirA}))
			s.handleRequest(req(t, 801, "readdir", map[string]any{"dirId": RootInodeID}))
		}
	}()

	if err := s.RemoveRemote("rem_b"); err != nil {
		t.Fatalf("RemoveRemote under traffic: %v", err)
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("traffic goroutine never finished — removal deadlocked the server")
	}
}

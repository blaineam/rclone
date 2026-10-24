package pcloud

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLiveRangeWriter edits a probe file in place on a real pCloud remote
// and checks the result byte for byte and by pCloud's own checksum.
//
// Opt-in: ES_PCLOUD_LIVE=remote:dir names where the probe goes (the
// directory is created if need be). The probe has a unique name, is
// written whole first, and is removed at the end whatever happens. Timings
// are logged so the cost of each call against the real service is known.
func TestLiveRangeWriter(t *testing.T) {
	target := os.Getenv("ES_PCLOUD_LIVE")
	if target == "" {
		t.Skip("set ES_PCLOUD_LIVE=remote:dir to run against a real pCloud remote")
	}
	// The library leaves RCLONE_CONFIG to the command line; honour it here,
	// and let the library ask for the config password (or take
	// RCLONE_CONFIG_PASS) as rclone would.
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	ctx := context.Background()
	f, err := fs.NewFs(ctx, target)
	require.NoError(t, err)
	_, isPcloud := f.(*Fs)
	require.True(t, isPcloud, "%v is not a pcloud remote", f)
	require.NotNil(t, f.Features().OpenRangeWriter, "pcloud advertises OpenRangeWriter")

	// A 3 MiB probe with a recognisable pattern, uploaded whole first.
	content := make([]byte, 3<<20)
	for i := range content {
		content[i] = byte(i*7 + 3)
	}
	name := fmt.Sprintf("es-range-probe-%d.bin", time.Now().UnixNano())
	t.Logf("probe: %s/%s (%d bytes)", target, name, len(content))
	start := time.Now()
	_, err = f.Put(ctx, bytes.NewReader(content), object.NewStaticObjectInfo(name, time.Now(), int64(len(content)), true, nil, f))
	require.NoError(t, err)
	t.Logf("uploaded whole in %v", time.Since(start).Round(time.Millisecond))
	defer func() {
		o, err := f.NewObject(ctx, name)
		if err != nil {
			t.Logf("cleanup: probe already gone (%v)", err)
			return
		}
		if err := o.Remove(ctx); err != nil {
			t.Errorf("cleanup: removing the probe failed: %v", err)
		} else {
			t.Logf("cleanup: probe removed")
		}
	}()

	// The edit: patch in the middle, append past the end, cut the tail.
	patch := []byte("PATCHED-IN-PLACE-BY-ENTER-SPACE")
	patchAt := int64(1 << 20)
	tail := make([]byte, 256<<10)
	for i := range tail {
		tail[i] = byte(0xA0 + i%16)
	}
	cut := int64(100)
	expected := append([]byte{}, content...)
	copy(expected[patchAt:], patch)
	expected = append(expected, tail...)
	expected = expected[:int64(len(expected))-cut]

	step := func(what string, fn func() error) {
		s := time.Now()
		err := fn()
		t.Logf("%-28s %8v  err=%v", what, time.Since(s).Round(time.Millisecond), err)
		require.NoError(t, err, what)
	}
	var w fs.RangeWriter
	step("open for ranged writes", func() (err error) { w, err = f.Features().OpenRangeWriter(ctx, name); return })
	step(fmt.Sprintf("patch %d bytes at %d", len(patch), patchAt), func() error { _, err := w.WriteAt(patch, patchAt); return err })
	step(fmt.Sprintf("append %d bytes", len(tail)), func() error { _, err := w.WriteAt(tail, int64(len(content))); return err })
	step("sync", w.Sync)
	step(fmt.Sprintf("truncate to %d", len(expected)), func() error { return w.Truncate(int64(len(expected))) })
	step("close", w.Close)

	// What pCloud now holds.
	o, err := f.NewObject(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, int64(len(expected)), o.Size(), "size after the edit")
	s := time.Now()
	rc, err := o.Open(ctx)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	t.Logf("read back %d bytes in %v", len(got), time.Since(s).Round(time.Millisecond))
	if !bytes.Equal(got, expected) {
		first := -1
		for i := range expected {
			if i >= len(got) || got[i] != expected[i] {
				first = i
				break
			}
		}
		t.Fatalf("content differs from the expected result; first difference at byte %d (got %d bytes, want %d)", first, len(got), len(expected))
	}
	for _, ht := range []hash.Type{hash.MD5, hash.SHA1, hash.SHA256} {
		if !f.Hashes().Contains(ht) {
			continue
		}
		sums, err := hash.StreamTypes(bytes.NewReader(expected), hash.NewHashSet(ht))
		require.NoError(t, err)
		want := sums[ht]
		gotHash, err := o.Hash(ctx, ht)
		require.NoError(t, err)
		assert.Equal(t, want, gotHash, "pCloud's %v of the edited file", ht)
		t.Logf("%v matches pCloud's checksum", ht)
	}
}

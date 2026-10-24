package sftp

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

// TestLiveRangeWriter edits a probe file in place on a real SFTP server and
// checks the result byte for byte: the test to run against a server whose
// handling of positioned writes and FSETSTAT size is unknown (a cloud
// provider's SFTP gateway such as pCloud's sftp.pcloud.com).
//
// Opt-in: ES_SFTP_LIVE=remote:dir names where the probe goes. The probe
// has a unique name, is written whole first, and is removed at the end
// whatever happens. Timings are logged.
func TestLiveRangeWriter(t *testing.T) {
	target := os.Getenv("ES_SFTP_LIVE")
	if target == "" {
		t.Skip("set ES_SFTP_LIVE=remote:dir to run against a real SFTP server")
	}
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	ctx := context.Background()
	f, err := fs.NewFs(ctx, target)
	require.NoError(t, err)
	_, isSftp := f.(*Fs)
	require.True(t, isSftp, "%v is not an sftp remote", f)
	require.NotNil(t, f.Features().OpenRangeWriter)

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
	if ra, ok := w.(io.ReaderAt); ok {
		buf := make([]byte, len(patch))
		step("read the patch back via handle", func() error { _, err := ra.ReadAt(buf, patchAt); return err })
		assert.Equal(t, patch, buf, "the handle reads what it wrote")
	}
	step(fmt.Sprintf("truncate to %d", len(expected)), func() error { return w.Truncate(int64(len(expected))) })
	step("close", w.Close)

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
	// Server-side hashes need a shell on the server; a gateway may have none.
	for _, ht := range []hash.Type{hash.MD5, hash.SHA1} {
		gotHash, err := o.Hash(ctx, ht)
		if err != nil || gotHash == "" {
			t.Logf("%v: server does not offer it (%v)", ht, err)
			continue
		}
		sums, err := hash.StreamTypes(bytes.NewReader(expected), hash.NewHashSet(ht))
		require.NoError(t, err)
		assert.Equal(t, sums[ht], gotHash, "server's %v of the edited file", ht)
		t.Logf("%v matches the server's checksum", ht)
	}
}

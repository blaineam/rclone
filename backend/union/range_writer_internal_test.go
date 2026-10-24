package union

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/rclone/rclone/backend/compress"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) {
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
}

func readFile(t *testing.T, dir, name string) string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	return string(b)
}

// newUnion configures and opens a union; configfile.Install() must have
// run once in the test already (a second Install empties the store).
func newUnion(t *testing.T, name string, upstreams string) fs.Fs {
	config.FileSetValue(name, "type", "union")
	config.FileSetValue(name, "upstreams", upstreams)
	f, err := fs.NewFs(context.Background(), name+":")
	require.NoError(t, err)
	return f
}

// A file on exactly one upstream is edited in place there; a file the
// union holds on several upstreams, or on a read-only one, is refused with
// a permission error and nothing changes.
func TestOpenRangeWriterForwardsToTheOneUpstream(t *testing.T) {
	ctx := context.Background()
	configfile.Install()
	dirs := MakeTestDirs(t, 3)
	writeFile(t, dirs[0], "one.bin", "0123456789")
	writeFile(t, dirs[0], "both.bin", "aaaaaaaaaa")
	writeFile(t, dirs[1], "both.bin", "aaaaaaaaaa")
	writeFile(t, dirs[2], "ro.bin", "rrrrrrrrrr")

	f := newUnion(t, "TestUnionRange", dirs[0]+" "+dirs[1]+" "+dirs[2]+":ro")
	do := f.Features().OpenRangeWriter
	require.NotNil(t, do, "every upstream is local, so the union advertises the feature")

	w, err := do(ctx, "one.bin")
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("XY"), 3)
	require.NoError(t, err)
	_, err = w.WriteAt([]byte("tail"), 10)
	require.NoError(t, err)
	require.NoError(t, w.Truncate(12))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	assert.Equal(t, "012XY56789ta", readFile(t, dirs[0], "one.bin"))
	_, err = os.Stat(filepath.Join(dirs[1], "one.bin"))
	assert.True(t, os.IsNotExist(err), "the edit stays on the upstream that holds the file")
	o, err := f.NewObject(ctx, "one.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(12), o.Size())

	_, err = do(ctx, "both.bin")
	assert.ErrorIs(t, err, fs.ErrorPermissionDenied, "a mirrored file has no single home")
	assert.Equal(t, "aaaaaaaaaa", readFile(t, dirs[0], "both.bin"))
	assert.Equal(t, "aaaaaaaaaa", readFile(t, dirs[1], "both.bin"))

	_, err = do(ctx, "ro.bin")
	assert.ErrorIs(t, err, fs.ErrorPermissionDenied, "a read-only upstream takes no writes")
	assert.Equal(t, "rrrrrrrrrr", readFile(t, dirs[2], "ro.bin"))

	_, err = do(ctx, "missing.bin")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

// An upstream that cannot write at an offset masks the feature off the
// whole union.
func TestOpenRangeWriterMaskedWhenAnUpstreamCannot(t *testing.T) {
	configfile.Install()
	dirs := MakeTestDirs(t, 2)
	config.FileSetValue("TestUnionRangeCompressed", "type", "compress")
	config.FileSetValue("TestUnionRangeCompressed", "remote", dirs[1])
	f := newUnion(t, "TestUnionRangeMasked", dirs[0]+" TestUnionRangeCompressed:")
	assert.Nil(t, f.Features().OpenRangeWriter)
}

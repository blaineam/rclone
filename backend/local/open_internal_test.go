package local

// Object.Open paths around the fork's iCloud branch that are not iCloud
// specific: options handling, no_check_updated, translated links, and a file
// vanishing before the open.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mandatoryOption is an option the local backend does not understand but
// that claims to be mandatory.
type mandatoryOption struct{}

func (mandatoryOption) Header() (string, string) { return "X-Test", "1" }
func (mandatoryOption) String() string           { return "mandatoryOption" }
func (mandatoryOption) Mandatory() bool          { return true }

func newOpenTestFs(t *testing.T, m configmap.Simple) (fs.Fs, string) {
	t.Helper()
	dir := t.TempDir()
	f, err := NewFs(context.Background(), "local", dir, m)
	require.NoError(t, err)
	return f, dir
}

func readAllClose(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return string(b)
}

func TestOpenUnsupportedMandatoryOptionIsLoggedNotFatal(t *testing.T) {
	ctx := context.Background()
	f, dir := newOpenTestFs(t, configmap.Simple{})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("abc"), 0o600))
	o, err := f.NewObject(ctx, "a.txt")
	require.NoError(t, err)
	rc, err := o.Open(ctx, mandatoryOption{})
	require.NoError(t, err)
	assert.Equal(t, "abc", readAllClose(t, rc))
}

// With no_check_updated, a reader sees the size as of the open even if the
// file grows underneath it.
func TestOpenNoCheckUpdatedLimitsToStatSize(t *testing.T) {
	ctx := context.Background()
	f, dir := newOpenTestFs(t, configmap.Simple{"no_check_updated": "true"})
	p := filepath.Join(dir, "grow.txt")
	require.NoError(t, os.WriteFile(p, []byte("abc"), 0o600))
	o, err := f.NewObject(ctx, "grow.txt")
	require.NoError(t, err)
	rc, err := o.Open(ctx)
	require.NoError(t, err)
	fh, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = fh.Write([]byte("def"))
	require.NoError(t, err)
	require.NoError(t, fh.Close())
	assert.Equal(t, "abc", readAllClose(t, rc))
}

func TestOpenTranslatedLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks")
	}
	ctx := context.Background()
	f, dir := newOpenTestFs(t, configmap.Simple{"links": "true"})
	require.NoError(t, os.Symlink("target/file", filepath.Join(dir, "link")))
	o, err := f.NewObject(ctx, "link"+fs.LinkSuffix)
	require.NoError(t, err)
	rc, err := o.Open(ctx)
	require.NoError(t, err)
	assert.Equal(t, "target/file", readAllClose(t, rc))
}

func TestOpenVanishedFile(t *testing.T) {
	ctx := context.Background()
	f, dir := newOpenTestFs(t, configmap.Simple{})
	p := filepath.Join(dir, "gone.txt")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	o, err := f.NewObject(ctx, "gone.txt")
	require.NoError(t, err)
	require.NoError(t, os.Remove(p))
	_, err = o.Open(ctx)
	require.Error(t, err)
}

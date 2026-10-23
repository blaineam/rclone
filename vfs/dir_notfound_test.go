package vfs

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyListFs wraps a backend and, while listErr is set, fails every List
// with it. Everything else passes through.
type flakyListFs struct {
	fs.Fs
	listErr error
}

func (f *flakyListFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.Fs.List(ctx, dir)
}

// Enter Space fork: a directory that has been listed with entries keeps them
// when a refresh reports "not found", which backends also report for
// transient failures, instead of becoming an empty directory for DirCacheTime.
func TestDirKeepsEntriesWhenRefreshReportsNotFound(t *testing.T) {
	r := fstest.NewRun(t)
	flaky := &flakyListFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCacheTime = fs.Duration(50 * time.Millisecond)
	vfs := New(context.Background(), flaky, &opt)
	t.Cleanup(func() { cleanupVFS(t, vfs) })

	file1 := r.WriteObject(context.Background(), "dir/file1", "file1 contents", t1)
	r.CheckRemoteItems(t, file1)

	// Populate the cache.
	node, err := vfs.Stat("dir/file1")
	require.NoError(t, err)
	assert.Equal(t, int64(14), node.Size())

	// Let the listing go stale, then make the backend say "not found".
	time.Sleep(60 * time.Millisecond)
	flaky.listErr = fs.ErrorDirNotFound

	node, err = vfs.Stat("dir/file1")
	require.NoError(t, err, "a transient not-found on refresh must not make a known file vanish")
	assert.Equal(t, int64(14), node.Size())
	dir, err := vfs.Stat("dir")
	require.NoError(t, err)
	entries, err := dir.(*Dir).ReadDirAll()
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the cached listing is kept")

	// It keeps asking: once the backend recovers, the next look (after the
	// short retry window) sees the remote again, including a new file.
	flaky.listErr = nil
	file2 := r.WriteObject(context.Background(), "dir/file2", "file2 contents", t1)
	r.CheckRemoteItems(t, file1, file2)
	deadline := time.Now().Add(15 * time.Second) // dirNotFoundRetry + slack
	for {
		if _, err := vfs.Stat("dir/file2"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the directory was never refreshed after the backend recovered")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A directory never listed before still treats not-found as empty, which
	// is what directories created in the cache but not yet on the remote need.
	flaky.listErr = fs.ErrorDirNotFound
	_, err = vfs.Stat("never-listed/x")
	assert.Error(t, err)

	// ...but that "empty" answer is also asked again after the short retry,
	// not after the full DirCacheTime (5 minutes by default).
	flaky.listErr = nil
	r.WriteObject(context.Background(), "never-listed/y", "y contents", t1)
	deadline = time.Now().Add(15 * time.Second)
	for {
		if _, err := vfs.Stat("never-listed/y"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a never-listed directory that reported not-found was cached empty for longer than the retry window")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

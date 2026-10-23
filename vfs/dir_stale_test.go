package vfs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowListFs delays every List by listDelay and counts them.
type slowListFs struct {
	fs.Fs
	listDelay time.Duration
	lists     atomic.Int32
}

func (f *slowListFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	f.lists.Add(1)
	time.Sleep(f.listDelay)
	return f.Fs.List(ctx, dir)
}

// A directory that has been listed once is served from its cache after
// DirCacheTime passes, while the re-listing runs in the background: the
// caller does not wait out the backend, and the new listing lands shortly
// after. A directory never listed before still waits for its first listing.
func TestDirStaleEntriesServedWhileRefreshing(t *testing.T) {
	r := fstest.NewRun(t)
	slow := &slowListFs{Fs: r.Fremote, listDelay: 150 * time.Millisecond}
	opt := vfscommon.Opt
	// The cache drops a directory not visited within twice DirCacheTime, so the
	// stale window below has to open after both root and "dir" went stale and
	// close before either is dropped.
	opt.DirCacheTime = fs.Duration(time.Second)
	vfs := New(context.Background(), slow, &opt)
	defer vfs.Shutdown()

	t1 := fstest.Time("2001-02-03T04:05:06.499999999Z")
	file1 := r.WriteObject(context.Background(), "dir/file1", "file1 contents", t1)
	r.CheckRemoteItems(t, file1)

	// First listing: cold, waits for the backend.
	t0 := time.Now()
	_, err := vfs.Stat("dir/file1")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(t0), slow.listDelay, "the first listing waits for the backend")

	// The backend changes, and the cache goes stale.
	file2 := r.WriteObject(context.Background(), "dir/file2", "file2 contents", t1)
	r.CheckRemoteItems(t, file1, file2)
	time.Sleep(1100 * time.Millisecond)

	// A stale directory answers at once from what it has...
	t0 = time.Now()
	_, err = vfs.Stat("dir/file1")
	require.NoError(t, err)
	assert.Less(t, time.Since(t0), slow.listDelay/2, "a stale directory does not wait for the re-listing")
	_, err = vfs.Stat("dir/file2")
	assert.Error(t, err, "the new file is not visible until the background listing lands")

	// ...and the re-listing lands on its own.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := vfs.Stat("dir/file2"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the background re-listing never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// One listing per directory to warm up (root and "dir"), one background
	// re-listing per directory for the stale reads; never one per stale read.
	lists := slow.lists.Load()
	assert.LessOrEqual(t, lists, int32(4), "listings so far: %d", lists)
}

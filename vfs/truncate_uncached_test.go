package vfs

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shrinking a file none of whose bytes are in the VFS cache must stick.
//
// Item.Truncate clipped the cached ranges to the new size, then the close's
// _ensure(0, size) started a downloader whose reads ran past that size, and
// WriteAtNoOverwrite wrote the remote's old tail back over the truncation --
// so the full original file was uploaded again and the shrink was lost.
func TestTruncateUncachedFileShrinks(t *testing.T) {
	for _, writeBack := range []fs.Duration{0, writeBackDelay} {
		t.Run(writeBack.String(), func(t *testing.T) {
			opt := vfscommon.Opt
			opt.CacheMode = vfscommon.CacheModeFull
			opt.WriteBack = writeBack
			r, vfs := newTestVFSOpt(t, &opt)

			r.WriteObject(context.Background(), "file1", "0123456789", t1)

			node, err := vfs.Stat("file1")
			require.NoError(t, err)
			require.NoError(t, node.Truncate(4))
			vfs.WaitForWriters(10 * time.Second)

			o, err := r.Fremote.NewObject(context.Background(), "file1")
			require.NoError(t, err)
			in, err := operations.Open(context.Background(), o)
			require.NoError(t, err)
			got, err := io.ReadAll(in)
			require.NoError(t, err)
			require.NoError(t, in.Close())
			assert.Equal(t, "0123", string(got))
			assert.Equal(t, int64(4), o.Size())
		})
	}
}

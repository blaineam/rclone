package vfs

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rangeFileCreate is fileCreate with cache mode off and range writes on.
func rangeFileCreate(t *testing.T) (*fstest.Run, *VFS, *File) {
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeOff
	opt.RangeWrites = true
	r, vfs := newTestVFSOpt(t, &opt)
	if r.Fremote.Features().OpenRangeWriter == nil {
		t.Skipf("%v cannot write at an offset", r.Fremote)
	}
	file1 := r.WriteObject(context.Background(), "dir/file1", "file1 contents", t1)
	r.CheckRemoteItems(t, file1)
	node, err := vfs.Stat("dir/file1")
	require.NoError(t, err)
	return r, vfs, node.(*File)
}

func remoteContents(t *testing.T, r *fstest.Run, remote string) string {
	o, err := r.Fremote.NewObject(context.Background(), remote)
	require.NoError(t, err)
	in, err := o.Open(context.Background())
	require.NoError(t, err)
	b, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	return string(b)
}

// An open for writing of an existing file that does not truncate it is an
// in-place edit and gets a RangeFileHandle; everything else keeps the handle
// it always had.
func TestRangeFileHandleOpenKinds(t *testing.T) {
	_, vfs, file := rangeFileCreate(t)

	for _, tc := range []struct {
		flags int
		want  string
	}{
		{os.O_RDONLY, "*vfs.ReadFileHandle"},
		{os.O_WRONLY, "*vfs.RangeFileHandle"},
		{os.O_RDWR, "*vfs.RangeFileHandle"},
		{os.O_WRONLY | os.O_APPEND, "*vfs.RangeFileHandle"},
		{os.O_WRONLY | os.O_TRUNC, "*vfs.WriteFileHandle"},
		{os.O_RDWR | os.O_TRUNC, "*vfs.WriteFileHandle"},
	} {
		fd, err := file.Open(tc.flags)
		require.NoError(t, err, "flags %s", decodeOpenFlags(tc.flags))
		assert.Equal(t, tc.want, typeName(fd), "flags %s", decodeOpenFlags(tc.flags))
		if tc.flags&os.O_TRUNC != 0 {
			// A streaming handle uploads on close; give it the content back.
			_, err = fd.Write([]byte("file1 contents"))
			require.NoError(t, err)
		}
		require.NoError(t, fd.Close())
	}

	// A file that does not exist yet is a new upload, never an edit.
	fd, err := vfs.OpenFile("dir/new", os.O_WRONLY|os.O_CREATE, 0o666)
	require.NoError(t, err)
	assert.Equal(t, "*vfs.WriteFileHandle", typeName(fd))
	_, err = fd.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
}

// Without the option nothing changes: cache mode off still hands out a
// WriteFileHandle for O_RDWR, as upstream does.
func TestRangeFileHandleNeedsTheOption(t *testing.T) {
	_, _, file, _ := fileCreate(t, vfscommon.CacheModeOff)
	fd, err := file.Open(os.O_RDWR)
	require.NoError(t, err)
	assert.Equal(t, "*vfs.WriteFileHandle", typeName(fd))
	require.NoError(t, fd.Close())
}

// A backend that cannot write at an offset refuses the open itself, with
// EPERM, instead of handing out a write handle that fails at its first write.
func TestRangeFileHandleRefusedWithoutFeature(t *testing.T) {
	r := fstest.NewRun(t)
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeOff
	opt.RangeWrites = true
	vfs := NewUnshared(context.Background(), noRangeFs{r.Fremote}, &opt)
	t.Cleanup(func() { cleanupVFS(t, vfs) })
	r.WriteObject(context.Background(), "dir/file1", "file1 contents", t1)

	_, err := vfs.OpenFile("dir/file1", os.O_RDWR, 0o666)
	assert.Equal(t, EPERM, err)
	assert.Equal(t, "file1 contents", remoteContents(t, r, "dir/file1"))
}

// noRangeFs is an Fs whose features say it cannot write at an offset.
type noRangeFs struct{ fs.Fs }

func (f noRangeFs) Features() *fs.Features {
	ft := *f.Fs.Features()
	ft.OpenRangeWriter = nil
	return &ft
}

// The edits Lathe makes to a film, on a small file: patch bytes in the
// middle, append past the end, read the result back through the handle while
// it is open, truncate, and find exactly that on the remote afterwards.
func TestRangeFileHandleEdits(t *testing.T) {
	r, _, file := rangeFileCreate(t)

	fd, err := file.Open(os.O_RDWR)
	require.NoError(t, err)
	fh := fd.(*RangeFileHandle)
	assert.Equal(t, int64(14), file.Size())

	// Patch in the middle.
	n, err := fh.WriteAt([]byte("XYZ"), 6)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, int64(14), file.Size(), "a patch inside the file does not change its size")

	// Read back through the handle: the patched bytes, from the remote.
	buf := make([]byte, 14)
	n, err = fh.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "file1 XYZtents", string(buf[:n]))

	// Append past the end.
	n, err = fh.WriteAt([]byte(" and more"), 14)
	require.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.Equal(t, int64(23), file.Size())
	buf = make([]byte, 32)
	n, err = fh.ReadAt(buf, 10)
	assert.Equal(t, io.EOF, err, "a read that reaches the end says so")
	assert.Equal(t, "ents and more", string(buf[:n]))
	_, err = fh.ReadAt(buf, 23)
	assert.Equal(t, io.EOF, err)

	// Sync is honoured, or a no-op where the backend cannot promise it.
	require.NoError(t, fh.Sync())

	// Truncate.
	require.NoError(t, fh.Truncate(20))
	assert.Equal(t, int64(20), file.Size())

	// Sequential methods share the position.
	pos, err := fh.Seek(-3, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(17), pos)
	n, err = fh.Write([]byte("END"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	_, err = fh.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf = make([]byte, 5)
	n, err = fh.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "file1", string(buf[:n]))

	require.NoError(t, fh.Close())
	assert.Equal(t, ECLOSED, fh.Close())
	_, err = fh.WriteAt([]byte("x"), 0)
	assert.Equal(t, ECLOSED, err)

	// After close the file reports the remote's size and the remote has the bytes.
	assert.Equal(t, int64(20), file.Size())
	assert.Equal(t, "file1 XYZtents anEND", remoteContents(t, r, "dir/file1"))
}

// O_APPEND positions the sequential writer at the end.
func TestRangeFileHandleAppend(t *testing.T) {
	r, _, file := rangeFileCreate(t)
	fd, err := file.Open(os.O_WRONLY | os.O_APPEND)
	require.NoError(t, err)
	_, err = fd.Write([]byte("!"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	assert.Equal(t, "file1 contents!", remoteContents(t, r, "dir/file1"))
}

// File.Truncate on a file with no open handle takes the in-place path too,
// instead of being refused in cache mode off.
func TestRangeFileHandleTruncateWithoutHandle(t *testing.T) {
	r, _, file := rangeFileCreate(t)
	require.NoError(t, file.Truncate(5))
	assert.Equal(t, "file1", remoteContents(t, r, "dir/file1"))
	assert.Equal(t, int64(5), file.Size())
	// and extend
	require.NoError(t, file.Truncate(7))
	assert.Equal(t, "file1\x00\x00", remoteContents(t, r, "dir/file1"))
}

// An in-place edit's bytes are on the backend when each write returns, so the
// durability waits (fsync, unmount) do not wait for the handle to close.
func TestRangeFileHandleDoesNotHoldUpWaitForWriters(t *testing.T) {
	_, vfs, file := rangeFileCreate(t)
	fd, err := file.Open(os.O_RDWR)
	require.NoError(t, err)
	_, err = fd.WriteAt([]byte("x"), 0)
	require.NoError(t, err)

	start := time.Now()
	vfs.WaitForWriters(10 * time.Second)
	assert.Less(t, time.Since(start), 2*time.Second, "WaitForWriters waited for an in-place edit to close")
	assert.Equal(t, 0, file.activeWriters())
	assert.Equal(t, int32(1), file.nwriters.Load(), "the handle is still a writer for every other purpose")
	require.NoError(t, fd.Close())
}

// A read-only VFS refuses the edit.
func TestRangeFileHandleReadOnly(t *testing.T) {
	_, vfs, file := rangeFileCreate(t)
	vfs.Opt.ReadOnly = true
	_, err := file.Open(os.O_RDWR)
	assert.Equal(t, EROFS, err)
}

func typeName(v any) string {
	switch v.(type) {
	case *ReadFileHandle:
		return "*vfs.ReadFileHandle"
	case *WriteFileHandle:
		return "*vfs.WriteFileHandle"
	case *RangeFileHandle:
		return "*vfs.RangeFileHandle"
	case *RWFileHandle:
		return "*vfs.RWFileHandle"
	}
	return "?"
}

// An existing file that is still empty is what every file being copied in
// looks like between its creation and its first write; it keeps streaming,
// and a truncate of it to a size is refused as before.
func TestRangeFileHandleEmptyFileStreams(t *testing.T) {
	_, vfs, _ := rangeFileCreate(t)
	fd, err := vfs.OpenFile("dir/empty.bin", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	fd, err = vfs.OpenFile("dir/empty.bin", os.O_WRONLY, 0o666)
	require.NoError(t, err)
	assert.Equal(t, "*vfs.WriteFileHandle", typeName(fd), "an empty file has nothing for a range write to preserve")
	require.NoError(t, fd.Close())

	node, err := vfs.Stat("dir/empty.bin")
	require.NoError(t, err)
	assert.Equal(t, EPERM, node.Truncate(4<<20), "preallocating an empty file is refused as it always was in cache mode off")
}

// A second open for writing while a streaming upload of the file is in
// flight must not become a range handle: the file exists on the backend only
// as the empty object its creation put there, and the rest of the copy's
// writes would otherwise go to positioned writes into the path the stream is
// replacing. It gets the handle it always got.
func TestRangeFileHandleNotWhileStreaming(t *testing.T) {
	_, vfs, _ := rangeFileCreate(t)

	// A new file, as a create-then-open does it: the create leaves an empty
	// object behind, the open streams.
	fd, err := vfs.OpenFile("dir/copy.bin", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	stream, err := vfs.OpenFile("dir/copy.bin", os.O_WRONLY|os.O_TRUNC, 0o666)
	require.NoError(t, err)
	assert.Equal(t, "*vfs.WriteFileHandle", typeName(stream))
	_, err = stream.Write([]byte("first part "))
	require.NoError(t, err)

	// The second open, mid-stream, without O_TRUNC.
	second, err := vfs.OpenFile("dir/copy.bin", os.O_WRONLY, 0o666)
	require.NoError(t, err)
	assert.Equal(t, "*vfs.WriteFileHandle", typeName(second), "a range handle over an in-flight stream")
	require.NoError(t, second.Close())

	_, err = stream.Write([]byte("second part"))
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	node, err := vfs.Stat("dir/copy.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(len("first part second part")), node.Size())
}

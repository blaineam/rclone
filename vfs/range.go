package vfs

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// RangeFileHandle edits an existing file in place, straight on the backend.
//
// It is what an open for writing WITHOUT O_TRUNC of a file that already has
// content gets with --vfs-cache-mode off and --vfs-range-writes, on a backend
// whose storage can write at an offset (fs.Features.OpenRangeWriter: local,
// sftp, smb, azurefiles, hidrive). The alternatives both lose: WriteFileHandle is a sequential upload
// from byte zero and refuses such an open (EPERM), and the cache modes
// download the whole file to patch a few bytes of it and upload the whole
// file on close. Here a write at an offset is one positioned write on the
// backend, a write past the end extends the file, Truncate sets its size,
// and Sync returns once the backend has the bytes on stable storage where it
// can promise that.
//
// Reads through this handle go to the backend as ranged reads of the live
// object, so they see the bytes written so far. There is no cache to be
// stale: this handle exists only in cache mode off.
type RangeFileHandle struct {
	baseHandle
	mu     sync.Mutex
	closed bool
	file   *File
	w      fs.RangeWriter
	size   int64 // the file's size as the writes so far have left it
	offset int64 // position for the sequential Read/Write/Seek methods
}

// openRange opens f in place for writes at arbitrary offsets. Called by
// File.Open once it has decided that this is the kind of open (see
// rangeOpenWanted); f must have an object, that is the file exists.
func (f *File) openRange() (*RangeFileHandle, error) {
	f.mu.RLock()
	d := f.d
	o := f.o
	appendMode := f.appendMode
	f.mu.RUnlock()
	if d.vfs.Opt.ReadOnly {
		return nil, EROFS
	}
	if o == nil {
		return nil, ENOENT
	}
	do := d.Fs().Features().OpenRangeWriter
	if do == nil {
		// The refusal the person sees as EPERM on open: say why here, where
		// the backend is known, because the errno alone says nothing.
		fs.Errorf(f.Path(), "can't edit a file in place: %v cannot write at an offset (no OpenRangeWriter); with --vfs-cache-mode off an existing file can only be replaced", d.Fs())
		return nil, EPERM
	}
	w, err := do(f.ctx, f.Path())
	if err != nil {
		if errors.Is(err, fs.ErrorPermissionDenied) {
			// The backend has the feature but not for this file: a union
			// holding it on several upstreams, a pCloud that refuses its
			// file API. The same EPERM as a backend without the feature,
			// with the backend's reason in the log.
			fs.Errorf(f.Path(), "can't edit a file in place: %v", err)
			return nil, EPERM
		}
		fs.Errorf(f.Path(), "RangeFileHandle: open failed: %v", err)
		return nil, err
	}
	fh := &RangeFileHandle{file: f, w: w, size: o.Size()}
	if appendMode {
		fh.offset = fh.size
	}
	// Size() reads f.size while there are writers; start it at the truth.
	f.setSize(fh.size)
	f.addWriter(fh)
	return fh, nil
}

// String converts it to printable
func (fh *RangeFileHandle) String() string {
	if fh == nil {
		return "<nil *RangeFileHandle>"
	}
	if fh.file == nil {
		return "<nil *RangeFileHandle.file>"
	}
	return fh.file.String() + " (range)"
}

// Node returns the Node associated with this - satisfies Noder interface
func (fh *RangeFileHandle) Node() Node {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.file
}

// WriteAt writes p at off on the backend. A write past the end extends the
// file. The bytes have been accepted by the backend when it returns.
func (fh *RangeFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.writeAt(p, off)
}

func (fh *RangeFileHandle) writeAt(p []byte, off int64) (n int, err error) {
	if fh.closed {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.WriteAt: error: %v", EBADF)
		return 0, ECLOSED
	}
	if off < 0 {
		return 0, EINVAL
	}
	n, err = fh.w.WriteAt(p, off)
	if end := off + int64(n); end > fh.size {
		fh.size = end
		fh.file.setSize(end)
	}
	if err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.WriteAt: write of %d at %d failed: %v", len(p), off, err)
		return n, err
	}
	return n, nil
}

// Write writes p at the handle's position.
func (fh *RangeFileHandle) Write(p []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	n, err = fh.writeAt(p, fh.offset)
	fh.offset += int64(n)
	return n, err
}

// WriteString a string to the file
func (fh *RangeFileHandle) WriteString(s string) (n int, err error) {
	return fh.Write([]byte(s))
}

// ReadAt reads len(p) bytes at off from the live object on the backend.
func (fh *RangeFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.readAt(p, off)
}

func (fh *RangeFileHandle) readAt(p []byte, off int64) (n int, err error) {
	if fh.closed {
		return 0, ECLOSED
	}
	if off < 0 {
		return 0, EINVAL
	}
	if off >= fh.size || len(p) == 0 {
		return 0, io.EOF
	}
	// Like os.File.ReadAt: a read that runs into the end returns what there
	// was and io.EOF.
	short := false
	if rest := fh.size - off; int64(len(p)) > rest {
		p = p[:rest]
		short = true
	}
	if ra, ok := fh.w.(io.ReaderAt); ok {
		// Exactly these bytes, on the handle that is editing the file.
		n, err = ra.ReadAt(p, off)
		if err == nil && short {
			err = io.EOF
		}
		if err != nil && !errors.Is(err, io.EOF) {
			fs.Errorf(fh.file.Path(), "RangeFileHandle.ReadAt: read of %d at %d failed: %v", len(p), off, err)
		}
		return n, err
	}
	o := fh.file.getObject()
	if o == nil {
		return 0, ENOENT
	}
	// A SeekOption, not a RangeOption: RangeOption clamps its end to the
	// object's size as the backend last saw it, which an append has moved.
	in, err := o.Open(fh.file.ctx, &fs.SeekOption{Offset: off})
	if err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.ReadAt: open failed: %v", err)
		return 0, err
	}
	n, err = io.ReadFull(in, p)
	closeErr := in.Close()
	switch {
	case err == nil:
		err = closeErr
		if err == nil && short {
			err = io.EOF
		}
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		// The backend had fewer bytes than our size says. Report what came.
		err = nil
		if n == 0 {
			err = io.EOF
		}
	}
	if err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.ReadAt: read of %d at %d failed: %v", len(p), off, err)
	}
	return n, err
}

// Read reads from the handle's position.
func (fh *RangeFileHandle) Read(p []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	n, err = fh.readAt(p, fh.offset)
	fh.offset += int64(n)
	return n, err
}

// Seek sets the position for Read and Write.
func (fh *RangeFileHandle) Seek(offset int64, whence int) (ret int64, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, ECLOSED
	}
	switch whence {
	case io.SeekStart:
		ret = offset
	case io.SeekCurrent:
		ret = fh.offset + offset
	case io.SeekEnd:
		ret = fh.size + offset
	default:
		return 0, EINVAL
	}
	if ret < 0 {
		return 0, EINVAL
	}
	fh.offset = ret
	return ret, nil
}

// Truncate sets the file's size on the backend.
func (fh *RangeFileHandle) Truncate(size int64) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return ECLOSED
	}
	if size < 0 {
		return EINVAL
	}
	if err := fh.w.Truncate(size); err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.Truncate: to %d failed: %v", size, err)
		return err
	}
	fh.size = size
	fh.file.setSize(size)
	return nil
}

// Sync completes when the backend has the bytes written so far on stable
// storage, where it can promise that.
func (fh *RangeFileHandle) Sync() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return ECLOSED
	}
	if err := fh.w.Sync(); err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.Sync: %v", err)
		return err
	}
	return nil
}

// Flush is called on each close() of a file descriptor. Same as Sync.
func (fh *RangeFileHandle) Flush() error {
	return fh.Sync()
}

// Stat returns info about the file
func (fh *RangeFileHandle) Stat() (os.FileInfo, error) {
	return fh.file, nil
}

// Name returns the file's path
func (fh *RangeFileHandle) Name() string {
	return fh.file.Path()
}

// Close releases the backend handle and re-reads the object, so the file's
// size and modtime are what the backend has, then unregisters the writer.
func (fh *RangeFileHandle) Close() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return ECLOSED
	}
	fh.closed = true
	err := fh.w.Close()
	if err != nil {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.Close: %v", err)
	}
	// Refresh BEFORE delWriter: with no writers Size() comes from the object,
	// so the stale one must not be visible in between.
	if o, statErr := fh.file.Fs().NewObject(fh.file.ctx, fh.file.Path()); statErr == nil {
		fh.file.setObject(o)
	} else {
		fs.Errorf(fh.file.Path(), "RangeFileHandle.Close: can't re-read the object: %v", statErr)
		if err == nil {
			err = statErr
		}
	}
	fh.file.delWriter(fh)
	return err
}

// Release is called when we are finished with the file handle
func (fh *RangeFileHandle) Release() error {
	err := fh.Close()
	if errors.Is(err, ECLOSED) {
		return nil
	}
	return err
}

// hasContent reports whether f exists on the backend with at least one byte.
func (f *File) hasContent() bool {
	return f.exists() && f.Size() > 0
}

// hasStreamingWriter reports whether a WriteFileHandle (a sequential upload
// of the whole file) is open on f right now.
func (f *File) hasStreamingWriter() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, w := range f.writers {
		if _, ok := w.(*WriteFileHandle); ok {
			return true
		}
	}
	return false
}

// rangeOpenWanted reports whether an open with these flags of a file in a
// VFS with these options is an in-place edit to serve with a
// RangeFileHandle: cache mode off with range writes on, a write that does
// not truncate, of a file that has content.
func rangeOpenWanted(opt *vfscommon.Options, flags int, hasContent bool) bool {
	if opt.CacheMode != vfscommon.CacheModeOff || !opt.RangeWrites {
		return false
	}
	if flags&os.O_TRUNC != 0 || !hasContent {
		return false
	}
	switch flags & accessModeMask {
	case os.O_WRONLY, os.O_RDWR:
		return true
	}
	return flags&os.O_APPEND != 0
}

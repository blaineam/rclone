package hidrive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/hidrive/api"
	"github.com/rclone/rclone/fs"
)

// OpenRangeWriter opens an existing file for writes at arbitrary offsets.
//
// HiDrive's API writes a file in place: PATCH /file with an offset replaces
// the bytes from there for the length of the body and leaves the rest of
// the file alone (the backend's chunked upload relies on that, landing
// chunks at their offsets in parallel), and POST /file/truncate sets the
// file's size either way. So an in-place edit costs the bytes written, not
// the file. Both calls are the ones the backend already drives for uploads
// (patchFile, resizeFile); nothing is created or truncated on open.
func (f *Fs) OpenRangeWriter(ctx context.Context, remote string) (fs.RangeWriter, error) {
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	o := obj.(*Object)
	if f.tokenRenewer != nil {
		f.tokenRenewer.Start()
	}
	return &rangeWriter{ctx: ctx, o: o, path: f.resolvePath(remote)}, nil
}

// rangeWriter is the fs.RangeWriter for a HiDrive file.
type rangeWriter struct {
	ctx    context.Context
	o      *Object
	path   string // the resolved path PATCH and truncate address
	mu     sync.Mutex
	closed bool
}

// WriteAt writes p at off; HiDrive holds the bytes when it returns. A body
// is at most MaximumUploadBytes, so a larger write goes in pieces.
func (w *rangeWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	written := 0
	for written < len(p) {
		n := min(len(p)-written, MaximumUploadBytes)
		chunk := p[written : written+n]
		err := w.o.fs.patchFile(w.ctx, w.path, bytes.NewReader(chunk), uint64(off)+uint64(written), time.Time{})
		if err != nil {
			return written, fmt.Errorf("OpenRangeWriter: write of %d bytes at %d failed: %w", n, off+int64(written), err)
		}
		written += n
	}
	return written, nil
}

// Truncate sets the file's size (POST /file/truncate).
func (w *rangeWriter) Truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if size < 0 {
		return fmt.Errorf("OpenRangeWriter: truncate to %d", size)
	}
	if _, err := w.o.fs.resizeFile(w.ctx, w.path, uint64(size), time.Time{}); err != nil {
		return fmt.Errorf("OpenRangeWriter: truncate to %d failed: %w", size, err)
	}
	return nil
}

// Sync is a no-op: each PATCH is stored when HiDrive answers it.
func (w *rangeWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	return nil
}

// Close re-reads the file so the object's size, modtime and hash are
// HiDrive's.
func (w *rangeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	if w.o.fs.tokenRenewer != nil {
		defer w.o.fs.tokenRenewer.Stop()
	}
	info, err := w.o.fs.fetchMetadataForPath(w.ctx, w.path, api.HiDriveObjectWithMetadataFields)
	if err != nil {
		return fmt.Errorf("OpenRangeWriter: re-reading the file after close failed: %w", err)
	}
	return w.o.setMetadata(info)
}

// Check the interface is satisfied
var _ fs.OpenRangeWriter = (*Fs)(nil)

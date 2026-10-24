package pcloud

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/pcloud/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/rest"
)

// OpenRangeWriter opens an existing file for writes at arbitrary offsets.
//
// Whether pCloud lets an app use its file API is up to pCloud: for rclone's
// own OAuth app it answers file_open on an existing file with 2003 "Access
// denied" (verified against a live account on 2026-10-23), the same answer
// that had the upstream OpenWriterAt disabled. On that answer the Fs
// remembers the denial and every later open is refused at once with
// ErrorPermissionDenied, so the VFS falls back to replacing the file
// whole, as it did before this feature existed. An app pCloud does allow
// gets the edit below.
//
// pCloud's file API edits the stored file in place: file_open on the
// fileid gives a descriptor, file_pwrite writes at an offset (a write past
// the end extends the file), file_truncate sets its size and file_close
// releases it. An in-place edit therefore costs the bytes written, not the
// file. These are the calls the backend's disabled OpenWriterAt drove; what
// pCloud refused there was file_open with O_CREAT in a folder, and this
// opens an existing file by its id with O_WRITE alone, so nothing is created
// and nothing is truncated on open.
//
// pCloud binds a descriptor to the HTTP connection that opened it, so the
// handle owns a client limited to one connection (newSingleConnClient) and
// keeps its descriptor for its life. Should that connection drop between two
// operations (an editor that pauses for minutes between writes), the
// descriptor goes with it: the operation that finds out reopens the file
// and runs once more.
func (f *Fs) OpenRangeWriter(ctx context.Context, remote string) (fs.RangeWriter, error) {
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	o := obj.(*Object)
	if f.fileAPIDenied.Load() {
		return nil, fmt.Errorf("OpenRangeWriter: pCloud denies the file API to this app (error 2003 on an earlier open): %w", fs.ErrorPermissionDenied)
	}
	fileID, err := strconv.ParseInt(fileIDtoNumber(o.id), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("OpenRangeWriter: file id %q: %w", o.id, err)
	}
	w := &rangeWriter{ctx: ctx, o: o, fileID: fileID, size: o.size}
	if err := w.reopen(); err != nil {
		if apiErr, ok := errorOf(err); ok && apiErr.Result == 2003 {
			// "Access denied": pCloud does not let this app use the file
			// API (it answered so for rclone's own OAuth app on an existing
			// file by id, 2026-10-23). Remember that for the life of the Fs
			// so later opens cost no round trip, say it once, and report a
			// permission error, which the VFS turns into the EPERM an open
			// for an in-place edit gets on a backend without the feature.
			if !f.fileAPIDenied.Swap(true) {
				fs.Logf(f, "pCloud denies its file API to this app (%v): files here are replaced whole, not edited in place", apiErr)
			}
			return nil, fmt.Errorf("OpenRangeWriter: pCloud refused to open the file for writing (%v): %w", apiErr, fs.ErrorPermissionDenied)
		}
		return nil, err
	}
	f.tokenRenewer.Start()
	// The hashes and the download link describe the content as it was.
	o.md5, o.sha1, o.sha256 = "", "", ""
	o.link = nil
	return w, nil
}

// errorOf returns the pCloud API error inside err, if there is one.
func errorOf(err error) (*api.Error, bool) {
	for err != nil {
		if apiErr, ok := err.(*api.Error); ok {
			return apiErr, true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

// rangeWriter is the fs.RangeWriter for a pCloud file.
type rangeWriter struct {
	ctx    context.Context
	o      *Object
	fileID int64
	mu     sync.Mutex
	client *rest.Client // limited to one connection: the descriptor lives on it
	fd     int64
	opened bool  // fd is valid, as far as we know
	size   int64 // the file's size as the writes so far have left it
	closed bool
}

// reopen opens the file for writing on a fresh single-connection client.
func (w *rangeWriter) reopen() error {
	client, err := w.o.fs.newSingleConnClient(w.ctx)
	if err != nil {
		return fmt.Errorf("OpenRangeWriter: client: %w", err)
	}
	res, err := fileOpen(w.ctx, client, w.o.fs, w.fileID)
	if err != nil {
		return fmt.Errorf("OpenRangeWriter: open failed: %w", err)
	}
	w.client, w.fd, w.opened = client, res.FileDescriptor, true
	return nil
}

// do runs op on the descriptor. A failure other than the context ending is
// taken to mean the connection, and the descriptor with it, is gone (pCloud
// error 1007 "Invalid or closed file descriptor", a reset connection): the
// file is reopened and op runs once more. Every op here is idempotent (the
// same bytes at the same offset, the same size).
func (w *rangeWriter) do(what string, op func() error) error {
	if !w.opened {
		if err := w.reopen(); err != nil {
			return err
		}
	}
	err := op()
	if err == nil {
		return nil
	}
	if fserrors.ContextError(w.ctx, &err) {
		return fmt.Errorf("OpenRangeWriter: %s failed: %w", what, err)
	}
	fs.Debugf(w.o, "OpenRangeWriter: %s failed (%v); reopening the file and retrying once", what, err)
	w.opened = false
	if rerr := w.reopen(); rerr != nil {
		return fmt.Errorf("OpenRangeWriter: %s failed: %w (and reopening the file: %v)", what, err, rerr)
	}
	if err = op(); err != nil {
		return fmt.Errorf("OpenRangeWriter: %s failed after reopening the file: %w", what, err)
	}
	return nil
}

// WriteAt writes p at off; pCloud has stored the bytes when it returns.
func (w *rangeWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	err := w.do(fmt.Sprintf("write of %d bytes at %d", len(p), off), func() error {
		res, err := filePWrite(w.ctx, w.client, w.o.fs.pacer, w.fd, off, p)
		if err != nil {
			return err
		}
		if res.Bytes != int64(len(p)) {
			return fmt.Errorf("pCloud wrote %d of %d bytes", res.Bytes, len(p))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if end := off + int64(len(p)); end > w.size {
		w.size = end
	}
	return len(p), nil
}

// Truncate sets the file's size (file_truncate).
func (w *rangeWriter) Truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	err := w.do(fmt.Sprintf("truncate to %d", size), func() error {
		_, err := fileTruncate(w.ctx, w.client, w.o.fs.pacer, w.fd, size)
		return err
	})
	if err != nil {
		return err
	}
	w.size = size
	return nil
}

// Sync is a no-op: pCloud acknowledges a file_pwrite once it holds the
// bytes, and its API has no flush beyond that.
func (w *rangeWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	return nil
}

// Close releases the descriptor and re-reads the file, so the object's
// size and modtime are pCloud's. pCloud's listing can lag a write by a
// moment (the upload path has the same wait), so a size that does not yet
// match what the writes left is re-read a few times before it is an error.
func (w *rangeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	defer w.o.fs.tokenRenewer.Stop()
	if w.opened {
		w.opened = false
		if _, err := fileClose(w.ctx, w.client, w.o.fs.pacer, w.fd); err != nil {
			// Every write was acknowledged on return; a descriptor that is
			// already gone (with its connection) loses nothing. The re-read
			// below is what decides whether the file is as the writes left it.
			fs.Debugf(w.o, "OpenRangeWriter: closing the descriptor failed: %v", err)
		}
	}
	var info *api.Item
	for try := 1; ; try++ {
		var err error
		info, err = w.o.fs.readMetaDataForPath(w.ctx, w.o.remote)
		if err != nil {
			return fmt.Errorf("OpenRangeWriter: re-reading the file after close failed: %w", err)
		}
		if info.Size == w.size || try >= 5 {
			break
		}
		fs.Debugf(w.o, "OpenRangeWriter: pCloud reports size %d after close, the writes left %d; re-reading (%d/5)", info.Size, w.size, try)
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err := w.o.setMetaData(info); err != nil {
		return err
	}
	if info.Size != w.size {
		return fmt.Errorf("OpenRangeWriter: pCloud reports size %d after close, the writes left %d", info.Size, w.size)
	}
	return nil
}

// Call pcloud file_truncate, see [API Doc.]
// [API Doc]: https://docs.pcloud.com/methods/fileops/file_truncate.html
func fileTruncate(ctx context.Context, client *rest.Client, pacer *fs.Pacer, fd, length int64) (*api.FileTruncateResponse, error) {
	opts := rest.Opts{
		Method:           "PUT",
		Path:             "/file_truncate",
		Parameters:       url.Values{},
		TransferEncoding: []string{"identity"}, // pcloud doesn't like chunked encoding
		ExtraHeaders: map[string]string{
			"Connection": "keep-alive",
		},
	}
	opts.Parameters.Set("fd", strconv.FormatInt(fd, 10))
	opts.Parameters.Set("length", strconv.FormatInt(length, 10))

	result := &api.FileTruncateResponse{}
	err := pacer.CallNoRetry(func() (bool, error) {
		resp, err := client.CallJSON(ctx, &opts, nil, result)
		err = result.Error.Update(err)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("truncate fd %d to %d: %w", fd, length, err)
	}
	return result, nil
}

// Check the interface is satisfied
var _ fs.OpenRangeWriter = (*Fs)(nil)

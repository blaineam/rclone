package vfsbridge

// Upload durability for the FSKit volume.
//
// With CacheModeFull a write lands in the local cache and uploads in the background, so
// "the copy finished" used to mean "the bytes are on this Mac". Three things make it mean
// what a person thinks it means:
//
//  1. close(2) of a written file waits for THAT file's upload (confirmClose), and fails
//     with EIO if the upload keeps failing instead of reporting success.
//  2. A copy is held back while too many files still wait to upload (throttleWrites), so
//     a Finder copy proceeds at upload speed instead of racing ahead of it.
//  3. What has not uploaded is visible (UploadStatus) and, if it cannot upload, can be
//     copied somewhere safe with its folder structure intact (RescuePending).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

const (
	// confirmFailAfterTries is how many failed attempts make a close fail. rclone keeps
	// retrying the file in the background with backoff; this only decides when the
	// caller is told.
	confirmFailAfterTries = 3
	// confirmTimeout bounds how long one close waits. A genuinely long upload is fine;
	// this is the backstop for one that never completes or fails.
	confirmTimeout = 30 * time.Minute

	cETIMEDOUT int32 = 60

	// bridgeWriteBack is the delay before a queued upload starts. Not zero: see AddRemote.
	bridgeWriteBack = time.Millisecond
)

// Variables, not constants, so tests can use small values.
var (
	// writeHighWater is how many bytes may wait to upload before writes are held back.
	writeHighWater int64 = 512 << 20
	// writeStallMax bounds how long one write call is held, kept well under the FSKit
	// module's request timeout, so a stalled upload slows a copy but never fails it.
	writeStallMax = 6 * time.Second
)

// SetConfirmUploads turns "close waits for the upload" on or off. On by default.
func (s *Server) SetConfirmUploads(on bool) { s.confirmUploads.Store(on) }

// SetFlowControl turns write back pressure on or off. On by default.
func (s *Server) SetFlowControl(on bool) { s.flowControl.Store(on) }

// confirmClose waits until the file behind a just-closed write handle has uploaded.
// Returns 0 on success or the errno to fail the close with.
func (s *Server) confirmClose(v *vfs.VFS, path string) int32 {
	ctx, cancel := context.WithTimeout(context.Background(), confirmTimeout)
	defer cancel()
	switch err := v.WaitForUpload(ctx, path, confirmFailAfterTries); {
	case err == nil:
		return 0
	case errors.Is(err, vfs.ErrUploadFailing):
		fs.Errorf(nil, "VFS bridge: %q has not uploaded after %d tries; failing the close, the file stays queued and keeps retrying", path, confirmFailAfterTries)
		return cEIO
	case errors.Is(err, context.DeadlineExceeded):
		fs.Errorf(nil, "VFS bridge: %q still uploading after %v; failing the close, the file stays queued", path, confirmTimeout)
		return cETIMEDOUT
	default:
		return cEIO
	}
}

// throttleWrites holds a write while more than writeHighWater bytes still wait to
// upload, for at most writeStallMax. Only CLOSED files count (an open file cannot upload
// yet), so this slows a many-file copy to the pace of the uploads without ever waiting
// on the file being written.
func (s *Server) throttleWrites(v *vfs.VFS) {
	if !s.flowControl.Load() {
		return
	}
	deadline := time.Now().Add(writeStallMax)
	tick := 5 * time.Millisecond
	for {
		if b, _ := v.QueuedUploadBytes(); b <= writeHighWater {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(tick)
		tick = min(tick*2, 100*time.Millisecond)
	}
}

// UploadItem is one file waiting to upload.
type UploadItem struct {
	Remote    string  `json:"remote"`
	Path      string  `json:"path"`
	Size      int64   `json:"size"`
	Uploading bool    `json:"uploading"`
	Tries     int     `json:"tries"`
	RetryIn   float64 `json:"retryIn"` // seconds; negative means due now
	// Failing is true once the file has failed confirmFailAfterTries times.
	Failing bool `json:"failing"`
}

// RemoteUploads is the upload queue of one remote.
type RemoteUploads struct {
	Remote         string       `json:"remote"`
	QueuedFiles    int          `json:"queuedFiles"`
	UploadingFiles int          `json:"uploadingFiles"`
	QueuedBytes    int64        `json:"queuedBytes"`
	FailingFiles   int          `json:"failingFiles"`
	Items          []UploadItem `json:"items"` // capped; failing and uploading first
}

// UploadStatus is the upload queue of every exposed remote.
type UploadStatus struct {
	Remotes      []RemoteUploads `json:"remotes"`
	QueuedFiles  int             `json:"queuedFiles"`
	QueuedBytes  int64           `json:"queuedBytes"`
	FailingFiles int             `json:"failingFiles"`
}

const maxStatusItems = 200

// UploadStatus reports what is waiting to upload, per remote.
func (s *Server) UploadStatus() UploadStatus {
	s.mu.RLock()
	names := make([]string, 0, len(s.vfses))
	for n := range s.vfses {
		names = append(names, n)
	}
	s.mu.RUnlock()
	sort.Strings(names)

	st := UploadStatus{Remotes: []RemoteUploads{}}
	for _, name := range names {
		s.mu.RLock()
		v := s.vfses[name]
		s.mu.RUnlock()
		if v == nil {
			continue
		}
		ru := RemoteUploads{Remote: name, Items: []UploadItem{}}
		for _, q := range v.UploadQueue() {
			ru.QueuedFiles++
			ru.QueuedBytes += q.Size
			if q.Uploading {
				ru.UploadingFiles++
			}
			failing := q.Tries >= confirmFailAfterTries && !q.Uploading
			if failing {
				ru.FailingFiles++
			}
			ru.Items = append(ru.Items, UploadItem{
				Remote: name, Path: q.Name, Size: q.Size, Uploading: q.Uploading,
				Tries: q.Tries, RetryIn: q.Expiry, Failing: failing,
			})
		}
		sort.SliceStable(ru.Items, func(i, j int) bool {
			a, b := ru.Items[i], ru.Items[j]
			if a.Failing != b.Failing {
				return a.Failing
			}
			return a.Uploading && !b.Uploading
		})
		if len(ru.Items) > maxStatusItems {
			ru.Items = ru.Items[:maxStatusItems]
		}
		st.QueuedFiles += ru.QueuedFiles
		st.QueuedBytes += ru.QueuedBytes
		st.FailingFiles += ru.FailingFiles
		st.Remotes = append(st.Remotes, ru)
	}
	return st
}

// RescuedFile is one pending file copied to the recovery folder.
type RescuedFile struct {
	Remote string `json:"remote"`
	Path   string `json:"path"`
	Dest   string `json:"dest,omitempty"`
	Size   int64  `json:"size"`
	// Error says why a file could not be copied; empty when it was.
	Error string `json:"error,omitempty"`
}

// RescueResult is what RescuePending did.
type RescueResult struct {
	Dest    string        `json:"dest"`
	Rescued []RescuedFile `json:"rescued"`
	Skipped []RescuedFile `json:"skipped"`
}

// RescuePending copies files still waiting to upload out of the cache into destDir, as
// destDir/<remote>/<path>, keeping each file's folder structure and modification time.
// With onlyFailing, only files that have failed confirmFailAfterTries times are copied.
//
// It only COPIES. The files stay queued and keep retrying, so nothing is lost if the
// upload succeeds a moment later, and the person decides what to do with the copies. An
// existing file at the destination is never overwritten: the copy gets a numbered name.
func (s *Server) RescuePending(destDir string, onlyFailing bool) RescueResult {
	res := RescueResult{Dest: destDir, Rescued: []RescuedFile{}, Skipped: []RescuedFile{}}
	s.mu.RLock()
	names := make([]string, 0, len(s.vfses))
	for n := range s.vfses {
		names = append(names, n)
	}
	s.mu.RUnlock()
	sort.Strings(names)

	for _, name := range names {
		s.mu.RLock()
		v := s.vfses[name]
		s.mu.RUnlock()
		if v == nil {
			continue
		}
		for _, q := range v.UploadQueue() {
			if onlyFailing && (q.Tries < confirmFailAfterTries || q.Uploading) {
				continue
			}
			item := RescuedFile{Remote: name, Path: q.Name, Size: q.Size}
			pf, ok := v.PendingFile(q.Name)
			switch {
			case !ok:
				item.Error = "no longer pending (it uploaded or was removed)"
				res.Skipped = append(res.Skipped, item)
				continue
			case !pf.Complete:
				item.Error = "only part of this file is on this Mac, so it cannot be saved on its own"
				res.Skipped = append(res.Skipped, item)
				continue
			}
			dest, err := copyOut(pf.DataPath, filepath.Join(destDir, name, filepath.FromSlash(q.Name)), pf.Size, pf.ModTime)
			if err != nil {
				item.Error = err.Error()
				res.Skipped = append(res.Skipped, item)
				continue
			}
			item.Dest = dest
			res.Rescued = append(res.Rescued, item)
		}
	}
	return res
}

// copyOut copies src to dst (creating parents), never overwriting: an existing dst gets
// " (recovered N)" before its extension. The copy is verified by size.
func copyOut(src, dst string, wantSize int64, mtime time.Time) (string, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()

	ext := filepath.Ext(dst)
	base := strings.TrimSuffix(dst, ext)
	target := dst
	var out *os.File
	for n := 1; ; n++ {
		out, err = os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			break
		}
		if !os.IsExist(err) || n > 1000 {
			return "", err
		}
		target = fmt.Sprintf("%s (recovered %d)%s", base, n, ext)
	}
	written, err := io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil && written != wantSize {
		err = fmt.Errorf("copied %d of %d bytes", written, wantSize)
	}
	if err != nil {
		_ = os.Remove(target)
		return "", err
	}
	if !mtime.IsZero() {
		_ = os.Chtimes(target, mtime, mtime)
	}
	return target, nil
}

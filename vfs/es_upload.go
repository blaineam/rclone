package vfs

// Enter Space fork additions. A volume that reports a copy as finished has to know
// whether the bytes reached the remote, and the app wants to show and rescue what has
// not. These read the upload queue and wait on one file. Their own file, so upstream
// merges do not touch them.

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rclone/rclone/vfs/vfscache"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
)

// ErrUploadFailing is returned by WaitForUpload when a file's upload has failed the
// allowed number of times. The file stays queued and keeps retrying.
var ErrUploadFailing = errors.New("upload keeps failing")

// UploadQueue returns the files queued for upload, uploading ones first.
func (vfs *VFS) UploadQueue() []writeback.QueueInfo {
	if vfs.cache == nil {
		return nil
	}
	return vfs.cache.QueueInfos()
}

// QueuedUploadBytes returns the total size and number of files waiting to upload.
func (vfs *VFS) QueuedUploadBytes() (bytes int64, files int) {
	if vfs.cache == nil {
		return 0, 0
	}
	return vfs.cache.QueuedBytes()
}

// PendingFile returns the cache file behind a dirty file, for copying it out.
func (vfs *VFS) PendingFile(remote string) (vfscache.PendingFile, bool) {
	if vfs.cache == nil {
		return vfscache.PendingFile{}, false
	}
	return vfs.cache.PendingFile(strings.Trim(remote, "/"))
}

// WaitForUpload blocks until the file at remote (a path inside this VFS) is no longer
// waiting to upload, i.e. it is on the remote. It returns nil on success,
// ErrUploadFailing once the upload has failed failAfter times (0 never gives up), and
// ctx's error if ctx ends first.
//
// A file that is dirty only because another handle is still open has nothing queued
// until that handle closes, so it is not waited for: this is the close of one handle
// among several, not the close that stores the file.
func (vfs *VFS) WaitForUpload(ctx context.Context, remote string, failAfter int) error {
	if vfs.cache == nil {
		return nil
	}
	remote = strings.Trim(remote, "/")
	tick := 2 * time.Millisecond
	for {
		st := vfs.cache.UploadState(remote)
		if !st.Queued {
			return nil
		}
		if failAfter > 0 && st.Tries >= failAfter && !st.Uploading {
			return ErrUploadFailing
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tick):
		}
		tick = min(tick*2, 250*time.Millisecond)
	}
}

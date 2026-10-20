package vfscache

// Enter Space fork additions: read-only views of the upload queue and of the cache
// file behind a pending upload, so the app can show queue state and, if an upload
// keeps failing, copy the data somewhere safe. Kept in their own file so upstream
// merges do not touch them.

import (
	"time"

	"github.com/rclone/rclone/lib/ranges"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
)

// QueueInfos returns the current upload queue, uploading items first.
func (c *Cache) QueueInfos() []writeback.QueueInfo {
	return c.writeback.Queue()
}

// PendingFile describes a dirty file whose data still lives only in the cache.
type PendingFile struct {
	Name     string    // remote path inside the VFS
	DataPath string    // OS path of the cache file holding the data
	Size     int64     // size the file should have
	ModTime  time.Time // last modification time
	// Complete is false when parts of the file were never fetched into the cache
	// (an in-place edit of a file that was not fully downloaded). Such a cache file
	// cannot be copied out as the user's file.
	Complete bool
}

// PendingFile returns the cache file behind name when that file is dirty.
func (c *Cache) PendingFile(name string) (pf PendingFile, ok bool) {
	item := c.DirtyItem(name)
	if item == nil {
		return pf, false
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	pf = PendingFile{
		Name:     item.name,
		DataPath: c.toOSPath(item.name),
		Size:     item.info.Size,
		ModTime:  item.info.ModTime,
		Complete: item.info.Size == 0 || item.info.Rs.FindMissing(ranges.Range{Pos: 0, Size: item.info.Size}).IsEmpty(),
	}
	return pf, true
}

// UploadState is where one file stands on its way to the remote.
type UploadState struct {
	// Queued is true while the file waits for, or is in, an upload.
	Queued    bool
	Uploading bool
	// Tries is how many upload attempts have failed or are running.
	Tries int
	// RetryIn is the seconds until the next attempt (negative: due now).
	RetryIn float64
	// DirtyOpen is true when the file is dirty but a handle is still open. Nothing
	// is queued until the last handle closes, so a caller must not wait for it.
	DirtyOpen bool
}

// UploadState reports the upload state of name.
func (c *Cache) UploadState(name string) (st UploadState) {
	name = clean(name)
	for _, qi := range c.writeback.Queue() {
		if qi.Name == name {
			return UploadState{Queued: true, Uploading: qi.Uploading, Tries: qi.Tries, RetryIn: qi.Expiry}
		}
	}
	c.mu.Lock()
	item := c.item[name]
	c.mu.Unlock()
	if item != nil {
		item.mu.Lock()
		st.DirtyOpen = item.info.Dirty && item.opens != 0
		item.mu.Unlock()
	}
	return st
}

// QueuedBytes returns the total size and number of files waiting to upload.
func (c *Cache) QueuedBytes() (bytes int64, files int) {
	return c.writeback.QueuedBytes()
}

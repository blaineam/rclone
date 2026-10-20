package writeback

// QueuedBytes returns the total size, and the number, of files queued for upload or
// uploading. Cheap enough to call on every write: the Enter Space bridge uses it to
// hold a copy back while too much is still waiting to upload.
func (wb *WriteBack) QueuedBytes() (bytes int64, files int) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	for _, it := range wb.lookup {
		bytes += it.size
		files++
	}
	return bytes, files
}

//go:build !(darwin && cgo)

package local

import (
	"io"
	"time"
)

func iCloudMaterializeEnabled() bool                         { return false }
func isICloudEvicted(_ string) bool                          { return false }
func materializeICloudFile(_ string, _ time.Duration) error  { return nil }
func evictICloudFile(_ string)                               {}

// icloudEvictOnClose stub — never used on non-darwin but must exist
// so local.go can reference the type unconditionally.
type icloudEvictOnClose struct {
	io.ReadCloser
	path string
}

func (e *icloudEvictOnClose) Close() error {
	return e.ReadCloser.Close()
}

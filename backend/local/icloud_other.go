//go:build !(darwin && cgo)

package local

import (
	"io"
	"time"
)

func iCloudMaterializeEnabled() bool { return false }

// iCloudMaterializeTimeout is referenced unconditionally by Object.Open, so
// the stub must exist too: without it every non-darwin (or CGO_ENABLED=0)
// build of backend/local failed with "undefined: iCloudMaterializeTimeout".
func iCloudMaterializeTimeout() time.Duration               { return 0 }
func isICloudEvicted(_ string) bool                         { return false }
func materializeICloudFile(_ string, _ time.Duration) error { return nil }
func evictICloudFile(_ string)                              {}

// icloudEvictOnClose stub — never used on non-darwin but must exist
// so local.go can reference the type unconditionally.
type icloudEvictOnClose struct {
	io.ReadCloser
	path string
}

func (e *icloudEvictOnClose) Close() error {
	return e.ReadCloser.Close()
}

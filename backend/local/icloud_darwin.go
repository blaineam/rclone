//go:build darwin && cgo

// iCloud Drive file materialization support for macOS and iOS.
//
// When reading files from iCloud Drive–synced directories, some files
// may be "evicted" (offloaded to iCloud, not present on local disk).
// This module detects evicted files and triggers on-demand download
// before rclone attempts to read them, then optionally evicts them
// after the read completes to free local storage.
//
// Enable with the environment variable RCLONE_ICLOUD_MATERIALIZE=true.
// Adjust the per-file timeout with RCLONE_ICLOUD_MATERIALIZE_TIMEOUT
// (duration string, default "5m").

package local

/*
#cgo LDFLAGS: -framework Foundation
#include "icloud_darwin.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/rclone/rclone/fs"
)

const defaultMaterializeTimeout = 5 * time.Minute

// The native calls are reached through these package variables so tests can
// drive the evicted-file paths (Object.Open's materialize/evict branch, the
// poll loop, the timeout) without a real iCloud Drive container. Production
// never reassigns them.
var (
	isICloudEvicted         = isICloudEvictedNative
	requestICloudDownload   = requestICloudDownloadNative
	requestICloudEviction   = requestICloudEvictionNative
	materializePollInterval = 500 * time.Millisecond
)

var (
	icloudEnabled     bool
	icloudEnabledOnce sync.Once
	icloudTimeout     time.Duration
	icloudTimeoutOnce sync.Once
)

// iCloudMaterializeEnabled returns true if iCloud materialization is enabled.
func iCloudMaterializeEnabled() bool {
	icloudEnabledOnce.Do(func() {
		val := os.Getenv("RCLONE_ICLOUD_MATERIALIZE")
		icloudEnabled = strings.EqualFold(val, "true") || val == "1"
	})
	return icloudEnabled
}

// iCloudMaterializeTimeout returns the configured timeout for materialization.
func iCloudMaterializeTimeout() time.Duration {
	icloudTimeoutOnce.Do(func() {
		icloudTimeout = defaultMaterializeTimeout
		if val := os.Getenv("RCLONE_ICLOUD_MATERIALIZE_TIMEOUT"); val != "" {
			if d, err := time.ParseDuration(val); err == nil {
				icloudTimeout = d
			}
		}
	})
	return icloudTimeout
}

// isICloudEvictedNative checks if the file at the given path is an iCloud
// evicted (dataless) stub that needs materialization before reading.
func isICloudEvictedNative(path string) bool {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return C.is_icloud_evicted(cPath) == 1
}

// cErr converts an error message strdup'd by the Objective-C side into a Go
// error, freeing it.
func cErr(cErrMsg *C.char) error {
	if cErrMsg == nil {
		return errors.New("unknown error")
	}
	defer C.free(unsafe.Pointer(cErrMsg))
	return errors.New(C.GoString(cErrMsg))
}

// requestICloudDownloadNative asks iCloud to start downloading the file.
func requestICloudDownloadNative(path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cErrMsg *C.char
	if C.materialize_icloud_file(cPath, &cErrMsg) != 0 {
		return cErr(cErrMsg)
	}
	return nil
}

// requestICloudEvictionNative asks iCloud to evict the file's local copy.
func requestICloudEvictionNative(path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cErrMsg *C.char
	if C.evict_icloud_file(cPath, &cErrMsg) != 0 {
		return cErr(cErrMsg)
	}
	return nil
}

// materializeICloudFile triggers iCloud to download the file and waits
// until it is fully materialized or the timeout expires.
func materializeICloudFile(path string, timeout time.Duration) error {
	// Trigger the download
	if err := requestICloudDownload(path); err != nil {
		return fmt.Errorf("iCloud download request failed: %s", err)
	}

	// Poll until the file is no longer evicted or timeout
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isICloudEvicted(path) {
			return nil
		}
		time.Sleep(materializePollInterval)
	}

	return fmt.Errorf("iCloud materialization timed out after %v", timeout)
}

// evictICloudFile triggers iCloud eviction to free local storage.
// Best-effort: logs warning on failure but does not return error.
func evictICloudFile(path string) {
	if err := requestICloudEviction(path); err != nil {
		fs.Infof(nil, "iCloud: eviction failed for %s: %s", path, err)
	}
}

// icloudEvictOnClose wraps an io.ReadCloser to evict the file after
// the read is complete (on Close). Only evicts if the file was
// originally evicted before materialization.
type icloudEvictOnClose struct {
	io.ReadCloser
	path string
	once sync.Once
}

func (e *icloudEvictOnClose) Close() error {
	err := e.ReadCloser.Close()
	e.once.Do(func() {
		fs.Debugf(nil, "iCloud: evicting %s after read complete", e.path)
		evictICloudFile(e.path)
	})
	return err
}

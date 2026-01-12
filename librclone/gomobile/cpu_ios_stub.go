//go:build ios && arm64

// cpu_ios_stub.go provides a stub implementation of internal/cpu.sysctlEnabled for iOS.
// This is needed because storj.io/common uses //go:linkname to access this function,
// but it only exists in Go's internal/cpu package for macOS (!ios), not iOS.
// See: https://github.com/golang/go/issues/67401

package gomobile

import _ "unsafe"

//go:linkname sysctlEnabled internal/cpu.sysctlEnabled
func sysctlEnabled(name []byte) bool {
	// On iOS, we cannot use sysctl, so we return false for all CPU feature checks.
	// This is safe - it just means the optimized code paths won't be used.
	return false
}

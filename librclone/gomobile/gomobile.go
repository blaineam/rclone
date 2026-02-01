// Package rclone exports shims for gomobile use
package rclone

import (
	"github.com/rclone/rclone/librclone/librclone"

	_ "github.com/rclone/rclone/backend/all" // import all backends
	_ "github.com/rclone/rclone/lib/plugin"  // import plugins

	_ "golang.org/x/mobile/event/key" // make go.mod add this as a dependency
)

// Initialize initializes rclone as a library
// Exported as RcloneInitialize
func Initialize() {
	librclone.Initialize()
}

// Finalize finalizes the library
// Exported as RcloneFinalize
func Finalize() {
	librclone.Finalize()
}

// RPCResult is returned from RPC
// Exported as RcloneRPCResult
//
//	Output will be returned as a serialized JSON object
//	Status is a HTTP status return (200=OK anything else fail)
type RPCResult struct {
	Output string
	Status int
}

// RPC has an interface optimised for gomobile, in particular
// the function signature is valid under gobind rules.
// Exported as RcloneRPC
//
// https://pkg.go.dev/golang.org/x/mobile/cmd/gobind#hdr-Type_restrictions
func RPC(method string, input string) (result *RPCResult) { //nolint:deadcode
	output, status := librclone.RPC(method, input)
	return &RPCResult{
		Output: output,
		Status: status,
	}
}

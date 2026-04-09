// Package rclone exports shims for gomobile use
package rclone

import (
	"fmt"
	"sync"

	"github.com/rclone/rclone/librclone/librclone"
	"github.com/rclone/rclone/librclone/vfsbridge"

	_ "github.com/rclone/rclone/backend/all" // import all backends
	_ "github.com/rclone/rclone/lib/plugin"  // import plugins

	_ "golang.org/x/mobile/event/key" // make go.mod add this as a dependency
)

var (
	bridgeServer *vfsbridge.Server
	bridgeMu     sync.Mutex
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

// StartVFSBridge starts a TCP server that bridges FSKitBridge's protobuf
// protocol to rclone's VFS layer for the given remote.
// Returns "host:port" on success, or "error: message" on failure.
// Exported as RcloneStartVFSBridge
func StartVFSBridge(remoteName string, port int) string {
	bridgeMu.Lock()
	defer bridgeMu.Unlock()

	if bridgeServer == nil {
		bridgeServer = vfsbridge.NewServer()
		addr := fmt.Sprintf("localhost:%d", port)
		listenAddr, err := bridgeServer.Start(addr)
		if err != nil {
			bridgeServer = nil
			return "error: " + err.Error()
		}
		if err := bridgeServer.AddRemote(remoteName); err != nil {
			bridgeServer.Stop()
			bridgeServer = nil
			return "error: " + err.Error()
		}
		return listenAddr
	}

	// Server already running, just add remote
	if err := bridgeServer.AddRemote(remoteName); err != nil {
		return "error: " + err.Error()
	}
	return bridgeServer.ListenAddr()
}

// StopVFSBridge stops the VFS bridge server.
// Exported as RcloneStopVFSBridge
func StopVFSBridge() {
	bridgeMu.Lock()
	defer bridgeMu.Unlock()

	if bridgeServer != nil {
		bridgeServer.Stop()
		bridgeServer = nil
	}
}

// Package rclone exports shims for gomobile use
package rclone

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rclone/rclone/librclone/librclone"
	"github.com/rclone/rclone/librclone/vfsbridge"

	// Backend imports are handled by rclone_direct.go (copied from rclone.go
	// at build time) which selectively imports only Tier 1-3 backends.
	// Do NOT import backend/all here — it pulls in all 73 backends and
	// negates the selective imports used to reduce binary size.
	_ "github.com/rclone/rclone/lib/plugin" // import plugins

	_ "golang.org/x/mobile/event/key" // make go.mod add this as a dependency
)

var (
	bridgeServer *vfsbridge.Server
	// pendingWriteThrough holds streaming-write choices made before the bridge started.
	pendingWriteThrough = map[string]bool{}
	bridgeMu            sync.Mutex
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
		for name, on := range pendingWriteThrough {
			bridgeServer.SetWriteThrough(name, on)
		}
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

// RemoveVFSBridgeRemote withdraws one remote from the running bridge, leaving
// every other remote's VFS and cache untouched. Returns "" on success, or
// "error: message" on failure. Removing a remote that is not exposed, or
// calling this with no bridge running, succeeds.
//
// Use this rather than StopVFSBridge + a round of StartVFSBridge calls when the
// user toggles a remote off: restarting the bridge drops every remote's cache
// and puts each survivor through a fresh fs.NewFs, which is network I/O.
// Exported as RcloneRemoveVFSBridgeRemote
func RemoveVFSBridgeRemote(remoteName string) string {
	bridgeMu.Lock()
	server := bridgeServer
	bridgeMu.Unlock()

	if server == nil {
		return ""
	}
	if err := server.RemoveRemote(remoteName); err != nil {
		return "error: " + err.Error()
	}
	return ""
}

// VFSBridgeGeneration returns a counter that changes whenever the set of
// remotes the bridge exposes changes. Zero when no bridge is running.
// Exported as RcloneVFSBridgeGeneration
func VFSBridgeGeneration() int64 {
	bridgeMu.Lock()
	defer bridgeMu.Unlock()

	if bridgeServer == nil {
		return 0
	}
	return int64(bridgeServer.Generation())
}

// VFSBridgeUploadStatus returns what is waiting to upload through the bridge, per
// remote, as JSON: {"remotes":[{"remote","queuedFiles","uploadingFiles","queuedBytes",
// "failingFiles","items":[{"remote","path","size","uploading","tries","retryIn",
// "failing"}]}],"queuedFiles","queuedBytes","failingFiles"}. Empty when no bridge runs.
// Exported as RcloneVFSBridgeUploadStatus
func VFSBridgeUploadStatus() string {
	bridgeMu.Lock()
	server := bridgeServer
	bridgeMu.Unlock()

	status := vfsbridge.UploadStatus{Remotes: []vfsbridge.RemoteUploads{}}
	if server != nil {
		status = server.UploadStatus()
	}
	out, err := json.Marshal(status)
	if err != nil {
		return "{}"
	}
	return string(out)
}

// RescueVFSBridgePendingUploads copies files still waiting to upload out of the cache
// into destDir as <remote>/<path>, keeping their folder structure, and returns the
// result as JSON: {"dest","rescued":[{"remote","path","dest","size"}],"skipped":[...,"error"]}.
// With onlyFailing, only files whose upload keeps failing are copied. The files stay
// queued and keep retrying. Returns "error: message" on failure.
// Exported as RcloneRescueVFSBridgePendingUploads
func RescueVFSBridgePendingUploads(destDir string, onlyFailing bool) string {
	bridgeMu.Lock()
	server := bridgeServer
	bridgeMu.Unlock()

	if server == nil {
		return "error: no bridge running"
	}
	if destDir == "" {
		return "error: no destination"
	}
	out, err := json.Marshal(server.RescuePending(destDir, onlyFailing))
	if err != nil {
		return "error: " + err.Error()
	}
	return string(out)
}

// SetVFSBridgeWriteThrough chooses whether writes to remoteName stream straight to the
// remote (true) or land in the local cache and upload in the background (false, the
// default). It takes effect the next time the remote is added to the volume. Streaming
// needs a backend that can upload without knowing the size first; otherwise the remote
// keeps the normal mode, and VFSBridgeUploadStatus says so. Works before the bridge starts.
// Exported as RcloneSetVFSBridgeWriteThrough
func SetVFSBridgeWriteThrough(remoteName string, on bool) {
	bridgeMu.Lock()
	defer bridgeMu.Unlock()

	if bridgeServer == nil {
		pendingWriteThrough[remoteName] = on
		return
	}
	bridgeServer.SetWriteThrough(remoteName, on)
}

// SetVFSBridgeConfirmUploads chooses whether closing a written file waits for its
// upload (the default) or returns once it is queued. A no-op with no bridge running.
// Exported as RcloneSetVFSBridgeConfirmUploads
func SetVFSBridgeConfirmUploads(on bool) {
	bridgeMu.Lock()
	server := bridgeServer
	bridgeMu.Unlock()

	if server != nil {
		server.SetConfirmUploads(on)
	}
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

// Package vfsbridge provides a TCP server that bridges JSON requests
// to rclone's VFS layer. This allows FSKit extensions to access
// rclone remotes as POSIX filesystems via a localhost TCP connection.
package vfsbridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// Server manages a TCP server bridging JSON requests to rclone VFS.
type Server struct {
	listener net.Listener
	vfses    map[string]*vfs.VFS
	mu       sync.RWMutex
	closed   atomic.Bool
	inodes   *InodeTable
	handles  *HandleTable
	// generation counts changes to the set of exposed remotes. Every response
	// carries it so the FSKit side can use it as the root's directory verifier
	// and notice a remote appearing or disappearing without polling for it.
	generation atomic.Uint64
	// rootModTime is when that set last changed, in Unix seconds, and is what
	// the synthetic root reports as its mtime. Held separately from generation
	// because a counter is not a timestamp and clients compare mtimes.
	rootModTime atomic.Int64
	// prefetchSlots caps how many speculative directory reads run at once.
	// Buffered to prefetchConcurrency; a non-blocking send is the admission
	// test, so a full buffer drops the prefetch instead of queueing it.
	prefetchSlots chan struct{}
}

const (
	// Per-listing cap on speculative child reads.
	prefetchMaxChildren = 24
	// Server-wide cap on speculative reads in flight. Small on purpose: these
	// must never compete with the listing the user is actually waiting for.
	prefetchConcurrency = 4
)

// NewServer creates a new VFS bridge server.
func NewServer() *Server {
	s := &Server{
		vfses:         make(map[string]*vfs.VFS),
		inodes:        NewInodeTable(),
		handles:       NewHandleTable(),
		prefetchSlots: make(chan struct{}, prefetchConcurrency),
	}
	// Start at 1: zero is FSDirectoryVerifierInitial on the FSKit side, which
	// means "no verifier yet" rather than a generation the module chose.
	s.generation.Store(1)
	s.rootModTime.Store(time.Now().Unix())
	return s
}

// bumpRemoteSet records that the exposed remote set just changed, moving both
// the generation the FSKit side uses as a directory verifier and the mtime the
// synthetic root reports.
func (s *Server) bumpRemoteSet() {
	s.generation.Add(1)
	s.rootModTime.Store(time.Now().Unix())
}

// Generation returns the current remote-set generation.
func (s *Server) Generation() uint64 { return s.generation.Load() }

// AddRemote initializes a VFS for the given remote and adds it to the server.
// Uses recover to prevent panics in rclone internals from crashing the host app.
func (s *Server) AddRemote(remoteName string) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic in AddRemote(%q): %v", remoteName, r)
			fs.Errorf(nil, "VFS bridge: %v", retErr)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vfses[remoteName]; exists {
		return nil
	}

	// Use fs.NewFs directly instead of cache.Get to avoid the rc/jobs
	// dependency which requires the RC server to be fully initialized.
	f, err := fs.NewFs(context.Background(), remoteName+":")
	if err != nil {
		return fmt.Errorf("failed to create fs for remote %q: %w", remoteName, err)
	}

	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeFull
	// Flush dirty files as soon as their last handle closes, rather than
	// waiting the default 5s write-back timer. A network volume that reports
	// close(2) success while the bytes are still only in a local cache is
	// lying about durability — and is exactly what the FSKit suite's
	// create-write-read tests catch as "acknowledged but never stored".
	opt.WriteBack = 0
	opt.DirCacheTime = fs.Duration(5 * time.Minute)
	opt.PollInterval = fs.Duration(1 * time.Minute)

	// How far the cache downloader runs AHEAD of the reader. This was 128KiB,
	// which is barely ahead at all: with CacheModeFull every read then waits on
	// its own backend round trip instead of landing in already-fetched cache.
	//
	// Measured cold on a crypt-wrapped remote, 40MB file: 60 reads of about
	// 680KB averaging 409ms EACH = 2.0 MB/s. With 128MiB of read-ahead the same
	// workload runs at 10.0 MB/s -- 5x, from this one line.
	//
	// 128MiB matches rclone's own vfs_read_chunk_size default, so the
	// downloader can stay a whole chunk ahead. It costs disk in the VFS cache,
	// not RAM, because CacheModeFull writes ahead into the cache file.
	//
	// For scale: the File Provider reads the same remote cold at 3.5 MB/s, so
	// 10 MB/s is roughly 3x the other path in this app rather than something
	// still lagging it. An earlier 114 MB/s figure for the File Provider was a
	// measurement error -- that file had already been materialised locally, so
	// it timed a disk read against a network fetch. Verify `stat -f %b` is 0
	// before trusting any "cold" number from that path.
	opt.ReadAhead = 128 * fs.Mebi

	// Left at rclone's default of 0 (single stream) DELIBERATELY.
	//
	// Measured, not assumed: with ReadAhead at 128MiB, ChunkStreams=4 gave
	// 8.0 MB/s and ChunkStreams=0 gave 10.0 MB/s on the same file and remote.
	// Parallel ranged reads against a crypt-wrapped remote pay a seek and a
	// fresh nonce derivation per stream, and that costs about 20% here.
	opt.ChunkStreams = 0

	v := vfs.New(context.Background(), f, &opt)
	s.vfses[remoteName] = v
	s.bumpRemoteSet()

	fs.Infof(nil, "VFS bridge: added remote %q", remoteName)
	return nil
}

// RemoveRemote withdraws a remote from the volume, flushing its pending
// uploads and releasing its VFS. Removing one that is not exposed is not an
// error.
//
// This is what lets the app toggle a remote off without restarting the bridge.
// Stopping the whole server to drop one remote takes every OTHER remote's VFS
// and cache down with it, and forces each survivor through a fresh fs.NewFs --
// network round trips for an auth refresh and a root listing -- on the way back
// up.
//
// The flush comes first and is not optional. With CacheModeFull a write lands
// in the local cache and uploads asynchronously, so a VFS shut down with
// uploads still queued discards them silently: the file was acknowledged,
// readable through the mount, and never stored.
func (s *Server) RemoveRemote(remoteName string) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic in RemoveRemote(%q): %v", remoteName, r)
			fs.Errorf(nil, "VFS bridge: %v", retErr)
		}
	}()

	s.mu.Lock()
	v, exists := s.vfses[remoteName]
	if !exists {
		s.mu.Unlock()
		return nil
	}
	delete(s.vfses, remoteName)
	s.bumpRemoteSet()
	s.mu.Unlock()

	// Everything below runs outside s.mu: closing handles and draining uploads
	// both block for as long as the backend takes, and holding the lock across
	// that would stall every other remote's requests.
	//
	// Close first, THEN drain. A handle still open is a writer the VFS is still
	// waiting on, so draining first means waiting out the whole timeout for
	// writers that only this loop can release -- 30s to toggle off a remote
	// with a file open. Closing queues the upload; the drain then has something
	// finite to wait for.
	for _, id := range s.inodes.UnbindSubtree(remoteName) {
		for _, h := range s.handles.PopAll(id) {
			if err := h.Close(); err != nil {
				fs.Errorf(nil, "VFS bridge: closing handle on item %d of removed remote %q: %v",
					id, remoteName, err)
			}
		}
	}
	v.WaitForWriters(removeFlushTimeout)
	v.Shutdown()

	fs.Infof(nil, "VFS bridge: removed remote %q", remoteName)
	return nil
}

// How long removing a remote waits for its queued uploads before giving up on
// them. Generous: this runs off the FSKit request path, and the alternative to
// waiting is discarding a write the user was told had succeeded.
const removeFlushTimeout = 30 * time.Second

// Start begins listening on the given address. Pass "localhost:0" for random port.
func (s *Server) Start(addr string) (string, error) {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("failed to listen: %w", err)
	}

	listenAddr := s.listener.Addr().String()
	fs.Infof(nil, "VFS bridge: listening on %s", listenAddr)

	purgeCachedDesktopServicesStores()

	go s.acceptLoop()
	return listenAddr, nil
}

// purgeCachedDesktopServicesStores deletes any .DS_Store the VFS cache is still
// holding for upload.
//
// Refusing new ones in doCreate is only half the fix. A .DS_Store written
// BEFORE that guard existed is already sitting in the write-back cache, and the
// VFS retries it on every single launch — forever, because the upload fails
// against a non-writable remote root. On the machine this was found on there
// were ten of them, and the errors reappeared seconds after startup no matter
// how many times the app was reinstalled. Without this, every existing user
// keeps those errors after updating and there is nothing they can do about it.
//
// Both trees matter: `vfs/` holds the file data and `vfsMeta/` the write-back
// bookkeeping that makes it a pending upload. Removing only one would leave the
// cache internally inconsistent.
//
// Scoped to the exact filename, best-effort, and never fatal — a cache we
// cannot tidy is not a reason to refuse to mount.
func purgeCachedDesktopServicesStores() {
	cacheDir := config.GetCacheDir()
	if cacheDir == "" {
		return
	}
	removed := 0
	for _, sub := range []string{"vfs", "vfsMeta"} {
		root := filepath.Join(cacheDir, sub)
		if _, err := os.Stat(root); err != nil {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil // unreadable subtree: skip it, don't abort the walk
			}
			if d.IsDir() || !isDesktopServicesStore(d.Name()) {
				return nil
			}
			if rmErr := os.Remove(path); rmErr == nil {
				removed++
			}
			return nil
		})
	}
	if removed > 0 {
		fs.Infof(nil, "VFS bridge: dropped %d cached .DS_Store upload(s) — Finder scratch is never stored remotely", removed)
	}
}

// ListenAddr returns the address the server is listening on.
func (s *Server) ListenAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop shuts down the server and all VFS instances.
// WaitForWriters blocks until every mounted remote has flushed its pending
// uploads, or until timeout elapses.
//
// With CacheModeFull a write lands in the local cache and is uploaded
// asynchronously, so "the handle closed" does not mean "the data is on the
// remote". Anything that claims durability -- sync, unmount, deactivate,
// shutdown -- has to go through here first.
func (s *Server) WaitForWriters(timeout time.Duration) {
	s.mu.RLock()
	vfses := make([]*vfs.VFS, 0, len(s.vfses))
	for _, v := range s.vfses {
		vfses = append(vfses, v)
	}
	s.mu.RUnlock()

	for _, v := range vfses {
		v.WaitForWriters(timeout)
	}
}

func (s *Server) Stop() {
	if s.closed.Swap(true) {
		return
	}
	if s.listener != nil {
		s.listener.Close()
	}
	// Flush before tearing anything down. Shutting a VFS down with uploads
	// still queued discards them silently.
	s.WaitForWriters(shutdownFlushTimeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, v := range s.vfses {
		v.Shutdown()
		fs.Infof(nil, "VFS bridge: shutdown VFS for %q", name)
	}
	s.vfses = make(map[string]*vfs.VFS)
}

// How long to wait for queued uploads at a durability point.
const shutdownFlushTimeout = 30 * time.Second

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			fs.Errorf(nil, "VFS bridge: accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	fs.Infof(nil, "VFS bridge: new connection from %s", conn.RemoteAddr())

	for {
		if s.closed.Load() {
			return
		}

		data, err := readLengthPrefixed(conn)
		if err != nil {
			if err != io.EOF && !s.closed.Load() {
				fs.Errorf(nil, "VFS bridge: read error: %v", err)
			}
			return
		}

		var req Request
		if err := json.Unmarshal(data, &req); err != nil {
			fs.Errorf(nil, "VFS bridge: unmarshal error: %v", err)
			continue
		}

		resp := s.handleRequest(&req)

		respData, err := json.Marshal(resp)
		if err != nil {
			fs.Errorf(nil, "VFS bridge: marshal error: %v", err)
			continue
		}

		if err := writeLengthPrefixed(conn, respData); err != nil {
			fs.Errorf(nil, "VFS bridge: write error: %v", err)
			return
		}
	}
}

// readLengthPrefixed reads a 4-byte big-endian length prefix + data.
func readLengthPrefixed(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 {
		return []byte{}, nil
	}
	if length > 64*1024*1024 {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}
	data := make([]byte, length)
	_, err := io.ReadFull(r, data)
	return data, err
}

// writeLengthPrefixed writes a 4-byte big-endian length prefix + data.
func writeLengthPrefixed(w io.Writer, data []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// --- JSON message types ---

// Request is the JSON request from the FSKit extension.
type Request struct {
	ID   uint64          `json:"id"`
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args,omitempty"`
}

// Response is the JSON response to the FSKit extension.
type Response struct {
	ID     uint64      `json:"id"`
	Ok     bool        `json:"ok"`
	Error  int32       `json:"error,omitempty"`
	Result interface{} `json:"result,omitempty"`
	// Gen is the remote-set generation current when this response was built.
	// It rides on every reply rather than living behind its own op so the FSKit
	// side learns that a remote appeared or disappeared from traffic it was
	// making anyway, with no polling and no extra round trip.
	Gen uint64 `json:"gen,omitempty"`
}

// ItemInfo is the JSON representation of a file/directory.
type ItemInfo struct {
	ID       uint64 `json:"id"`
	ParentID uint64 `json:"parentId"`
	Name     string `json:"name"`
	IsDir    bool   `json:"isDir"`
	Size     int64  `json:"size"`
	Mode     uint32 `json:"mode"`
	ModTime  int64  `json:"modTime"`
	UID      uint32 `json:"uid"`
	GID      uint32 `json:"gid"`
}

// StatFSInfo is volume statistics.
type StatFSInfo struct {
	TotalBytes uint64 `json:"totalBytes"`
	FreeBytes  uint64 `json:"freeBytes"`
	UsedBytes  uint64 `json:"usedBytes"`
}

// DirEntry is an entry in directory enumeration.
type DirEntry struct {
	Item   ItemInfo `json:"item"`
	Cookie uint64   `json:"cookie"`
}

// POSIX error codes for darwin
const (
	cEPERM     int32 = 1
	cENOENT    int32 = 2
	cEIO       int32 = 5
	cEEXIST    int32 = 17
	cEXDEV     int32 = 18
	cENOTDIR   int32 = 20
	cEISDIR    int32 = 21
	cEINVAL    int32 = 22
	cENOSPC    int32 = 28
	cEROFS     int32 = 30
	cENOTEMPTY int32 = 66
	cENOSYS    int32 = 78
)

func errorResp(id uint64, errno int32) *Response {
	return &Response{ID: id, Ok: false, Error: errno}
}

func okResp(id uint64, result interface{}) *Response {
	return &Response{ID: id, Ok: true, Result: result}
}

func mapVFSErr(err error) int32 {
	if os.IsNotExist(err) {
		return cENOENT
	}
	if os.IsExist(err) {
		return cEEXIST
	}
	if os.IsPermission(err) {
		return cEPERM
	}
	switch err {
	case vfs.ENOTEMPTY:
		return cENOTEMPTY
	case vfs.EROFS:
		return cEROFS
	case vfs.ENOSYS:
		return cENOSYS
	}
	// EIO is the fallback for "we do not recognise this error", so on its own it
	// tells a caller nothing and the underlying cause is discarded here -- which
	// is where it is needed. An operation inside a remote failing EIO after ~18s
	// looks identical whether the backend is unreachable, refusing, or timing
	// out, and the FSKit side can only ever see the number. Say what it was.
	fs.Errorf(nil, "VFS bridge: unmapped error -> EIO: %v", err)
	return cEIO
}

func (s *Server) remoteNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.vfses))
	for name := range s.vfses {
		names = append(names, name)
	}
	return names
}

// isAggregateRoot reports whether id refers to the SYNTHETIC root directory --
// the volume's own root, whose children are the remote names rather than any
// one remote's contents.
//
// That root is not backed by a vfs.Node, so it is absent from the inode table
// and every lookup through InodeTable.GetNode returns nil for it. Callers that
// resolve an id straight through GetNode therefore reported ENOENT for the
// mount's own root. doLookup and doReadDir special-cased it from the start;
// nothing else did, and doGetAttr not doing so broke mounting outright: the
// first thing FSKit does after activate is fetch the root's type, so the volume
// failed at fetchAndSetTypeForItem with ENOENT before any user-visible
// operation ran.
//
// The root is synthetic REGARDLESS OF HOW MANY REMOTES ARE EXPOSED, including
// one and none. It used to be synthetic only above one remote, with a single
// remote's own root bound to this id instead -- which meant the identity of
// inode 2 depended on a count that the app changes at runtime. Adding a second
// remote to a one-remote volume, or removing the second of two, silently
// changed what the mount's root WAS while the kernel held a vnode for it, so
// the set of exposed remotes could only be changed by unmounting first. Keeping
// the shape invariant is what makes AddRemote and RemoveRemote safe on a live
// mount.
func (s *Server) isAggregateRoot(id uint64) bool {
	return id == RootInodeID
}

// handleRequest dispatches a JSON request to the appropriate VFS operation,
// stamping every reply with the remote-set generation it was answered under.
func (s *Server) handleRequest(req *Request) *Response {
	resp := s.dispatch(req)
	resp.Gen = s.generation.Load()
	return resp
}

func (s *Server) dispatch(req *Request) *Response {
	switch req.Op {
	case "getResourceIdentifier":
		return okResp(req.ID, map[string]string{
			"name": "Enter Space", "containerId": "com.blainemiller.ReSpace.FSKit",
		})
	case "getVolumeIdentifier":
		return okResp(req.ID, map[string]string{
			"id": "enterspace-volume", "name": "Enter Space",
		})
	case "getVolumeBehavior":
		return okResp(req.ID, map[string]bool{
			"xattrInhibited": true, "accessCheckInhibited": true,
			"renameInhibited": true, "preallocateInhibited": true,
		})
	case "getPathConf":
		return okResp(req.ID, map[string]int{"maxLinkCount": 1, "maxNameLength": 255})
	case "getCapabilities":
		return okResp(req.ID, map[string]interface{}{
			"persistentObjectIds": true, "symbolicLinks": false,
			"hardLinks": false, "2tbFiles": true,
			"64bitObjectIds": true, "caseSensitive": true,
		})
	case "statfs":
		return s.doStatFS(req)
	case "mount":
		return okResp(req.ID, nil)
	case "unmount", "sync", "deactivate":
		// These are durability points. Returning ok without flushing meant a
		// file written just before an unmount -- or before the app quit -- was
		// reported as safely stored and then silently dropped with the cache.
		s.WaitForWriters(shutdownFlushTimeout)
		return okResp(req.ID, nil)
	case "activate":
		return s.doActivate(req)
	case "getattr":
		return s.doGetAttr(req)
	case "setattr":
		return s.doSetAttr(req)
	case "lookup":
		return s.doLookup(req)
	case "reclaim":
		return s.doReclaim(req)
	case "create":
		return s.doCreate(req)
	case "remove":
		return s.doRemove(req)
	case "rename":
		return s.doRename(req)
	case "readdir":
		return s.doReadDir(req)
	case "open":
		return s.doOpen(req)
	case "close":
		return s.doClose(req)
	case "read":
		return s.doRead(req)
	case "write":
		return s.doWrite(req)
	case "access":
		return okResp(req.ID, true)
	case "debugGoroutines":
		// Full goroutine dump, over the bridge's own channel.
		//
		// This exists because there was no other way to get one. The Go code
		// runs as a gomobile c-archive inside the app, where: rclone's own
		// fs.Infof output goes nowhere the app surfaces; SIGQUIT produces no
		// traceback (the c-archive runtime installs no handler); and rclone's
		// rc server does expose /debug/pprof but behind bcrypt htpasswd auth
		// whose password lives in the Keychain. So a hang in here was
		// unobservable by any means -- which is precisely the condition that
		// let one survive several measurement cycles.
		//
		// Safe to ship: this socket is loopback-only and already exposes every
		// file operation on every mounted remote, so stack traces add no
		// meaningful exposure, and being able to ask a stuck volume what it is
		// stuck on is worth far more in the field than it costs.
		var buf bytes.Buffer
		if p := pprof.Lookup("goroutine"); p != nil {
			_ = p.WriteTo(&buf, 2)
		}
		return okResp(req.ID, buf.String())
	default:
		return errorResp(req.ID, cENOSYS)
	}
}

func (s *Server) doStatFS(req *Request) *Response {
	// Sum across every mounted remote. The old form reported getFirstVFS()'s
	// figures as the whole volume's, and "first" is arbitrary Go map iteration
	// order -- so on a multi-remote volume `df` reported one randomly chosen
	// remote's capacity, and reported a DIFFERENT one from mount to mount.
	s.mu.RLock()
	vfses := make([]*vfs.VFS, 0, len(s.vfses))
	for _, v := range s.vfses {
		vfses = append(vfses, v)
	}
	s.mu.RUnlock()

	if len(vfses) == 0 {
		return okResp(req.ID, StatFSInfo{})
	}

	var totalSum, usedSum, freeSum uint64
	for _, v := range vfses {
		total, used, free := v.Statfs()
		// Statfs returns -1 for "unknown" on backends that cannot report it.
		// Treat unknown as zero rather than letting it wrap to a huge uint64.
		if total > 0 {
			totalSum += uint64(total)
		}
		if used > 0 {
			usedSum += uint64(used)
		}
		if free > 0 {
			freeSum += uint64(free)
		}
	}
	return okResp(req.ID, StatFSInfo{
		TotalBytes: totalSum, FreeBytes: freeSum, UsedBytes: usedSum,
	})
}

func (s *Server) doActivate(req *Request) *Response {
	return okResp(req.ID, s.inodes.GetOrCreateRootInfo(s.rootModTime.Load()))
}

func (s *Server) nodeToItemInfo(node vfs.Node, id uint64) ItemInfo {
	mode := uint32(0644)
	if node.IsDir() {
		mode = 0755
	}
	return ItemInfo{
		ID: id, ParentID: s.inodes.GetParentID(id),
		Name: node.Name(), IsDir: node.IsDir(),
		Size: node.Size(), Mode: mode,
		ModTime: node.ModTime().Unix(),
		UID:     501, GID: 20,
	}
}

func (s *Server) doGetAttr(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	if s.isAggregateRoot(args.ItemID) {
		return okResp(req.ID, s.inodes.GetOrCreateRootInfo(s.rootModTime.Load()))
	}

	node := s.inodes.GetNode(args.ItemID)
	if node == nil {
		return errorResp(req.ID, cENOENT)
	}
	return okResp(req.ID, s.nodeToItemInfo(node, args.ItemID))
}

func (s *Server) doSetAttr(req *Request) *Response {
	var args struct {
		ItemID  uint64 `json:"itemId"`
		Size    *int64 `json:"size,omitempty"`
		ModTime *int64 `json:"modTime,omitempty"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	if s.isAggregateRoot(args.ItemID) {
		// The aggregate root is synthesised, not stored anywhere.
		return errorResp(req.ID, cEPERM)
	}

	node := s.inodes.GetNode(args.ItemID)
	if node == nil {
		return errorResp(req.ID, cENOENT)
	}
	if args.Size != nil {
		if err := node.Truncate(*args.Size); err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
	}
	if args.ModTime != nil {
		t := time.Unix(*args.ModTime, 0)
		_ = node.SetModTime(t)
	}
	return okResp(req.ID, s.nodeToItemInfo(node, args.ItemID))
}

func (s *Server) doLookup(req *Request) *Response {
	var args struct {
		DirID uint64 `json:"dirId"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}

	if s.isAggregateRoot(args.DirID) {
		s.mu.RLock()
		v, ok := s.vfses[args.Name]
		s.mu.RUnlock()
		if !ok {
			return errorResp(req.ID, cENOENT)
		}
		root, _ := v.Root()
		childID := s.inodes.Assign(root, args.Name)
		info := s.nodeToItemInfo(root, childID)
		// The node here is the REMOTE'S OWN ROOT, whose vfs name is "/" -- not
		// the name the caller asked for. Reporting node.Name() therefore
		// answered "look up fskittest_a" with "found it, and it is called /".
		//
		// That is not a cosmetic mislabel. FSKit takes this string as the item's
		// real on-disk name (the field exists so a volume can correct case or
		// Unicode composition), and "/" is not a name any item can have. FSKit
		// rejects the reply and then never completes the request -- so lookup of
		// ANY path below a remote hung forever and, because BridgeClient
		// serialises every operation on one socket behind one lock, the whole
		// volume stopped answering behind it. doReadDir always set this field
		// explicitly, which is why listing the root worked while descending
		// into it did not.
		info.Name = args.Name
		return okResp(req.ID, info)
	}

	parentNode := s.inodes.GetNode(args.DirID)
	if parentNode == nil {
		return errorResp(req.ID, cENOENT)
	}
	dir, ok := parentNode.(*vfs.Dir)
	if !ok {
		return errorResp(req.ID, cENOTDIR)
	}
	child, err := dir.Stat(args.Name)
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	childPath := s.inodes.GetPath(args.DirID) + "/" + args.Name
	childID := s.inodes.Assign(child, childPath)
	return okResp(req.ID, s.nodeToItemInfo(child, childID))
}

func (s *Server) doReclaim(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	// Close anything still open for this item. Reclaim is where FSKit
	// guarantees nothing references the item any more, so a handle surviving to
	// here was never closed and never will be. Leaving it open leaves an
	// un-uploaded file and a node that blocks any later rename onto it.
	for _, h := range s.handles.PopAll(args.ItemID) {
		if err := h.Close(); err != nil {
			fs.Errorf(nil, "VFS bridge: reclaim closed a leaked handle for item %d with error: %v",
				args.ItemID, err)
		} else {
			fs.Errorf(nil, "VFS bridge: reclaim closed a handle for item %d that was never closed",
				args.ItemID)
		}
	}
	s.inodes.Remove(args.ItemID)
	return okResp(req.ID, nil)
}

// isDesktopServicesStore reports whether name is Finder's per-directory view
// state file.
//
// Finder writes `.DS_Store` into every directory a user so much as looks at,
// including the root of a mounted volume. On this bridge that means uploading
// it to the REMOTE — where, if the remote's root is not writable (an SFTP
// account chrooted to a share list, an S3 bucket root), the create fails and
// rclone then logs a failed partial-copy cleanup on top of it. The user sees a
// stack of alarming errors about a file they never created and do not want
// backed up.
//
// `.DS_Store` is already in `DefaultSyncExcludes.regenerable` on the Swift
// side, but that only governs scheduled syncs; writes through the mount never
// pass a sync filter, which is why excluding it there was not enough.
func isDesktopServicesStore(name string) bool {
	return name == ".DS_Store"
}

func (s *Server) doCreate(req *Request) *Response {
	var args struct {
		DirID uint64 `json:"dirId"`
		Name  string `json:"name"`
		IsDir bool   `json:"isDir"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}

	// Never persist Finder's view-state file to a remote. EPERM rather than a
	// fake success: pretending the write worked would leave Finder believing a
	// file exists that no later read can produce. Finder treats an unwritable
	// `.DS_Store` as normal (it is the same answer a read-only volume gives)
	// and silently keeps view state in memory instead.
	if !args.IsDir && isDesktopServicesStore(args.Name) {
		return errorResp(req.ID, cEPERM)
	}

	if s.isAggregateRoot(args.DirID) {
		// Top-level entries ARE the remotes. Creating one here would have to
		// mean adding a remote to rclone.conf, which is not this layer's job.
		return errorResp(req.ID, cEPERM)
	}

	parentNode := s.inodes.GetNode(args.DirID)
	if parentNode == nil {
		return errorResp(req.ID, cENOENT)
	}
	dir, ok := parentNode.(*vfs.Dir)
	if !ok {
		return errorResp(req.ID, cENOTDIR)
	}

	parentPath := s.inodes.GetPath(args.DirID)
	childPath := parentPath + "/" + args.Name

	// Create through the PARENT DIRECTORY NODE, not through a VFS looked up by
	// path. The previous form built childPath -- which is prefixed with the
	// remote name once more than one remote is mounted, e.g. "scratch/foo.txt"
	// -- and passed it to getFirstVFS(). That VFS is an arbitrary Go map
	// iteration order pick, and the prefixed path does not exist inside ANY
	// remote (there is no "scratch" directory *inside* the scratch remote), so
	// every create and mkdir inside a remote failed with ENOENT while reads,
	// enumeration and in-place writes -- all of which are node-based -- worked
	// fine. Worse than the ENOENT: had a directory of that name happened to
	// exist in whichever remote came out first, the file would have been
	// created in the WRONG remote.
	//
	// dir is the parent *vfs.Dir resolved from the inode table above, so it
	// already belongs to the right VFS and takes a leaf name, not a path.
	if args.IsDir {
		if _, err := dir.Mkdir(args.Name); err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
	} else {
		// Mirrors VFS.Create, which is OpenFile with these flags (vfs/vfs.go).
		// Dir.Create only returns the File NODE -- it is not added to the
		// directory until opened for write -- so the open/close is required to
		// actually materialise the file, not just bookkeeping.
		const createFlags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
		f, err := dir.Create(args.Name, createFlags)
		if err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
		fh, err := f.Open(createFlags)
		if err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
		if err := fh.Close(); err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
	}

	child, err := dir.Stat(args.Name)
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	childID := s.inodes.Assign(child, childPath)
	return okResp(req.ID, s.nodeToItemInfo(child, childID))
}

func (s *Server) doRemove(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	if s.isAggregateRoot(args.ItemID) {
		return errorResp(req.ID, cEPERM)
	}

	// Resolve through the inode table to the actual vfs.Node and remove via the
	// node itself. The previous form looked the PATH up and applied it to
	// getFirstVFS(), which is only correct when exactly one remote is mounted.
	// With several, paths are prefixed with the remote name and "first" is an
	// arbitrary Go map iteration order -- so a delete inside one remote was
	// issued against a different remote at a path that usually did not exist,
	// and occasionally did. Node-based removal always reaches the owning VFS.
	node := s.inodes.GetNode(args.ItemID)
	if node == nil {
		return errorResp(req.ID, cENOENT)
	}
	// Close our own handles on this item FIRST.
	//
	// rclone's Remove waits for a file's open handles to finish, so removing an
	// item we are still holding open waits on ourselves and never returns. That
	// is not hypothetical: recursive removal of a populated tree blocked here
	// for over ten minutes, and because a connection processes requests in
	// order, every later operation on it timed out behind this one.
	//
	// The caller is deleting the item, so any handle we hold is stale by
	// definition. Releasing it before removing is both correct and the thing
	// that stops the wait.
	for _, h := range s.handles.PopAll(args.ItemID) {
		if err := h.Close(); err != nil {
			fs.Errorf(nil, "VFS bridge: closing handle before remove of item %d: %v",
				args.ItemID, err)
		}
	}
	if err := node.Remove(); err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	s.inodes.Remove(args.ItemID)
	return okResp(req.ID, nil)
}

func (s *Server) doRename(req *Request) *Response {
	var args struct {
		SrcDirID uint64 `json:"srcDirId"`
		SrcName  string `json:"srcName"`
		DstDirID uint64 `json:"dstDirId"`
		DstName  string `json:"dstName"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	if s.isAggregateRoot(args.SrcDirID) || s.isAggregateRoot(args.DstDirID) {
		// Renaming a top-level entry would mean renaming a remote.
		return errorResp(req.ID, cEPERM)
	}

	// Same correctness problem as doRemove: the old form built paths and handed
	// them to getFirstVFS(), which silently targeted the wrong remote as soon as
	// more than one was mounted. Resolve both directories as nodes instead.
	srcNode := s.inodes.GetNode(args.SrcDirID)
	dstNode := s.inodes.GetNode(args.DstDirID)
	if srcNode == nil || dstNode == nil {
		return errorResp(req.ID, cENOENT)
	}
	srcDir, ok1 := srcNode.(*vfs.Dir)
	dstDir, ok2 := dstNode.(*vfs.Dir)
	if !ok1 || !ok2 {
		return errorResp(req.ID, cENOTDIR)
	}
	if srcDir.VFS() != dstDir.VFS() {
		// Moving between two remotes is a copy+delete, not a rename. Report it
		// honestly so the caller falls back rather than losing the file.
		return errorResp(req.ID, cEXDEV)
	}
	if err := srcDir.Rename(args.SrcName, args.DstName, dstDir); err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	return okResp(req.ID, nil)
}

func (s *Server) doReadDir(req *Request) *Response {
	var args struct {
		DirID uint64 `json:"dirId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}

	if s.isAggregateRoot(args.DirID) {
		names := s.remoteNames()
		sort.Strings(names) // map order is random; a listing that reshuffles is not a listing
		entries := make([]DirEntry, 0, len(names))
		for i, name := range names {
			s.mu.RLock()
			v := s.vfses[name]
			s.mu.RUnlock()
			root, _ := v.Root()
			childID := s.inodes.Assign(root, name)
			info := s.nodeToItemInfo(root, childID)
			info.Name = name
			entries = append(entries, DirEntry{Item: info, Cookie: uint64(i + 1)})
		}
		return okResp(req.ID, entries)
	}

	node := s.inodes.GetNode(args.DirID)
	if node == nil {
		return errorResp(req.ID, cENOENT)
	}
	dir, ok := node.(*vfs.Dir)
	if !ok {
		return errorResp(req.ID, cENOTDIR)
	}
	items, err := dir.ReadDirAll()
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}

	parentPath := s.inodes.GetPath(args.DirID)
	entries := make([]DirEntry, 0, len(items))
	children := make([]*vfs.Dir, 0, 8)
	for i, item := range items {
		childPath := parentPath + "/" + item.Name()
		childID := s.inodes.Assign(item, childPath)
		entries = append(entries, DirEntry{
			Item: s.nodeToItemInfo(item, childID), Cookie: uint64(i + 1),
		})
		if d, ok := item.(*vfs.Dir); ok {
			children = append(children, d)
		}
	}
	// Reply FIRST, warm afterwards: the caller is blocked on this response and
	// prefetching is by definition speculative.
	resp := okResp(req.ID, entries)
	s.prefetchChildren(children)
	return resp
}

// prefetchChildren warms the VFS directory cache for the subdirectories of a
// directory that was just listed.
//
// WHY. Every cold listing costs one full round trip to the backend; a repeat
// listing inside DirCacheTime costs almost nothing. Measured on a live mount:
// 211ms-1039ms cold against ~27ms warm. Navigation is depth-first — you list a
// folder, look at it, then open one of the folders in it — so the directory the
// user is about to ask for is almost always one we could have fetched while
// they were still reading the current one.
//
// This is deliberately ONE level deep. Recursing would turn opening a shallow
// folder into a tree walk of the whole remote, which is the mistake that made
// the old catalog scans hostile; and the win is nearly all in the first level,
// because by the time the user descends we get another chance to prefetch.
//
// Bounded three ways, because this spends the user's egress and API quota on
// directories they may never open:
//   - at most prefetchMaxChildren directories per listing, so a folder with 900
//     subdirectories does not fan out to 900 backend calls;
//   - at most prefetchConcurrency in flight across the whole server, so a burst
//     of listings cannot crowd out the foreground request the user is waiting
//     on — the same reasoning as the sftp channel cap on the app side;
//   - dropped entirely rather than queued when that budget is full. A prefetch
//     that runs late is worthless: either the cache is already warm or the user
//     has moved on.
//
// Errors are discarded on purpose. A failed prefetch is not a failed anything —
// the foreground read will surface the real error when it happens.
func (s *Server) prefetchChildren(children []*vfs.Dir) {
	if len(children) > prefetchMaxChildren {
		children = children[:prefetchMaxChildren]
	}
	for _, d := range children {
		if s.closed.Load() {
			return
		}
		select {
		case s.prefetchSlots <- struct{}{}:
		default:
			return // budget full — drop, do not queue
		}
		go func(dir *vfs.Dir) {
			defer func() { <-s.prefetchSlots }()
			// ReadDirAll populates the same cache the foreground path reads,
			// so a subsequent real listing is a cache hit rather than a
			// second backend call.
			_, _ = dir.ReadDirAll()
		}(d)
	}
}

func (s *Server) doOpen(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
		Write  bool   `json:"write"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}

	if s.isAggregateRoot(args.ItemID) {
		// Directories open trivially, same as the branch below.
		return okResp(req.ID, nil)
	}

	node := s.inodes.GetNode(args.ItemID)
	if node == nil {
		return errorResp(req.ID, cENOENT)
	}
	if node.IsDir() {
		return okResp(req.ID, nil)
	}

	flags := os.O_RDONLY
	if args.Write {
		flags = os.O_RDWR
	}

	handle, err := node.Open(flags)
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	s.handles.Put(args.ItemID, handle, args.Write)
	return okResp(req.ID, nil)
}

func (s *Server) doClose(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	handle, wasWrite := s.handles.PopLast(args.ItemID)
	if handle == nil {
		return okResp(req.ID, nil)
	}
	// A close error is the filesystem's last chance to say the data did not
	// make it. With CacheModeFull this is where the upload is queued, so
	// swallowing it at debug level reported a durable write that never
	// happened. Surface it.
	if err := handle.Close(); err != nil {
		fs.Errorf(nil, "VFS bridge: close failed for item %d: %v", args.ItemID, err)
		return errorResp(req.ID, mapVFSErr(err))
	}
	// Closing a write handle queues the upload but does not wait for it.
	// Drain the queue here so close(2) is actually durable. Short bound —
	// the module's request timeout is 10s and the caller may also call sync.
	if wasWrite {
		s.WaitForWriters(closeFlushTimeout)
	}
	return okResp(req.ID, nil)
}

// How long a write-close waits for CacheModeFull writeback. Kept shorter than
// the module's per-request timeout so a stuck upload becomes a close error
// rather than an unattributed bridge ETIMEDOUT.
const closeFlushTimeout = 8 * time.Second

func (s *Server) doRead(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
		Offset int64  `json:"offset"`
		Length int64  `json:"length"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	handle := s.handles.GetForRead(args.ItemID)
	if handle == nil {
		// A read with no registered open handle is LEGAL, and answering ENOENT
		// for it corrupts files.
		//
		// The kernel's paging path issues reads that do not correspond to an
		// FSKit open. The ordinary mmap lifecycle is open -> mmap -> close, and
		// the mapping stays valid afterwards: every page fault from then on
		// arrives here with the handle already popped by that close. Observed
		// directly in a QuickLook trace -- `open, close, read` -- alongside
		// hundreds of `-[FSVolumeConnector readFrom:...] reply:error:2` from
		// FSKit, error 2 being the ENOENT this used to return.
		//
		// A reader that gets ENOENT mid-file keeps whatever it already decoded
		// and renders the rest as garbage, which is why images came out with
		// their first rows intact and the remainder corrupt while `cat` -- which
		// holds its descriptor open -- read the identical file perfectly.
		//
		// So open one lazily and keep it in the table: reclaim already closes
		// everything an item still holds, so its lifetime is bounded exactly as
		// an explicitly-opened handle's is.
		node := s.inodes.GetNode(args.ItemID)
		if node == nil {
			return errorResp(req.ID, cENOENT)
		}
		file, ok := node.(*vfs.File)
		if !ok {
			return errorResp(req.ID, cEISDIR)
		}
		h, err := file.Open(os.O_RDONLY)
		if err != nil {
			fs.Errorf(nil, "VFS bridge: lazy read-open of item %d failed: %v", args.ItemID, err)
			return errorResp(req.ID, mapVFSErr(err))
		}
		s.handles.Put(args.ItemID, h, false)
		handle = h
	}
	buf := make([]byte, args.Length)
	n, err := handle.ReadAt(buf, args.Offset)
	if err != nil && err != io.EOF {
		return errorResp(req.ID, mapVFSErr(err))
	}
	return okResp(req.ID, buf[:n])
}

func (s *Server) doWrite(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
		Offset int64  `json:"offset"`
		Data   []byte `json:"data"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	// Must be a WRITABLE handle. When an item is open for both reading and
	// writing, taking whichever handle was stored last could send the write to
	// the read handle and fail — or worse, appear to succeed against the wrong
	// one. See `HandleTable`.
	handle := s.handles.GetForWrite(args.ItemID)
	if handle == nil {
		return errorResp(req.ID, cENOENT)
	}
	n, err := handle.WriteAt(args.Data, args.Offset)
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	return okResp(req.ID, n)
}

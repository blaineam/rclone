// Package vfsbridge provides a TCP server that bridges JSON requests
// to rclone's VFS layer. This allows FSKit extensions to access
// rclone remotes as POSIX filesystems via a localhost TCP connection.
package vfsbridge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
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
}

// NewServer creates a new VFS bridge server.
func NewServer() *Server {
	return &Server{
		vfses:   make(map[string]*vfs.VFS),
		inodes:  NewInodeTable(),
		handles: NewHandleTable(),
	}
}

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
	opt.DirCacheTime = fs.Duration(5 * time.Minute)
	opt.PollInterval = fs.Duration(1 * time.Minute)
	opt.ReadAhead = 128 * fs.SizeSuffix(1024)

	v := vfs.New(context.Background(), f, &opt)
	s.vfses[remoteName] = v

	fs.Infof(nil, "VFS bridge: added remote %q", remoteName)
	return nil
}

// Start begins listening on the given address. Pass "localhost:0" for random port.
func (s *Server) Start(addr string) (string, error) {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("failed to listen: %w", err)
	}

	listenAddr := s.listener.Addr().String()
	fs.Infof(nil, "VFS bridge: listening on %s", listenAddr)

	go s.acceptLoop()
	return listenAddr, nil
}

// ListenAddr returns the address the server is listening on.
func (s *Server) ListenAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop shuts down the server and all VFS instances.
func (s *Server) Stop() {
	if s.closed.Swap(true) {
		return
	}
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, v := range s.vfses {
		v.Shutdown()
		fs.Infof(nil, "VFS bridge: shutdown VFS for %q", name)
	}
	s.vfses = make(map[string]*vfs.VFS)
}

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
	cENOTDIR   int32 = 20
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
	return cEIO
}

func (s *Server) getFirstVFS() *vfs.VFS {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.vfses {
		return v
	}
	return nil
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

// handleRequest dispatches a JSON request to the appropriate VFS operation.
func (s *Server) handleRequest(req *Request) *Response {
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
	case "mount", "unmount", "sync", "deactivate":
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
	default:
		return errorResp(req.ID, cENOSYS)
	}
}

func (s *Server) doStatFS(req *Request) *Response {
	v := s.getFirstVFS()
	if v == nil {
		return okResp(req.ID, StatFSInfo{})
	}
	total, used, free := v.Statfs()
	return okResp(req.ID, StatFSInfo{
		TotalBytes: uint64(total), FreeBytes: uint64(free), UsedBytes: uint64(used),
	})
}

func (s *Server) doActivate(req *Request) *Response {
	rootInfo := s.inodes.GetOrCreateRootInfo()
	if len(s.vfses) == 1 {
		if v := s.getFirstVFS(); v != nil {
			r, _ := v.Root()
			s.inodes.AssignRoot(r)
		}
	}
	return okResp(req.ID, rootInfo)
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
		UID: 501, GID: 20,
	}
}

func (s *Server) doGetAttr(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
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

	if args.DirID == RootInodeID && len(s.vfses) > 1 {
		s.mu.RLock()
		v, ok := s.vfses[args.Name]
		s.mu.RUnlock()
		if !ok {
			return errorResp(req.ID, cENOENT)
		}
		root, _ := v.Root()
		childID := s.inodes.Assign(root, args.Name)
		return okResp(req.ID, s.nodeToItemInfo(root, childID))
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
	s.inodes.Remove(args.ItemID)
	return okResp(req.ID, nil)
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

	v := s.getFirstVFS()
	if v == nil {
		return errorResp(req.ID, cEIO)
	}

	if args.IsDir {
		if err := v.Mkdir(childPath, 0755); err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
	} else {
		fh, err := v.Create(childPath)
		if err != nil {
			return errorResp(req.ID, mapVFSErr(err))
		}
		_ = fh.Close()
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
	path := s.inodes.GetPath(args.ItemID)
	if path == "" {
		return errorResp(req.ID, cENOENT)
	}
	v := s.getFirstVFS()
	if v == nil {
		return errorResp(req.ID, cEIO)
	}
	if err := v.Remove(path); err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
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
	oldPath := s.inodes.GetPath(args.SrcDirID) + "/" + args.SrcName
	newPath := s.inodes.GetPath(args.DstDirID) + "/" + args.DstName
	v := s.getFirstVFS()
	if v == nil {
		return errorResp(req.ID, cEIO)
	}
	if err := v.Rename(oldPath, newPath); err != nil {
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

	if args.DirID == RootInodeID && len(s.vfses) > 1 {
		names := s.remoteNames()
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
	for i, item := range items {
		childPath := parentPath + "/" + item.Name()
		childID := s.inodes.Assign(item, childPath)
		entries = append(entries, DirEntry{
			Item: s.nodeToItemInfo(item, childID), Cookie: uint64(i + 1),
		})
	}
	return okResp(req.ID, entries)
}

func (s *Server) doOpen(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
		Write  bool   `json:"write"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
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
	s.handles.Put(args.ItemID, handle)
	return okResp(req.ID, nil)
}

func (s *Server) doClose(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	handle := s.handles.Get(args.ItemID)
	if handle == nil {
		return okResp(req.ID, nil)
	}
	if err := handle.Close(); err != nil {
		fs.Debugf(nil, "VFS bridge: close error for item %d: %v", args.ItemID, err)
	}
	s.handles.Remove(args.ItemID)
	return okResp(req.ID, nil)
}

func (s *Server) doRead(req *Request) *Response {
	var args struct {
		ItemID uint64 `json:"itemId"`
		Offset int64  `json:"offset"`
		Length int64  `json:"length"`
	}
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return errorResp(req.ID, cEINVAL)
	}
	handle := s.handles.Get(args.ItemID)
	if handle == nil {
		return errorResp(req.ID, cENOENT)
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
	handle := s.handles.Get(args.ItemID)
	if handle == nil {
		return errorResp(req.ID, cENOENT)
	}
	n, err := handle.WriteAt(args.Data, args.Offset)
	if err != nil {
		return errorResp(req.ID, mapVFSErr(err))
	}
	return okResp(req.ID, n)
}

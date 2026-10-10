//go:build unix

package nfs

import (
	"context"
	"fmt"
	"net"
	"sync"

	nfs "github.com/willscott/go-nfs"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// Server contains everything to run the Server
type Server struct {
	opt                 Options
	handler             nfs.Handler
	ctx                 context.Context // for global config
	listener            net.Listener
	UnmountedExternally bool
	// pmapPort and pmapPrograms record what NewServer registered with
	// rpcbind, so Shutdown can remove exactly that and nothing else.
	pmapPort     int
	pmapPrograms [][2]uint32
	shutdownOnce sync.Once
	shutdownErr  error
}

// NewServer creates a new server
func NewServer(ctx context.Context, vfs *vfs.VFS, opt *Options) (s *Server, err error) {
	if vfs.Opt.CacheMode == vfscommon.CacheModeOff {
		fs.LogPrintf(fs.LogLevelWarning, ctx, "NFS writes don't work without a cache, the filesystem will be served read-only")
	}
	// Our NFS server doesn't have any authentication, we run it on localhost and random port by default
	if opt.ListenAddr == "" {
		opt.ListenAddr = "localhost:"
	}

	s = &Server{
		ctx: ctx,
		opt: *opt,
	}
	s.handler, err = NewHandler(ctx, vfs, opt)
	if err != nil {
		return nil, fmt.Errorf("failed to make NFS handler: %w", err)
	}
	s.listener, err = net.Listen("tcp", s.opt.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to open listening socket: %w", err)
	}
	// Register with local rpcbind so macOS NFS clients can discover us via portmapper.
	// Both NFS (100003) and Mount (100005) programs are served on the same port by go-nfs.
	if tcpAddr, ok := s.listener.Addr().(*net.TCPAddr); ok {
		s.pmapPort = tcpAddr.Port
		s.pmapPrograms = tryRegisterPortmapper(tcpAddr.Port)
	}
	return s, nil
}

// Addr returns the listening address of the server
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// Shutdown stops the server and removes its rpcbind registrations. Safe to
// call more than once.
func (s *Server) Shutdown() error {
	s.shutdownOnce.Do(func() {
		s.shutdownErr = s.listener.Close()
		tryUnregisterPortmapper(s.pmapPort, s.pmapPrograms)
	})
	return s.shutdownErr
}

// Serve starts the server
func (s *Server) Serve() (err error) {
	fs.Logf(nil, "NFS Server running at %s\n", s.listener.Addr())
	return nfs.Serve(s.listener, s.handler)
}

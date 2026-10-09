//go:build unix

package nfs

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain keeps every test in this package away from the machine's real
// rpcbind: NewServer registers with it, and a test must never leave
// registrations pointing at its throwaway ports in the system portmapper.
// Port 1 on loopback has nothing listening, so registration is skipped.
func TestMain(m *testing.M) {
	pmapAddr = "127.0.0.1:1"
	os.Exit(m.Run())
}

// fakeRpcbind accepts one connection and answers each PMAP_SET on it with
// the next result in results (1 = registered, 0 = refused), recording the
// (program, version, port) of each call.
func fakeRpcbind(t *testing.T, results ...uint32) (addr string, calls func() [][3]uint32) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var got [][3]uint32
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for _, res := range results {
			hdr := make([]byte, 4)
			if _, err := io.ReadFull(conn, hdr); err != nil {
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(hdr)&^0x80000000)
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
			w := func(i int) uint32 { return binary.BigEndian.Uint32(body[i*4:]) }
			mu.Lock()
			got = append(got, [3]uint32{w(10), w(11), w(13)})
			mu.Unlock()
			_, _ = conn.Write(rpcReply(w(0), 1, 0, 0, 0, 0, res))
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		<-done
	})
	return l.Addr().String(), func() [][3]uint32 {
		<-done
		mu.Lock()
		defer mu.Unlock()
		return got
	}
}

func usePmapAddr(t *testing.T, addr string) {
	t.Helper()
	old := pmapAddr
	pmapAddr = addr
	t.Cleanup(func() { pmapAddr = old })
}

func TestTryRegisterPortmapperRegistersBothPrograms(t *testing.T) {
	addr, calls := fakeRpcbind(t, 1, 1)
	usePmapAddr(t, addr)
	tryRegisterPortmapper(2049)
	assert.Equal(t, [][3]uint32{{100003, 3, 2049}, {100005, 3, 2049}}, calls())
}

// A refusal for one program is logged and the other is still registered.
func TestTryRegisterPortmapperContinuesAfterRefusal(t *testing.T) {
	addr, calls := fakeRpcbind(t, 0, 1)
	usePmapAddr(t, addr)
	tryRegisterPortmapper(4242)
	assert.Len(t, calls(), 2)
}

// No rpcbind: registration is skipped silently.
func TestTryRegisterPortmapperUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	usePmapAddr(t, addr)
	tryRegisterPortmapper(2049) // must return promptly without panicking
}

func newTestVFS(t *testing.T, mode vfscommon.CacheMode) *vfs.VFS {
	t.Helper()
	f, err := fs.NewFs(context.Background(), t.TempDir())
	require.NoError(t, err)
	opt := vfscommon.Opt
	opt.CacheMode = mode
	v := vfs.New(context.Background(), f, &opt)
	t.Cleanup(v.Shutdown)
	return v
}

// NewServer registers the port it actually bound with rpcbind.
func TestNewServerRegistersBoundPort(t *testing.T) {
	addr, calls := fakeRpcbind(t, 1, 1)
	usePmapAddr(t, addr)
	opt := Opt
	opt.ListenAddr = "127.0.0.1:0"
	s, err := NewServer(context.Background(), newTestVFS(t, vfscommon.CacheModeOff), &opt)
	require.NoError(t, err)
	defer func() { _ = s.Shutdown() }()
	port := uint32(s.listener.Addr().(*net.TCPAddr).Port)
	assert.Equal(t, [][3]uint32{{100003, 3, port}, {100005, 3, port}}, calls())
}

func TestNewServerErrors(t *testing.T) {
	v := newTestVFS(t, vfscommon.CacheModeOff)

	opt := Opt
	opt.ListenAddr = "256.0.0.1:0"
	_, err := NewServer(context.Background(), v, &opt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listening socket")

	opt = Opt
	opt.ListenAddr = "127.0.0.1:0"
	opt.HandleCache = handleCache(99)
	_, err = NewServer(context.Background(), v, &opt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NFS handler")
}

// An empty listen address defaults to a random loopback port.
func TestNewServerDefaultAddress(t *testing.T) {
	opt := Opt
	opt.ListenAddr = ""
	opt.HandleCacheDir = filepath.Join(t.TempDir(), "handles")
	s, err := NewServer(context.Background(), newTestVFS(t, vfscommon.CacheModeFull), &opt)
	require.NoError(t, err)
	defer func() { _ = s.Shutdown() }()
	host, _, err := net.SplitHostPort(s.listener.Addr().String())
	require.NoError(t, err)
	ip := net.ParseIP(host)
	require.NotNil(t, ip)
	assert.True(t, ip.IsLoopback(), "default listen address %s is not loopback", host)
}

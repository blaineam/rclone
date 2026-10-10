//go:build unix

package nfs

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pmapRecord is one call seen by the fake rpcbind: procedure, program,
// version, port.
type pmapRecord [4]uint32

// fakeRpcbindServer is a stand-in rpcbind on an ephemeral loopback port. It
// accepts any number of connections (registration and unregistration each
// dial their own), answers every call with refuse(call) ? false : true and
// records it. Never the machine's real portmapper.
type fakeRpcbindServer struct {
	l      net.Listener
	refuse func(pmapRecord) bool
	mu     sync.Mutex
	calls  []pmapRecord
	wg     sync.WaitGroup
}

func startFakeRpcbindServer(t *testing.T, refuse func(pmapRecord) bool) *fakeRpcbindServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &fakeRpcbindServer{l: l, refuse: refuse}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go f.serve(conn)
		}
	}()
	usePmapAddr(t, l.Addr().String())
	t.Cleanup(func() {
		_ = l.Close()
		f.wg.Wait()
	})
	return f
}

func (f *fakeRpcbindServer) serve(conn net.Conn) {
	defer f.wg.Done()
	defer func() { _ = conn.Close() }()
	for {
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr)&^0x80000000)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		w := func(i int) uint32 { return binary.BigEndian.Uint32(body[i*4:]) }
		rec := pmapRecord{w(5), w(10), w(11), w(13)}
		res := uint32(1)
		if f.refuse != nil && f.refuse(rec) {
			res = 0
		}
		f.mu.Lock()
		f.calls = append(f.calls, rec)
		f.mu.Unlock()
		if _, err := conn.Write(rpcReply(w(0), 1, 0, 0, 0, 0, res)); err != nil {
			return
		}
	}
}

func (f *fakeRpcbindServer) seen() []pmapRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pmapRecord(nil), f.calls...)
}

// Shutdown removes exactly the registrations NewServer made, once.
func TestShutdownUnregistersFromRpcbind(t *testing.T) {
	f := startFakeRpcbindServer(t, nil)
	opt := Opt
	opt.ListenAddr = "127.0.0.1:0"
	s, err := NewServer(context.Background(), newTestVFS(t, vfscommon.CacheModeOff), &opt)
	require.NoError(t, err)
	port := uint32(s.listener.Addr().(*net.TCPAddr).Port)

	require.NoError(t, s.Shutdown())
	// A second Shutdown (nfsmount's unmount path and an atexit handler can
	// both run) must neither error nor unregister again.
	require.NoError(t, s.Shutdown())

	assert.Equal(t, []pmapRecord{
		{pmapProcSet, 100003, 3, port},
		{pmapProcSet, 100005, 3, port},
		{pmapProcUnset, 100003, 3, port},
		{pmapProcUnset, 100005, 3, port},
	}, f.seen())
}

// A program rpcbind refused belongs to someone else (PMAP_UNSET matches on
// program+version only), so it must not be unset at shutdown.
func TestShutdownLeavesRefusedProgramAlone(t *testing.T) {
	f := startFakeRpcbindServer(t, func(r pmapRecord) bool {
		return r[0] == pmapProcSet && r[1] == 100003
	})
	opt := Opt
	opt.ListenAddr = "127.0.0.1:0"
	s, err := NewServer(context.Background(), newTestVFS(t, vfscommon.CacheModeOff), &opt)
	require.NoError(t, err)
	port := uint32(s.listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, s.Shutdown())

	assert.Equal(t, []pmapRecord{
		{pmapProcSet, 100003, 3, port},
		{pmapProcSet, 100005, 3, port},
		{pmapProcUnset, 100005, 3, port},
	}, f.seen())
}

// An unset rpcbind refuses is logged, not fatal; and with nothing registered
// Shutdown does not dial rpcbind at all.
func TestUnregisterPortmapperEdgeCases(t *testing.T) {
	f := startFakeRpcbindServer(t, func(r pmapRecord) bool { return r[0] == pmapProcUnset })
	tryUnregisterPortmapper(2049, [][2]uint32{{100003, 3}})
	assert.Equal(t, []pmapRecord{{pmapProcUnset, 100003, 3, 2049}}, f.seen())

	tryUnregisterPortmapper(2049, nil)
	assert.Len(t, f.seen(), 1, "nothing registered: rpcbind must not be contacted")

	// rpcbind gone by shutdown time: returns promptly.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	usePmapAddr(t, addr)
	tryUnregisterPortmapper(2049, [][2]uint32{{100003, 3}})
}

// With no rpcbind at all nothing is registered and Shutdown still works.
func TestShutdownWithoutRpcbind(t *testing.T) {
	opt := Opt
	opt.ListenAddr = "127.0.0.1:0"
	s, err := NewServer(context.Background(), newTestVFS(t, vfscommon.CacheModeOff), &opt)
	require.NoError(t, err)
	assert.Empty(t, s.pmapPrograms)
	require.NoError(t, s.Shutdown())
}

func TestSetPortmapperAddrForTest(t *testing.T) {
	before := pmapAddr
	restore := SetPortmapperAddrForTest("127.0.0.1:2")
	assert.Equal(t, "127.0.0.1:2", pmapAddr)
	restore()
	assert.Equal(t, before, pmapAddr)
}

//go:build unix

package nfs

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/rclone/rclone/fs"
)

// pmapAddr is where rpcbind listens. A variable only so tests can point the
// registration at a fake portmapper instead of the machine's real one.
var pmapAddr = "127.0.0.1:111"

const (
	pmapProgram   = 100000
	pmapVersion   = 2
	pmapProcSet   = 1
	pmapProcUnset = 2
	ipprotoTCP    = 6
)

// tryRegisterPortmapper registers the NFS (100003) and Mount (100005) RPC programs
// with the local rpcbind/portmapper on port 111. This lets macOS NFS clients
// discover the server port via standard portmapper queries.
//
// Logs a warning and returns silently if rpcbind is not running — the server
// still works for clients that connect using the explicit port in the nfs:// URL.
//
// Returns the (program, version) pairs rpcbind accepted, which are exactly the
// ones tryUnregisterPortmapper must remove again at shutdown. A program rpcbind
// refused (typically because another server already holds it) is NOT returned:
// PMAP_UNSET matches on program and version only, so unsetting it would delete
// the other server's registration.
func tryRegisterPortmapper(port int) (registered [][2]uint32) {
	conn, err := net.DialTimeout("tcp", pmapAddr, 2*time.Second)
	if err != nil {
		fs.Logf(nil, "rpcbind not reachable, skipping portmapper registration: %v", err)
		return nil
	}
	defer conn.Close()

	for _, prog := range [][2]uint32{
		{100003, 3}, // NFS program
		{100005, 3}, // Mount program
	} {
		if err := pmapCall(conn, pmapProcSet, prog[0], prog[1], uint32(port)); err != nil {
			fs.Logf(nil, "portmapper: failed to register program %d: %v", prog[0], err)
		} else {
			fs.Logf(nil, "portmapper: registered program %d v%d TCP port %d", prog[0], prog[1], port)
			registered = append(registered, prog)
		}
	}
	return registered
}

// tryUnregisterPortmapper removes registrations made by tryRegisterPortmapper.
//
// Without it every server that ever ran left rpcbind advertising its program
// on a port that is now closed (or, worse, reused by something else), so the
// next NFS client to ask the portmapper was sent to a dead port until rpcbind
// restarted. Best effort, like registration: failures are logged, never fatal.
func tryUnregisterPortmapper(port int, programs [][2]uint32) {
	if len(programs) == 0 {
		return
	}
	conn, err := net.DialTimeout("tcp", pmapAddr, 2*time.Second)
	if err != nil {
		fs.Logf(nil, "rpcbind not reachable, skipping portmapper unregistration: %v", err)
		return
	}
	defer conn.Close()

	for _, prog := range programs {
		if err := pmapCall(conn, pmapProcUnset, prog[0], prog[1], uint32(port)); err != nil {
			fs.Logf(nil, "portmapper: failed to unregister program %d: %v", prog[0], err)
		} else {
			fs.Logf(nil, "portmapper: unregistered program %d v%d", prog[0], prog[1])
		}
	}
}

// pmapSetCall sends a single PMAP_SET (procedure 1) RPC call on conn and reads the reply.
func pmapSetCall(conn net.Conn, prognum, versnum, port uint32) error {
	return pmapCall(conn, pmapProcSet, prognum, versnum, port)
}

// pmapCall sends a single PMAP_SET or PMAP_UNSET RPC call on conn and reads
// the boolean reply. Both take the same mapping argument (PMAP_UNSET ignores
// the protocol and port). The RPC wire format is Sun RPC (RFC 5531) with TCP
// record marking (RFC 5531 §11).
func pmapCall(conn net.Conn, proc, prognum, versnum, port uint32) error {
	xid := rand.Uint32()

	// RPC CALL body: 14 uint32s = 56 bytes
	// [XID, CALL=0, RPCvers=2, prog, vers, proc, credFlavor=0, credLen=0,
	//  verfFlavor=0, verfLen=0, prognum, versnum, protocol, port]
	body := make([]byte, 56)
	for i, v := range []uint32{
		xid, 0, 2, pmapProgram, pmapVersion, proc, // header
		0, 0, 0, 0, // AUTH_NULL cred + verf
		prognum, versnum, ipprotoTCP, port, // PMAP_SET args
	} {
		binary.BigEndian.PutUint32(body[i*4:], v)
	}

	// TCP record mark: length | 0x80000000 (last fragment bit)
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body))|0x80000000)
	copy(frame[4:], body)

	conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		return fmt.Errorf("send: %w", err)
	}

	// Read reply record mark (4 bytes)
	hdr := make([]byte, 4)
	if err := pmapReadAll(conn, hdr); err != nil {
		return fmt.Errorf("recv header: %w", err)
	}
	fragLen := binary.BigEndian.Uint32(hdr) &^ 0x80000000
	if fragLen > 256 {
		return fmt.Errorf("reply unexpectedly large: %d bytes", fragLen)
	}

	// Read reply body
	// Expected: [XID, REPLY=1, MSG_ACCEPTED=0, verfFlavor=0, verfLen=0, accept_stat=0, result=1]
	reply := make([]byte, fragLen)
	if err := pmapReadAll(conn, reply); err != nil {
		return fmt.Errorf("recv reply: %w", err)
	}
	if len(reply) < 28 {
		return fmt.Errorf("reply too short: %d bytes", len(reply))
	}
	if binary.BigEndian.Uint32(reply[0:]) != xid {
		return fmt.Errorf("XID mismatch in reply")
	}
	if binary.BigEndian.Uint32(reply[4:]) != 1 {
		return fmt.Errorf("not an RPC reply (msg_type=%d)", binary.BigEndian.Uint32(reply[4:]))
	}
	if binary.BigEndian.Uint32(reply[8:]) != 0 {
		return fmt.Errorf("message rejected (reply_stat=%d)", binary.BigEndian.Uint32(reply[8:]))
	}
	if binary.BigEndian.Uint32(reply[20:]) != 0 {
		return fmt.Errorf("call not accepted (accept_stat=%d)", binary.BigEndian.Uint32(reply[20:]))
	}
	if binary.BigEndian.Uint32(reply[24:]) != 1 {
		if proc == pmapProcUnset {
			return fmt.Errorf("portmapper returned false — not registered")
		}
		return fmt.Errorf("portmapper returned false — already registered?")
	}
	return nil
}

func pmapReadAll(conn net.Conn, buf []byte) error {
	for off := 0; off < len(buf); {
		n, err := conn.Read(buf[off:])
		off += n
		if err != nil && off < len(buf) {
			return err
		}
	}
	return nil
}

// SetPortmapperAddrForTest points rpcbind registration at addr and returns a
// function restoring the previous address. For tests of packages built on
// this server (nfsmount), which must never register throwaway ports with the
// machine's real rpcbind. Not for production use.
func SetPortmapperAddrForTest(addr string) (restore func()) {
	old := pmapAddr
	pmapAddr = addr
	return func() { pmapAddr = old }
}

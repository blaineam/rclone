//go:build unix

package nfs

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePortmapper reads one PMAP_SET call from conn, hands the decoded call
// words to check, and answers with the reply built by reply(xid).
func fakePortmapper(t *testing.T, conn net.Conn, check func(words []uint32), reply func(xid uint32) []byte) {
	t.Helper()
	go func() {
		defer func() { _ = conn.Close() }()
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		mark := binary.BigEndian.Uint32(hdr)
		body := make([]byte, mark&^0x80000000)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		words := make([]uint32, len(body)/4)
		for i := range words {
			words[i] = binary.BigEndian.Uint32(body[i*4:])
		}
		if mark&0x80000000 == 0 {
			words = nil // not a last fragment: make check fail loudly
		}
		check(words)
		_, _ = conn.Write(reply(words[0]))
	}()
}

func rpcReply(words ...uint32) []byte {
	out := make([]byte, 4+4*len(words))
	binary.BigEndian.PutUint32(out, uint32(4*len(words))|0x80000000)
	for i, w := range words {
		binary.BigEndian.PutUint32(out[4+4*i:], w)
	}
	return out
}

func TestPmapSetCallWireFormatAndSuccess(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	var got []uint32
	fakePortmapper(t, server, func(w []uint32) { got = w }, func(xid uint32) []byte {
		return rpcReply(xid, 1, 0, 0, 0, 0, 1)
	})

	require.NoError(t, pmapSetCall(client, 100003, 3, 2049))
	require.Len(t, got, 14)
	assert.Equal(t, []uint32{0, 2, pmapProgram, pmapVersion, pmapProcSet}, got[1:6], "CALL, RPC v2, PMAP v2 SET")
	assert.Equal(t, []uint32{0, 0, 0, 0}, got[6:10], "AUTH_NULL cred and verifier")
	assert.Equal(t, []uint32{100003, 3, ipprotoTCP, 2049}, got[10:14])
}

func TestPmapSetCallRejectsBadReplies(t *testing.T) {
	for name, tc := range map[string]struct {
		reply   func(xid uint32) []byte
		wantErr string
	}{
		"xid mismatch":   {func(x uint32) []byte { return rpcReply(x+1, 1, 0, 0, 0, 0, 1) }, "XID mismatch"},
		"not a reply":    {func(x uint32) []byte { return rpcReply(x, 0, 0, 0, 0, 0, 1) }, "not an RPC reply"},
		"denied":         {func(x uint32) []byte { return rpcReply(x, 1, 1, 0, 0, 0, 1) }, "message rejected"},
		"not accepted":   {func(x uint32) []byte { return rpcReply(x, 1, 0, 0, 0, 1, 1) }, "call not accepted"},
		"returned false": {func(x uint32) []byte { return rpcReply(x, 1, 0, 0, 0, 0, 0) }, "already registered"},
		"too short":      {func(x uint32) []byte { return rpcReply(x, 1, 0) }, "too short"},
		"too large": {func(x uint32) []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, 4096|0x80000000)
			return b
		}, "unexpectedly large"},
		"connection closed": {func(x uint32) []byte { return nil }, "recv header"},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			fakePortmapper(t, server, func([]uint32) {}, tc.reply)
			err := pmapSetCall(client, 100005, 3, 2049)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

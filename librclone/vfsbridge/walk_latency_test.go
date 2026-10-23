package vfsbridge

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// makeTree builds 81 dirs / 1075 files: root + 8 dirs each with 9 subdirs (1+8+72=81),
// files spread ~13 per leaf dir.
func makeTree(t *testing.T, root string) (dirs, files int) {
	t.Helper()
	write := func(p string) {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		files++
	}
	dirs = 1
	for i := 0; i < 8; i++ {
		d := filepath.Join(root, fmt.Sprintf("d%02d", i))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs++
		for k := 0; k < 3; k++ {
			write(filepath.Join(d, fmt.Sprintf("f%02d.txt", k)))
		}
		for j := 0; j < 9; j++ {
			sd := filepath.Join(d, fmt.Sprintf("s%02d", j))
			if err := os.MkdirAll(sd, 0o755); err != nil {
				t.Fatal(err)
			}
			dirs++
			n := 14
			if (i*9+j)%2 == 0 {
				n = 15
			}
			for k := 0; k < n; k++ {
				write(filepath.Join(sd, fmt.Sprintf("file%03d.bin", k)))
			}
		}
	}
	for files < 1075 {
		write(filepath.Join(root, fmt.Sprintf("top%03d.txt", files)))
	}
	return
}

type tcpClient struct {
	c         net.Conn
	nextID    uint64
	twoWrites bool
	rtts      []time.Duration
}

func dialClient(t *testing.T, addr string, nodelay, twoWrites bool) *tcpClient {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.(*net.TCPConn).SetNoDelay(nodelay)
	return &tcpClient{c: c, nextID: 1, twoWrites: twoWrites}
}

func (c *tcpClient) call(t *testing.T, op string, args any) map[string]any {
	t.Helper()
	id := c.nextID
	c.nextID++
	raw, _ := json.Marshal(args)
	body, _ := json.Marshal(map[string]any{"id": id, "op": op, "args": json.RawMessage(raw)})
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	t0 := time.Now()
	if c.twoWrites {
		c.c.Write(lenBuf[:])
		c.c.Write(body)
	} else {
		c.c.Write(append(lenBuf[:], body...))
	}
	if _, err := io.ReadFull(c.c, lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	resp := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(c.c, resp); err != nil {
		t.Fatal(err)
	}
	c.rtts = append(c.rtts, time.Since(t0))
	var m map[string]any
	json.Unmarshal(resp, &m)
	if m["ok"] != true {
		t.Fatalf("%s failed: %v", op, m)
	}
	return m
}

// walk mimics find: readdir, then for each child lookup + getattr; recurse into dirs.
func (c *tcpClient) walk(t *testing.T, dirID uint64) (files int) {
	r := c.call(t, "readdir", map[string]any{"dirId": dirID})
	for _, e := range r["result"].([]any) {
		item := e.(map[string]any)["item"].(map[string]any)
		name := item["name"].(string)
		l := c.call(t, "lookup", map[string]any{"dirId": dirID, "name": name})
		id := uint64(l["result"].(map[string]any)["id"].(float64))
		c.call(t, "getattr", map[string]any{"itemId": id})
		if item["isDir"].(bool) {
			files += c.walk(t, id)
		} else {
			files++
		}
	}
	return
}

// TestWalkLatencyOverTCP walks an 81-directory, 1075-file tree through the
// bridge over a real loopback socket, the way the FSKit module does (one
// request in flight per connection, length-prefixed JSON), and reports the
// cost per operation. It exists because a warm walk of such a tree on the
// mounted volume cost ~25ms per operation (2026-10-22) and the first question
// was whether the bridge was paying it. It is not: measured here at ~25µs
// per op over TCP and ~1.5µs in the handlers, with Nagle on or off. The
// assertion is loose on purpose; it guards against the bridge regressing into
// the millisecond range, not against machine noise.
func TestWalkLatencyOverTCP(t *testing.T) {
	s, backingA, _ := newTestServer(t)
	dirs, files := makeTree(t, backingA)
	t.Logf("tree: %d dirs, %d files", dirs, files)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rootA := remoteRootID(t, s, "rem_a")

	// In-process handler cost on a warm tree.
	c0 := dialClient(t, addr, true, false)
	c0.walk(t, rootA) // warm the VFS dir cache
	t.Logf("cold walk over tcp (nodelay, 1 write): %d ops, total %v", len(c0.rtts), sum(c0.rtts))

	for _, cfg := range []struct {
		name           string
		nodelay, twoWr bool
	}{
		{"nagle-on, two writes (Swift today)", false, true},
		{"nagle-on, one write", false, false},
		{"nodelay, two writes", true, true},
		{"nodelay, one write", true, false},
	} {
		c := dialClient(t, addr, cfg.nodelay, cfg.twoWr)
		t0 := time.Now()
		n := c.walk(t, rootA)
		t.Logf("%-38s files=%d ops=%d total=%v mean/op=%v", cfg.name, n, len(c.rtts), time.Since(t0), sum(c.rtts)/time.Duration(len(c.rtts)))
		c.c.Close()
	}

	// Pure handler time, no socket.
	t0 := time.Now()
	nops := 0
	var walkDirect func(dirID uint64)
	walkDirect = func(dirID uint64) {
		r := s.handleRequest(req(t, 1, "readdir", map[string]any{"dirId": dirID}))
		nops++
		for _, e := range r.Result.([]DirEntry) {
			l := s.handleRequest(req(t, 1, "lookup", map[string]any{"dirId": dirID, "name": e.Item.Name}))
			s.handleRequest(req(t, 1, "getattr", map[string]any{"itemId": l.Result.(ItemInfo).ID}))
			nops += 2
			if e.Item.IsDir {
				walkDirect(l.Result.(ItemInfo).ID)
			}
		}
	}
	walkDirect(rootA)
	t.Logf("in-process handlers only: ops=%d total=%v mean/op=%v", nops, time.Since(t0), time.Since(t0)/time.Duration(nops))
	if mean := sum(c0.rtts) / time.Duration(len(c0.rtts)); mean > 2*time.Millisecond {
		t.Fatalf("bridge round trip averages %v per op over loopback; the FSKit module cannot be fast on top of that", mean)
	}
}

func sum(ds []time.Duration) (s time.Duration) {
	for _, d := range ds {
		s += d
	}
	return
}

//go:build !plan9

package sftp

// Tests for the STAT -> LSTAT fallback in (*Fs).stat and statusCodeOf.
//
// The servers that need the fallback refuse SSH_FXP_STAT while answering
// SSH_FXP_LSTAT (and readdir). These tests reproduce that against an
// in-process SSH+SFTP server on 127.0.0.1: golang.org/x/crypto/ssh for the
// transport and pkg/sftp's in-memory request server for the filesystem, with
// a lister wrapper that refuses whichever request the test asks it to.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// statRefusal is the vendor status code the NAS that needed the fallback
// sends: outside the nine codes SFTP v3 defines.
const statRefusal = 31

// refusingLister wraps the in-memory filesystem's lister and refuses STAT
// and/or LSTAT on demand, counting LSTATs.
type refusingLister struct {
	inner       sftp.FileLister
	refuseStat  atomic.Bool
	refuseLstat atomic.Bool
	lstats      atomic.Int32
}

func (l *refusingLister) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	if r.Method == "Stat" && l.refuseStat.Load() {
		// pkg/sftp's request server sends the numeric value of an fxerr as the
		// status code; derive one outside the v3 range from an exported value.
		return nil, sftp.ErrSSHFxOk + statRefusal
	}
	return l.inner.Filelist(r)
}

func (l *refusingLister) Lstat(r *sftp.Request) (sftp.ListerAt, error) {
	l.lstats.Add(1)
	if l.refuseLstat.Load() {
		return nil, sftp.ErrSSHFxOpUnsupported
	}
	return l.inner.(sftp.LstatFileLister).Lstat(r)
}

// startSFTPServer runs a password-authenticated SSH server on loopback whose
// only capability is the sftp subsystem, and returns its port.
func startSFTPServer(t *testing.T, lister *refusingLister) int {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(hostPriv)
	require.NoError(t, err)
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == "tester" && string(pass) == "secret" {
				return nil, nil
			}
			return nil, errors.New("denied")
		},
	}
	cfg.AddHostKey(signer)

	handlers := sftp.InMemHandler()
	lister.inner = handlers.FileList
	handlers.FileList = lister

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	var connsMu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = l.Close()
		connsMu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		connsMu.Unlock()
		wg.Wait()
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			conns = append(conns, nc)
			connsMu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				serveSSHConn(nc, cfg, handlers)
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

func serveSSHConn(nc net.Conn, cfg *ssh.ServerConfig, handlers sftp.Handlers) {
	sc, chans, reqs, err := ssh.NewServerConn(nc, cfg)
	if err != nil {
		return
	}
	defer func() { _ = sc.Close() }()
	go ssh.DiscardRequests(reqs)
	for nch := range chans {
		if nch.ChannelType() != "session" {
			_ = nch.Reject(ssh.UnknownChannelType, "session only")
			continue
		}
		ch, chReqs, err := nch.Accept()
		if err != nil {
			return
		}
		go func() {
			for r := range chReqs {
				// Payload is a length-prefixed string; "sftp" is the only
				// subsystem served. exec/shell are refused.
				ok := r.Type == "subsystem" && bytes.HasSuffix(r.Payload, []byte("sftp"))
				_ = r.Reply(ok, nil)
				if ok {
					go func() {
						srv := sftp.NewRequestServer(ch, handlers)
						_ = srv.Serve()
						_ = srv.Close()
					}()
				}
			}
		}()
	}
}

// newStatTestFs connects an sftp Fs to a fresh loopback server holding
// /dir/file.txt.
func newStatTestFs(t *testing.T) (*Fs, *refusingLister, context.Context) {
	t.Helper()
	lister := &refusingLister{}
	port := startSFTPServer(t, lister)

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ci.ConnectTimeout = fs.Duration(5 * time.Second)
	m := configmap.Simple{
		"host":             "127.0.0.1",
		"port":             strconv.Itoa(port),
		"user":             "tester",
		"pass":             obscure.MustObscure("secret"),
		"known_hosts_file": "none",
		"shell_type":       "none",
		"md5sum_command":   "none",
		"sha1sum_command":  "none",
		"set_modtime":      "false",
	}
	// configmap.Simple carries no defaults; fill in every option's default
	// (subsystem, concurrency, chunk size, ...) the way a configured remote
	// would get them.
	ri, err := fs.Find("sftp")
	require.NoError(t, err)
	for _, o := range ri.Options {
		if _, ok := m[o.Name]; !ok && o.Default != nil {
			m[o.Name] = o.String()
		}
	}
	f, err := NewFs(ctx, "stattest", "/dir", m)
	require.NoError(t, err)
	sf := f.(*Fs)
	t.Cleanup(func() { _ = sf.Shutdown(context.Background()) })

	require.NoError(t, sf.Mkdir(ctx, ""))
	payload := []byte("hello sftp")
	src := object.NewStaticObjectInfo("file.txt", time.Now(), int64(len(payload)), true, nil, nil)
	_, err = sf.Put(ctx, bytes.NewReader(payload), src)
	require.NoError(t, err)
	return sf, lister, ctx
}

func TestStatusCodeOf(t *testing.T) {
	assert.Equal(t, uint32(0), statusCodeOf(nil))
	assert.Equal(t, uint32(0), statusCodeOf(errors.New("plain")))
	assert.Equal(t, uint32(0), statusCodeOf(io.EOF))
	se := &sftp.StatusError{Code: statRefusal}
	assert.Equal(t, uint32(statRefusal), statusCodeOf(se))
	assert.Equal(t, uint32(statRefusal), statusCodeOf(fmt.Errorf("wrapped: %w", se)))
	assert.Equal(t, uint32(8), statusCodeOf(fmt.Errorf("stat: %w", &sftp.StatusError{Code: 8})))
}

func TestStatWorksNormally(t *testing.T) {
	f, lister, ctx := newStatTestFs(t)
	lister.lstats.Store(0)

	o, err := f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(10), o.Size())
	assert.Equal(t, int32(0), lister.lstats.Load(), "LSTAT used although STAT works")

	// Absolute paths are used as given.
	info, err := f.stat(ctx, "/dir/file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(10), info.Size())
}

// The NAS case: STAT refused with an out-of-range code, LSTAT works.
func TestStatFallsBackToLstat(t *testing.T) {
	f, lister, ctx := newStatTestFs(t)
	lister.refuseStat.Store(true)
	lister.lstats.Store(0)

	o, err := f.NewObject(ctx, "file.txt")
	require.NoError(t, err, "a refused STAT must fall back to LSTAT")
	assert.Equal(t, int64(10), o.Size())

	// Again: the fallback is used every time (only the log is once-only).
	_, err = f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	assert.Equal(t, int32(2), lister.lstats.Load())

	// Directories too.
	info, err := f.stat(ctx, "/dir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

// Not-exist is final: no LSTAT, so a dangling symlink still reads as missing.
func TestStatNotExistDoesNotFallBack(t *testing.T) {
	f, lister, ctx := newStatTestFs(t)
	lister.lstats.Store(0)

	_, err := f.NewObject(ctx, "missing.txt")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.Equal(t, int32(0), lister.lstats.Load())
}

// Both refused: STAT's original error comes back, with its own code.
func TestStatAndLstatBothRefused(t *testing.T) {
	f, lister, ctx := newStatTestFs(t)
	lister.refuseStat.Store(true)
	lister.refuseLstat.Store(true)
	lister.lstats.Store(0)

	for i := 0; i < 2; i++ {
		_, err := f.stat(ctx, "file.txt")
		require.Error(t, err)
		assert.Equal(t, uint32(statRefusal), statusCodeOf(err), "want STAT's error, got %v", err)
	}
	assert.Equal(t, int32(2), lister.lstats.Load())
}

// No connection at all: the dial error is reported as a stat error.
func TestStatNoConnection(t *testing.T) {
	f, _, ctx := newStatTestFs(t)
	require.NoError(t, f.Shutdown(ctx))
	f.opt.Port = "1" // nothing listens here
	_, err := f.stat(ctx, "file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stat:")
}

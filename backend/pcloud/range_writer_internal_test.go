package pcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pcloudMock is enough of pCloud's API to list one folder and drive the
// file API (file_open, file_pwrite, file_truncate, file_close) against
// in-memory files. It refuses anything that would upload a whole file.
type pcloudMock struct {
	t      *testing.T
	mu     sync.Mutex
	files  map[int64]*mockFile // by fileid
	fds    map[int64]int64     // fd -> fileid
	nextFD int64
	calls  []string // method names, in order
	// failNextOp makes the next file_pwrite or file_truncate answer with
	// pCloud's "Invalid or closed file descriptor" once.
	failNextOp bool
	// denyOpen makes file_open answer "Access denied" (2003).
	denyOpen bool
}

type mockFile struct {
	id      int64
	name    string
	content []byte
	modTime time.Time
}

func (m *pcloudMock) item(f *mockFile) map[string]any {
	return map[string]any{
		"name":     f.name,
		"id":       fmt.Sprintf("f%d", f.id),
		"fileid":   f.id,
		"isfolder": false,
		"size":     len(f.content),
		"modified": f.modTime.UTC().Format(time.RFC1123Z),
		"created":  f.modTime.UTC().Format(time.RFC1123Z),
	}
}

func (m *pcloudMock) reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	require.NoError(m.t, json.NewEncoder(w).Encode(v))
}

func (m *pcloudMock) fail(w http.ResponseWriter, result int, msg string) {
	m.reply(w, map[string]any{"result": result, "error": msg})
}

func (m *pcloudMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	method := strings.TrimPrefix(r.URL.Path, "/")
	m.calls = append(m.calls, method)
	q := r.URL.Query()
	fdOf := func() (*mockFile, bool) {
		fd, _ := strconv.ParseInt(q.Get("fd"), 10, 64)
		id, ok := m.fds[fd]
		if !ok {
			m.fail(w, 1007, "Invalid or closed file descriptor.")
			return nil, false
		}
		return m.files[id], true
	}
	switch method {
	case "listfolder":
		if q.Get("folderid") != "0" {
			m.fail(w, 2005, "Directory does not exist.")
			return
		}
		contents := []any{}
		for _, f := range m.files {
			contents = append(contents, m.item(f))
		}
		m.reply(w, map[string]any{"result": 0, "metadata": map[string]any{
			"name": "/", "id": "d0", "folderid": 0, "isfolder": true, "contents": contents,
		}})
	case "file_open":
		if m.denyOpen {
			m.fail(w, 2003, "Access denied. You do not have permissions to perform this operation.")
			return
		}
		// An existing file, opened by id, for writing only: no O_CREAT, no
		// O_TRUNC, so nothing is created and nothing is cut.
		assert.Equal(m.t, "0x0002", q.Get("flags"), "file_open flags")
		assert.Empty(m.t, q.Get("folderid"), "file_open must not name a folder (that is the create path pCloud refuses)")
		id, _ := strconv.ParseInt(q.Get("fileid"), 10, 64)
		f, ok := m.files[id]
		if !ok {
			m.fail(w, 2009, "File not found.")
			return
		}
		m.nextFD++
		m.fds[m.nextFD] = f.id
		m.reply(w, map[string]any{"result": 0, "fd": m.nextFD, "fileid": f.id})
	case "file_pwrite":
		if m.failNextOp {
			m.failNextOp = false
			m.fail(w, 1007, "Invalid or closed file descriptor.")
			return
		}
		f, ok := fdOf()
		if !ok {
			return
		}
		off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
		data, err := io.ReadAll(r.Body)
		require.NoError(m.t, err)
		if end := off + int64(len(data)); end > int64(len(f.content)) {
			f.content = append(f.content, make([]byte, end-int64(len(f.content)))...)
		}
		copy(f.content[off:], data)
		f.modTime = time.Now()
		m.reply(w, map[string]any{"result": 0, "bytes": len(data)})
	case "file_truncate":
		if m.failNextOp {
			m.failNextOp = false
			m.fail(w, 1007, "Invalid or closed file descriptor.")
			return
		}
		f, ok := fdOf()
		if !ok {
			return
		}
		length, _ := strconv.ParseInt(q.Get("length"), 10, 64)
		if length <= int64(len(f.content)) {
			f.content = f.content[:length]
		} else {
			f.content = append(f.content, make([]byte, length-int64(len(f.content)))...)
		}
		f.modTime = time.Now()
		m.reply(w, map[string]any{"result": 0})
	case "file_close":
		fd, _ := strconv.ParseInt(q.Get("fd"), 10, 64)
		if _, ok := m.fds[fd]; !ok {
			m.fail(w, 1007, "Invalid or closed file descriptor.")
			return
		}
		delete(m.fds, fd)
		m.reply(w, map[string]any{"result": 0})
	default:
		m.t.Errorf("unexpected pCloud call %s %s", r.Method, r.URL)
		m.fail(w, 2000, "unexpected call")
	}
}

func (m *pcloudMock) count(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c == method {
			n++
		}
	}
	return n
}

func (m *pcloudMock) contentOf(id int64) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return string(m.files[id].content)
}

// newMockFs serves the mock over TLS (the backend only speaks https to its
// hostname) and makes an Fs on it.
func newMockFs(t *testing.T) (*pcloudMock, *Fs, context.Context) {
	m := &pcloudMock{
		t:     t,
		files: map[int64]*mockFile{1: {id: 1, name: "doc.bin", content: []byte("0123456789"), modTime: time.Unix(1700000000, 0)}},
		fds:   map[int64]int64{},
	}
	srv := httptest.NewTLSServer(m)
	t.Cleanup(srv.Close)
	ctx, ci := fs.AddConfig(context.Background())
	ci.InsecureSkipVerify = true // the test server's certificate
	conf := configmap.Simple{
		"token":          `{"access_token":"test","token_type":"bearer","expiry":"0001-01-01T00:00:00Z"}`,
		"hostname":       strings.TrimPrefix(srv.URL, "https://"),
		"root_folder_id": "d0",
	}
	f, err := NewFs(ctx, "TestPcloudRange", "", conf)
	require.NoError(t, err)
	return m, f.(*Fs), ctx
}

// An in-place edit on pCloud is file_open by id, file_pwrite at offsets,
// file_truncate and file_close on one descriptor: the bytes written travel,
// not the file, and the object describes the result after Close.
func TestOpenRangeWriterEditsInPlace(t *testing.T) {
	m, f, ctx := newMockFs(t)
	require.NotNil(t, f.Features().OpenRangeWriter, "pcloud advertises OpenRangeWriter")

	w, err := f.OpenRangeWriter(ctx, "doc.bin")
	require.NoError(t, err)

	n, err := w.WriteAt([]byte("XY"), 3)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "012XY56789", m.contentOf(1), "a patch inside the file")

	n, err = w.WriteAt([]byte("tail"), 10)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "012XY56789tail", m.contentOf(1), "a write past the end extends the file")

	require.NoError(t, w.Truncate(12))
	assert.Equal(t, "012XY56789ta", m.contentOf(1), "truncate cuts it")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	assert.Equal(t, int64(12), w.(*rangeWriter).o.Size(), "the object knows the size after close")
	assert.Equal(t, 1, m.count("file_open"), "one descriptor for the whole edit")
	assert.Equal(t, 1, m.count("file_close"))
	assert.Equal(t, 2, m.count("file_pwrite"))
	assert.Equal(t, 1, m.count("file_truncate"))
	assert.Equal(t, 0, m.count("uploadfile"), "nothing is uploaded whole")

	o, err := f.NewObject(ctx, "doc.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(12), o.Size())

	_, err = w.WriteAt([]byte("x"), 0)
	assert.Error(t, err, "closed")
}

// A descriptor is bound to its connection; when pCloud says it is gone the
// handle reopens the file and the write lands anyway.
func TestOpenRangeWriterReopensALostDescriptor(t *testing.T) {
	m, f, ctx := newMockFs(t)
	w, err := f.OpenRangeWriter(ctx, "doc.bin")
	require.NoError(t, err)

	m.mu.Lock()
	m.failNextOp = true
	m.mu.Unlock()
	_, err = w.WriteAt([]byte("ab"), 0)
	require.NoError(t, err)
	assert.Equal(t, "ab23456789", m.contentOf(1))
	assert.Equal(t, 2, m.count("file_open"), "reopened once")

	m.mu.Lock()
	m.failNextOp = true
	m.mu.Unlock()
	require.NoError(t, w.Truncate(4))
	assert.Equal(t, "ab23", m.contentOf(1))
	assert.Equal(t, 3, m.count("file_open"))
	require.NoError(t, w.Close())
}

// No object, no handle; and a pCloud that refuses the file API is a
// permission error, which the VFS turns into EPERM on the open.
func TestOpenRangeWriterRefusals(t *testing.T) {
	m, f, ctx := newMockFs(t)
	_, err := f.OpenRangeWriter(ctx, "missing.bin")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	m.mu.Lock()
	m.denyOpen = true
	m.mu.Unlock()
	_, err = f.OpenRangeWriter(ctx, "doc.bin")
	assert.ErrorIs(t, err, fs.ErrorPermissionDenied)
	assert.Equal(t, "0123456789", m.contentOf(1), "untouched")
	opens := m.count("file_open")

	// The denial is remembered: the next open is refused without asking
	// pCloud again, even once pCloud would allow it.
	m.mu.Lock()
	m.denyOpen = false
	m.mu.Unlock()
	_, err = f.OpenRangeWriter(ctx, "doc.bin")
	assert.ErrorIs(t, err, fs.ErrorPermissionDenied)
	assert.Equal(t, opens, m.count("file_open"), "no further file_open after a 2003")
}

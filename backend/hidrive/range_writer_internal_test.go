package hidrive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hidriveMock is enough of HiDrive's API to stat paths and take PATCH /file
// at an offset and POST /file/truncate against one in-memory file. A PUT
// of the whole file is an error.
type hidriveMock struct {
	t       *testing.T
	mu      sync.Mutex
	content []byte
	modTime time.Time
	calls   []string
}

func (m *hidriveMock) meta(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	require.NoError(m.t, json.NewEncoder(w).Encode(map[string]any{
		"type": "file", "name": "doc.bin", "id": "b1", "path": "/doc.bin",
		"size": len(m.content), "mtime": m.modTime.Unix(), "chash": "",
	}))
}

func (m *hidriveMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, r.Method+" "+r.URL.Path)
	q := r.URL.Query()
	switch r.Method + " " + r.URL.Path {
	case "GET /meta":
		switch q.Get("path") {
		case "/":
			w.Header().Set("Content-Type", "application/json")
			require.NoError(m.t, json.NewEncoder(w).Encode(map[string]any{"type": "dir", "name": "", "path": "/"}))
		case "/doc.bin":
			m.meta(w)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"msg":"not found"}`))
		}
	case "PATCH /file":
		require.Equal(m.t, "/doc.bin", q.Get("path"))
		off, err := strconv.ParseInt(q.Get("offset"), 10, 64)
		require.NoError(m.t, err)
		data, err := io.ReadAll(r.Body)
		require.NoError(m.t, err)
		if end := off + int64(len(data)); end > int64(len(m.content)) {
			m.content = append(m.content, make([]byte, end-int64(len(m.content)))...)
		}
		copy(m.content[off:], data)
		m.modTime = time.Now()
		w.WriteHeader(http.StatusNoContent)
	case "POST /file/truncate":
		require.Equal(m.t, "/doc.bin", q.Get("path"))
		size, err := strconv.ParseInt(q.Get("size"), 10, 64)
		require.NoError(m.t, err)
		if size <= int64(len(m.content)) {
			m.content = m.content[:size]
		} else {
			m.content = append(m.content, make([]byte, size-int64(len(m.content)))...)
		}
		m.modTime = time.Now()
		m.meta(w)
	default:
		m.t.Errorf("unexpected HiDrive call %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":400,"msg":"unexpected call"}`))
	}
}

func (m *hidriveMock) contentOf() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return string(m.content)
}

func (m *hidriveMock) count(call string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c == call {
			n++
		}
	}
	return n
}

func newMockFs(t *testing.T) (*hidriveMock, *Fs, context.Context) {
	m := &hidriveMock{t: t, content: []byte("0123456789"), modTime: time.Unix(1700000000, 0)}
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	ctx := context.Background()
	conf := configmap.Simple{
		"token":       `{"access_token":"test","token_type":"bearer","expiry":"0001-01-01T00:00:00Z"}`,
		"endpoint":    srv.URL,
		"root_prefix": "/",
	}
	f, err := NewFs(ctx, "TestHiDriveRange", "", conf)
	require.NoError(t, err)
	return m, f.(*Fs), ctx
}

// An in-place edit on HiDrive is PATCH /file at offsets and POST
// /file/truncate: the bytes written travel, not the file, and the object
// describes the result after Close.
func TestOpenRangeWriterEditsInPlace(t *testing.T) {
	m, f, ctx := newMockFs(t)
	require.NotNil(t, f.Features().OpenRangeWriter, "hidrive advertises OpenRangeWriter")

	w, err := f.OpenRangeWriter(ctx, "doc.bin")
	require.NoError(t, err)

	n, err := w.WriteAt([]byte("XY"), 3)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "012XY56789", m.contentOf(), "a patch inside the file")

	n, err = w.WriteAt([]byte("tail"), 10)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "012XY56789tail", m.contentOf(), "a write past the end extends the file")

	require.NoError(t, w.Truncate(12))
	assert.Equal(t, "012XY56789ta", m.contentOf(), "truncate cuts it")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	assert.Equal(t, int64(12), w.(*rangeWriter).o.Size(), "the object knows the size after close")
	assert.Equal(t, 2, m.count("PATCH /file"))
	assert.Equal(t, 1, m.count("POST /file/truncate"))
	assert.Equal(t, 0, m.count("PUT /file"), "nothing is uploaded whole")

	_, err = w.WriteAt([]byte("x"), 0)
	assert.Error(t, err, "closed")

	_, err = f.OpenRangeWriter(ctx, "missing.bin")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

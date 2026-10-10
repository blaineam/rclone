package internxt

// FindLeaf / NewObject / findFile against a fake Internxt API on
// loopback. The fake stores names in NFC and, like the real server's
// existence check, matches them byte-wise -- the situation the fork's
// normalization-insensitive matching exists for.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
	"github.com/internxt/rclone-adapter/files"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fakeRootID = "root-uuid"

type fakeFile struct {
	UUID, PlainName, Type, Size string
}

type fakeInternxt struct {
	mu          sync.Mutex
	folders     map[string][]map[string]any // parent -> folders
	files       map[string][]fakeFile       // parent -> files
	failList    int                         // HTTP status for list calls (0 = ok)
	failExist   int                         // HTTP status for existence checks
	failMeta    int                         // HTTP status for meta calls
	existChecks []string                    // plainName+"."+type asked about
}

func (s *fakeInternxt) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.TrimPrefix(r.URL.Path, "/drive")
	parts := strings.Split(strings.Trim(p, "/"), "/")
	w.Header().Set("Content-Type", "application/json")
	switch {
	// /folders/content/{parent}/folders|files
	case len(parts) == 4 && parts[0] == "folders" && parts[1] == "content" && r.Method == http.MethodGet:
		if s.failList != 0 {
			http.Error(w, `{"error":"boom"}`, s.failList)
			return
		}
		if r.URL.Query().Get("offset") != "0" {
			_, _ = w.Write([]byte(`{"folders":[],"files":[]}`))
			return
		}
		parent := parts[2]
		if parts[3] == "folders" {
			_ = json.NewEncoder(w).Encode(map[string]any{"folders": s.folders[parent]})
			return
		}
		var out []map[string]any
		for _, f := range s.files[parent] {
			out = append(out, map[string]any{"uuid": f.UUID, "plainName": f.PlainName, "type": f.Type, "size": f.Size})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"files": out})
	// /folders/content/{parent}/files/existence
	case len(parts) == 5 && parts[4] == "existence" && r.Method == http.MethodPost:
		if s.failExist != 0 {
			http.Error(w, `{"error":"boom"}`, s.failExist)
			return
		}
		var body files.CheckFilesExistenceRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		var results []files.FileExistenceResult
		for _, q := range body.Files {
			s.existChecks = append(s.existChecks, q.PlainName+"."+q.Type)
			res := files.FileExistenceResult{PlainName: q.PlainName, Type: q.Type}
			for _, f := range s.files[parts[2]] {
				if f.PlainName == q.PlainName { // byte-wise, like the real server
					res.Exists, res.UUID, res.Type = true, f.UUID, f.Type
				}
			}
			results = append(results, res)
		}
		_ = json.NewEncoder(w).Encode(files.CheckFilesExistenceResponse{Files: results})
	// /files/{uuid}/meta
	case len(parts) == 3 && parts[0] == "files" && parts[2] == "meta":
		if s.failMeta != 0 {
			http.Error(w, `{"error":"boom"}`, s.failMeta)
			return
		}
		for _, list := range s.files {
			for _, f := range list {
				if f.UUID == parts[1] {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"uuid": f.UUID, "plainName": f.PlainName, "type": f.Type, "size": f.Size,
					})
					return
				}
			}
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

func newFakeInternxtFs(t *testing.T) (*Fs, *fakeInternxt, context.Context) {
	t.Helper()
	srv := &fakeInternxt{
		folders: map[string][]map[string]any{
			fakeRootID: {{"uuid": "folder-cafe", "plainName": nfc}},
		},
		files: map[string][]fakeFile{
			fakeRootID: {
				{UUID: "file-cafe", PlainName: nfc, Type: "txt", Size: "4"},
				{UUID: "file-noext", PlainName: "README", Type: "", Size: "1"},
			},
		},
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1

	ri, err := fs.Find("internxt")
	require.NoError(t, err)
	enc := ri.Options.Get("encoding").Default.(encoder.MultiEncoder)

	cfg := &config.Config{Token: "test-token", RootFolderID: fakeRootID, Endpoints: endpoints.NewConfig(ts.URL)}
	cfg.ApplyDefaults()
	f := &Fs{name: "ix", opt: Options{Encoding: enc}, cfg: cfg, authFailed: true}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(minSleep), pacer.DecayConstant(decayConstant)))
	f.dirCache = dircache.New("", fakeRootID, f)
	return f, srv, ctx
}

func TestFindLeafNormalizationInsensitive(t *testing.T) {
	f, srv, ctx := newFakeInternxtFs(t)

	for _, leaf := range []string{nfc, nfd} {
		id, found, err := f.FindLeaf(ctx, fakeRootID, leaf)
		require.NoError(t, err)
		assert.True(t, found, "FindLeaf(%q)", leaf)
		assert.Equal(t, "folder-cafe", id)
	}

	_, found, err := f.FindLeaf(ctx, fakeRootID, "nope")
	require.NoError(t, err)
	assert.False(t, found)

	srv.failList = http.StatusBadRequest
	_, _, err = f.FindLeaf(ctx, fakeRootID, nfc)
	assert.Error(t, err)
}

func TestNewObjectNormalizationInsensitive(t *testing.T) {
	f, srv, ctx := newFakeInternxtFs(t)

	for _, name := range []string{nfc + ".txt", nfd + ".txt"} {
		o, err := f.NewObject(ctx, name)
		require.NoError(t, err, "NewObject(%q)", name)
		assert.Equal(t, name, o.Remote())
		assert.Equal(t, int64(4), o.Size())
	}

	// A file stored without an extension.
	_, err := f.NewObject(ctx, "README")
	require.NoError(t, err)

	_, err = f.NewObject(ctx, "missing.txt")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	// Parent directory that does not exist.
	_, err = f.NewObject(ctx, "nodir/x.txt")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	// A failing lookup is an error, not "not found". The parent dir is
	// cached by now so the failure lands on the file existence check.
	srv.failExist = http.StatusBadRequest
	_, err = f.NewObject(ctx, "x.txt")
	require.Error(t, err)
	assert.NotErrorIs(t, err, fs.ErrorObjectNotFound)
}

// findFile (used by NewObject, Put and Update) retries the byte-wise
// existence check with the other Unicode normalization form. Upstream's
// findFile asks for two spellings per name (split at the extension, and
// the whole name with no type), so each variant costs two criteria.
func TestFindFileTriesNormalizationVariants(t *testing.T) {
	f, srv, ctx := newFakeInternxtFs(t)

	// NFD in, NFC stored: the first (verbatim) check misses, the NFC
	// variant finds the existing file instead of creating a duplicate.
	file, err := f.findFile(ctx, nfd+".txt", fakeRootID)
	require.NoError(t, err)
	require.NotNil(t, file)
	assert.Equal(t, "file-cafe", file.UUID)
	assert.Equal(t, []string{nfd + ".txt", nfd + ".txt.", nfc + ".txt", nfc + ".txt."}, srv.existChecks)

	// Verbatim match: the first variant's request only.
	srv.existChecks = nil
	file, err = f.findFile(ctx, nfc+".txt", fakeRootID)
	require.NoError(t, err)
	require.NotNil(t, file)
	assert.Len(t, srv.existChecks, 2)

	// Not there in any form.
	file, err = f.findFile(ctx, "new.txt", fakeRootID)
	require.NoError(t, err)
	assert.Nil(t, file)

	// Same base name, different extension: not the same file.
	file, err = f.findFile(ctx, nfc+".md", fakeRootID)
	require.NoError(t, err)
	assert.Nil(t, file)
}

func TestFindFileErrors(t *testing.T) {
	f, srv, ctx := newFakeInternxtFs(t)

	// Since v1.75.2 a failing existence check is surfaced (upstream no
	// longer guesses "does not exist", which could overwrite or duplicate),
	// and it stops the variant loop.
	srv.failExist = http.StatusBadRequest
	_, err := f.findFile(ctx, nfd+".txt", fakeRootID)
	require.Error(t, err)
	assert.Empty(t, srv.existChecks)

	// A failing metadata fetch for a file that does exist is an error,
	// and stops the variant loop.
	srv.failExist = 0
	srv.failMeta = http.StatusBadRequest
	srv.existChecks = nil
	_, err = f.findFile(ctx, nfc+".txt", fakeRootID)
	require.Error(t, err)
	assert.Len(t, srv.existChecks, 2)
}

// An existence hit without a UUID cannot be resolved to a file.
func TestFindFileExistsWithoutUUID(t *testing.T) {
	f, srv, ctx := newFakeInternxtFs(t)
	srv.files[fakeRootID] = append(srv.files[fakeRootID], fakeFile{PlainName: "ghost", Type: "bin"})
	file, err := f.findFile(ctx, "ghost.bin", fakeRootID)
	require.NoError(t, err)
	assert.Nil(t, file)
}

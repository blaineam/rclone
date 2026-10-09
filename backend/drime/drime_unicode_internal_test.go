package drime

// FindLeaf and NewObject (readMetaDataForPath) against a fake Drime API on
// loopback that stores names in NFC, looked up with both NFC and the NFD
// names Apple clients send.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rclone/rclone/backend/drime/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nfc = "café"
	nfd = "café"
)

func newFakeDrimeFs(t *testing.T) (*Fs, *int, context.Context) {
	t.Helper()
	fail := new(int)
	mux := http.NewServeMux()
	mux.HandleFunc("/drive/file-entries", func(w http.ResponseWriter, r *http.Request) {
		if *fail != 0 {
			http.Error(w, `{"message":"boom"}`, *fail)
			return
		}
		var items []api.Item
		switch r.URL.Query().Get("folderId") {
		case "", "root":
			items = []api.Item{
				{ID: "10", Name: nfc, Type: api.ItemTypeFolder},
				{ID: "11", Name: nfc + ".txt", Type: "text", FileSize: 4},
			}
		case "10":
			items = []api.Item{{ID: "12", Name: "inner.txt", Type: "text", FileSize: 1}}
		}
		// Two pages, to walk the pagination as well.
		page := r.URL.Query().Get("page")
		out := api.Listing{CurrentPage: 1, LastPage: 2}
		if page == "2" {
			out = api.Listing{CurrentPage: 2, LastPage: 2, Data: items}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ri, err := fs.Find("drime")
	require.NoError(t, err)
	f := &Fs{
		name: "drime",
		opt: Options{
			Enc:       ri.Options.Get("encoding").Default.(encoder.MultiEncoder),
			ListChunk: 50,
		},
		srv:   rest.NewClient(ts.Client()).SetRoot(ts.URL),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(minSleep), pacer.DecayConstant(decayConstant))),
	}
	f.srv.SetErrorHandler(errorHandler)
	f.features = (&fs.Features{}).Fill(ctx, f)
	f.dirCache = dircache.New("", "", f)
	return f, fail, ctx
}

func TestDrimeFindLeafNormalizationInsensitive(t *testing.T) {
	f, fail, ctx := newFakeDrimeFs(t)
	for _, leaf := range []string{nfc, nfd} {
		id, found, err := f.FindLeaf(ctx, "", leaf)
		require.NoError(t, err)
		assert.True(t, found, "FindLeaf(%q)", leaf)
		assert.Equal(t, "10", id)
	}
	// Files are not directories.
	_, found, err := f.FindLeaf(ctx, "", nfc+".txt")
	require.NoError(t, err)
	assert.False(t, found)

	*fail = http.StatusBadRequest
	_, _, err = f.FindLeaf(ctx, "", nfc)
	assert.Error(t, err)
}

func TestDrimeNewObjectNormalizationInsensitive(t *testing.T) {
	f, fail, ctx := newFakeDrimeFs(t)
	for _, name := range []string{nfc + ".txt", nfd + ".txt", nfd + "/inner.txt"} {
		o, err := f.NewObject(ctx, name)
		require.NoError(t, err, "NewObject(%q)", name)
		assert.Equal(t, name, o.Remote())
	}

	_, err := f.NewObject(ctx, "missing.txt")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	// A directory is not an object.
	_, err = f.NewObject(ctx, nfc)
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	// Missing parent directory.
	_, err = f.NewObject(ctx, "nodir/x.txt")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	*fail = http.StatusBadRequest
	_, err = f.NewObject(ctx, "x.txt")
	require.Error(t, err)
	assert.NotErrorIs(t, err, fs.ErrorObjectNotFound)
}

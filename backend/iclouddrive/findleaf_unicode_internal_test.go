//go:build !plan9 && !solaris

package iclouddrive

// findLeafItem against a fake iCloud Drive web service on loopback. iCloud
// returns names in NFD; the leaf being looked up may arrive in either form,
// and both sides must be normalized before comparing.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rclone/rclone/backend/iclouddrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	icNFC = "café"
	icNFD = "café"
)

func newFakeICloudDriveFs(t *testing.T) (*Fs, *int) {
	t.Helper()
	fail := new(int)
	mux := http.NewServeMux()
	mux.HandleFunc("/retrieveItemDetailsInFolders", func(w http.ResponseWriter, r *http.Request) {
		if *fail != 0 {
			http.Error(w, "{}", *fail)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Names exactly as iCloud sends them: decomposed.
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"drivewsid": "FOLDER::com.apple.CloudDocs::root",
			"type":      "FOLDER",
			"items": []map[string]any{
				{"drivewsid": "FOLDER::com.apple.CloudDocs::d1", "name": icNFD, "type": "FOLDER"},
				{"drivewsid": "FILE::com.apple.CloudDocs::f1", "name": icNFD, "extension": "txt", "type": "FILE"},
			},
		}})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client, err := api.New("tester@example.invalid", "", "", "", nil, nil, "test", api.WsDrive)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(`{"webservices":{"drivews":{"url":"`+ts.URL+`"},"docws":{"url":"`+ts.URL+`"}}}`),
		&client.Session.AccountInfo))
	service, err := api.NewDriveService(client)
	require.NoError(t, err)

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ri, err := fs.Find("iclouddrive")
	require.NoError(t, err)
	f := &Fs{
		name:    "icd",
		opt:     Options{Enc: ri.Options.Get("encoding").Default.(encoder.MultiEncoder)},
		icloud:  client,
		service: service,
		pacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(minSleep), pacer.DecayConstant(decayConstant))),
	}
	return f, fail
}

func TestFindLeafItemNormalizesBothSides(t *testing.T) {
	f, fail := newFakeICloudDriveFs(t)
	ctx := context.Background()
	root := "FOLDER::com.apple.CloudDocs::root"

	for _, leaf := range []string{icNFC, icNFD} {
		item, found, err := f.findLeafItem(ctx, root, leaf)
		require.NoError(t, err)
		require.True(t, found, "findLeafItem(%q)", leaf)
		assert.True(t, item.IsFolder())

		item, found, err = f.findLeafItem(ctx, root, leaf+".txt")
		require.NoError(t, err)
		require.True(t, found, "findLeafItem(%q.txt)", leaf)
		assert.False(t, item.IsFolder())
	}

	// Case-insensitive, as before.
	_, found, err := f.findLeafItem(ctx, root, "CAFÉ.TXT")
	require.NoError(t, err)
	assert.True(t, found)

	_, found, err = f.findLeafItem(ctx, root, "nope")
	require.NoError(t, err)
	assert.False(t, found)

	*fail = http.StatusBadRequest
	_, _, err = f.findLeafItem(ctx, root, icNFC)
	assert.Error(t, err)
}

package webdav

// Tests for the Enter Space display_name option: the served root reports
// DAV:displayname so macOS mounts the volume under that name rather than
// "127.0.0.1". Unlike webdav_test.go this file carries no darwin/windows
// build constraint — macOS is exactly where the option matters.

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type davMultistatus struct {
	Responses []struct {
		Href     string `xml:"href"`
		Propstat []struct {
			Prop struct {
				DisplayName *string `xml:"displayname"`
				Checksums   *struct {
					Inner string `xml:",innerxml"`
				} `xml:"checksums"`
			} `xml:"prop"`
			Status string `xml:"status"`
		} `xml:"propstat"`
	} `xml:"response"`
}

// startDisplayNameServer serves a temp dir (holding sub/ and a.txt) on
// loopback and returns its base URL.
func startDisplayNameServer(t *testing.T, displayName, etagHash string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o600))
	f, err := fs.NewFs(context.Background(), dir)
	require.NoError(t, err)

	opt := Opt
	opt.HTTP.ListenAddr = []string{"127.0.0.1:0"}
	opt.DisplayName = displayName
	opt.EtagHash = etagHash
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeOff

	w, err := newWebDAV(context.Background(), f, &opt, &vfsOpt, &proxy.Opt)
	require.NoError(t, err)
	assert.Equal(t, displayName, w.displayName)
	go func() { _ = w.Serve() }()
	t.Cleanup(func() { _ = w.Shutdown() })
	return strings.TrimRight(w.server.URLs()[0], "/")
}

func propfind(t *testing.T, url, depth string) davMultistatus {
	t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`
	r, err := http.NewRequest("PROPFIND", url, strings.NewReader(body))
	require.NoError(t, err)
	r.Header.Set("Depth", depth)
	r.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusMultiStatus, resp.StatusCode, string(raw))
	var ms davMultistatus
	require.NoError(t, xml.Unmarshal(raw, &ms), "PROPFIND reply is not well-formed XML:\n%s", raw)
	return ms
}

// displayNames maps each href in a multistatus to its displayname ("" when
// absent or empty).
func displayNames(ms davMultistatus) map[string]string {
	out := map[string]string{}
	for _, r := range ms.Responses {
		name := ""
		for _, ps := range r.Propstat {
			if ps.Prop.DisplayName != nil {
				name = *ps.Prop.DisplayName
			}
		}
		out[r.Href] = name
	}
	return out
}

func TestDisplayNameOnRoot(t *testing.T) {
	base := startDisplayNameServer(t, "Enter Space", "")
	names := displayNames(propfind(t, base+"/", "1"))
	assert.Equal(t, "Enter Space", names["/"], "root must report the configured display name: %v", names)
	// Only the root is renamed; children keep their own names.
	assert.Equal(t, "a.txt", names["/a.txt"])
	assert.Equal(t, "sub", names["/sub/"])
}

// Without the option the root keeps x/net/webdav's default (an empty name),
// so nothing changes for upstream users.
func TestDisplayNameUnset(t *testing.T) {
	base := startDisplayNameServer(t, "", "")
	names := displayNames(propfind(t, base+"/", "0"))
	assert.Equal(t, "", names["/"])
}

// The name is XML text, not markup: characters a user can put in a volume
// name must not break the reply.
func TestDisplayNameIsEscaped(t *testing.T) {
	const name = `Tom & Jerry's <Drive> "2"`
	base := startDisplayNameServer(t, name, "")
	names := displayNames(propfind(t, base+"/", "0"))
	assert.Equal(t, name, names["/"])
}

// The display name must not leak into the checksum property built after it,
// and checksums still work alongside it.
func TestDisplayNameWithEtagHash(t *testing.T) {
	base := startDisplayNameServer(t, "Vol", "MD5")
	ms := propfind(t, base+"/", "1")
	assert.Equal(t, "Vol", displayNames(ms)["/"])
	found := false
	for _, r := range ms.Responses {
		for _, ps := range r.Propstat {
			if ps.Prop.Checksums == nil {
				continue
			}
			found = true
			assert.Equal(t, "/a.txt", r.Href)
			assert.Contains(t, ps.Prop.Checksums.Inner, "MD5:5d41402abc4b2a76b9719d911017c592")
			assert.NotContains(t, ps.Prop.Checksums.Inner, "Vol")
		}
	}
	assert.True(t, found, "no checksum property on the file")
}

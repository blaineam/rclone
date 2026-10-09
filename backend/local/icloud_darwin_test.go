//go:build darwin && cgo

package local

// Tests for iCloud Drive materialization (icloud_darwin.go) and the evicted
// branch of Object.Open.
//
// None of these touch a real iCloud container. The native calls are exercised
// only against ordinary temp files (which are never dataless), and the evicted
// paths are driven through the package-level seams isICloudEvicted /
// requestICloudDownload / requestICloudEviction.

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetICloudEnv forgets the cached env-derived settings so the next call
// re-reads RCLONE_ICLOUD_MATERIALIZE / _TIMEOUT, and forgets them again when
// the test ends so no other test inherits them.
func resetICloudEnv(t *testing.T) {
	t.Helper()
	reset := func() {
		icloudEnabledOnce = sync.Once{}
		icloudEnabled = false
		icloudTimeoutOnce = sync.Once{}
		icloudTimeout = 0
	}
	reset()
	t.Cleanup(reset)
}

// forceICloudSettings pins the materialization switches without the env.
func forceICloudSettings(t *testing.T, enabled bool, timeout time.Duration) {
	t.Helper()
	resetICloudEnv(t)
	icloudEnabledOnce.Do(func() { icloudEnabled = enabled })
	icloudTimeoutOnce.Do(func() { icloudTimeout = timeout })
}

// fakeICloud swaps the native seams for an in-memory model of one file's
// eviction state, restoring the real ones when the test ends.
type fakeICloud struct {
	mu          sync.Mutex
	evicted     map[string]int // path -> how many more polls report evicted
	downloadErr error
	downloads   []string
	evictions   []string
	onDownload  func(path string)
}

func installFakeICloud(t *testing.T) *fakeICloud {
	t.Helper()
	f := &fakeICloud{evicted: map[string]int{}}
	origEvicted, origDownload, origEvict, origPoll :=
		isICloudEvicted, requestICloudDownload, requestICloudEviction, materializePollInterval
	t.Cleanup(func() {
		isICloudEvicted, requestICloudDownload, requestICloudEviction, materializePollInterval =
			origEvicted, origDownload, origEvict, origPoll
	})
	materializePollInterval = time.Millisecond
	isICloudEvicted = func(path string) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		n, ok := f.evicted[path]
		if !ok || n == 0 {
			return false
		}
		if n > 0 {
			f.evicted[path] = n - 1
		}
		return true // n < 0 means "evicted forever"
	}
	requestICloudDownload = func(path string) error {
		f.mu.Lock()
		f.downloads = append(f.downloads, path)
		hook, err := f.onDownload, f.downloadErr
		f.mu.Unlock()
		if hook != nil {
			hook(path)
		}
		return err
	}
	requestICloudEviction = func(path string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.evictions = append(f.evictions, path)
		return nil
	}
	return f
}

func (f *fakeICloud) setEvicted(path string, polls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evicted[path] = polls
}

func (f *fakeICloud) evictionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.evictions)
}

func TestICloudMaterializeEnabledEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		set  bool
		want bool
	}{
		{set: false, want: false},
		{val: "", set: true, want: false},
		{val: "true", set: true, want: true},
		{val: "TRUE", set: true, want: true},
		{val: "True", set: true, want: true},
		{val: "1", set: true, want: true},
		{val: "0", set: true, want: false},
		{val: "false", set: true, want: false},
		{val: "yes", set: true, want: false},
		{val: " true", set: true, want: false},
	} {
		t.Run(tc.val, func(t *testing.T) {
			resetICloudEnv(t)
			if tc.set {
				t.Setenv("RCLONE_ICLOUD_MATERIALIZE", tc.val)
			} else {
				t.Setenv("RCLONE_ICLOUD_MATERIALIZE", "")
				require.NoError(t, os.Unsetenv("RCLONE_ICLOUD_MATERIALIZE"))
			}
			assert.Equal(t, tc.want, iCloudMaterializeEnabled())
		})
	}
}

// The value is read once per process: flipping the env afterwards must not
// change the answer mid-run.
func TestICloudMaterializeEnabledIsLatched(t *testing.T) {
	resetICloudEnv(t)
	t.Setenv("RCLONE_ICLOUD_MATERIALIZE", "1")
	require.True(t, iCloudMaterializeEnabled())
	t.Setenv("RCLONE_ICLOUD_MATERIALIZE", "0")
	assert.True(t, iCloudMaterializeEnabled())
}

func TestICloudMaterializeTimeoutEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want time.Duration
	}{
		{val: "", want: defaultMaterializeTimeout},
		{val: "90s", want: 90 * time.Second},
		{val: "1h2m", want: time.Hour + 2*time.Minute},
		{val: "0", want: 0},
		{val: "garbage", want: defaultMaterializeTimeout},
		{val: "10", want: defaultMaterializeTimeout}, // no unit: not a duration
	} {
		t.Run(tc.val, func(t *testing.T) {
			resetICloudEnv(t)
			t.Setenv("RCLONE_ICLOUD_MATERIALIZE_TIMEOUT", tc.val)
			assert.Equal(t, tc.want, iCloudMaterializeTimeout())
		})
	}
}

// The native helpers on ordinary (non-ubiquitous) files: never dataless, and
// download/evict requests fail cleanly with an error rather than crashing.
func TestICloudNativeOnOrdinaryFiles(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "plain.txt")
	require.NoError(t, os.WriteFile(p, []byte("hello"), 0o600))

	assert.False(t, isICloudEvictedNative(p), "a regular file is not a dataless stub")
	assert.False(t, isICloudEvictedNative(dir), "a directory is not a dataless stub")
	assert.False(t, isICloudEvictedNative(filepath.Join(dir, "missing")), "stat failure reads as not evicted")

	// A file outside any ubiquity container cannot be downloaded or evicted.
	// Foundation reports that as an error; what matters here is that it is
	// surfaced as one (with a message) and the C string is handled safely.
	if err := requestICloudDownloadNative(p); err != nil {
		assert.NotEmpty(t, err.Error())
	}
	if err := requestICloudEvictionNative(p); err != nil {
		assert.NotEmpty(t, err.Error())
	}
	// evictICloudFile is best-effort: it must never panic or remove the file.
	evictICloudFile(p)
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))
}

// materializeICloudFile against the real Foundation call on a temp file:
// either Foundation refuses (error wrapped as a download-request failure) or
// accepts and the poll sees a non-dataless file straight away.
func TestMaterializeOrdinaryFileNative(t *testing.T) {
	p := filepath.Join(t.TempDir(), "plain.txt")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	err := materializeICloudFile(p, time.Second)
	if err != nil {
		assert.Contains(t, err.Error(), "iCloud download request failed")
	}
}

func TestMaterializeICloudFileSeams(t *testing.T) {
	t.Run("download request error", func(t *testing.T) {
		f := installFakeICloud(t)
		f.downloadErr = errors.New("not in a ubiquity container")
		err := materializeICloudFile("/x", time.Second)
		require.Error(t, err)
		assert.Equal(t, "iCloud download request failed: not in a ubiquity container", err.Error())
	})
	t.Run("materializes after a few polls", func(t *testing.T) {
		f := installFakeICloud(t)
		f.setEvicted("/x", 3)
		require.NoError(t, materializeICloudFile("/x", 10*time.Second))
		assert.Equal(t, []string{"/x"}, f.downloads)
	})
	t.Run("times out", func(t *testing.T) {
		f := installFakeICloud(t)
		f.setEvicted("/x", -1)
		start := time.Now()
		err := materializeICloudFile("/x", 20*time.Millisecond)
		require.Error(t, err)
		assert.Equal(t, "iCloud materialization timed out after 20ms", err.Error())
		assert.Less(t, time.Since(start), 5*time.Second)
	})
	t.Run("zero timeout never polls", func(t *testing.T) {
		f := installFakeICloud(t)
		f.setEvicted("/x", -1)
		err := materializeICloudFile("/x", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")
	})
}

func TestEvictICloudFileSeam(t *testing.T) {
	f := installFakeICloud(t)
	evictICloudFile("/a")
	assert.Equal(t, []string{"/a"}, f.evictions)

	// A failing eviction is logged, not returned or panicked.
	requestICloudEviction = func(string) error { return errors.New("busy") }
	evictICloudFile("/b")
}

type closeCounter struct {
	io.Reader
	closes atomic.Int32
	err    error
}

func (c *closeCounter) Close() error {
	c.closes.Add(1)
	return c.err
}

func TestICloudEvictOnCloseEvictsOnce(t *testing.T) {
	f := installFakeICloud(t)
	inner := &closeCounter{Reader: strings.NewReader("data"), err: errors.New("close failed")}
	rc := &icloudEvictOnClose{ReadCloser: inner, path: "/p"}

	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "data", string(b))
	assert.Equal(t, 0, f.evictionCount(), "must not evict before Close")

	// The underlying close error is passed through, and eviction still runs.
	assert.EqualError(t, rc.Close(), "close failed")
	assert.Equal(t, 1, f.evictionCount())

	// A second Close reaches the inner closer but never evicts twice.
	_ = rc.Close()
	assert.Equal(t, int32(2), inner.closes.Load())
	assert.Equal(t, 1, f.evictionCount())
}

// newICloudTestObject makes a local Fs over a temp dir holding one file.
func newICloudTestObject(t *testing.T, content string) (*Object, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	p := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	f, err := NewFs(ctx, "local", dir, configmap.Simple{})
	require.NoError(t, err)
	o, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	return o.(*Object), o.(*Object).path
}

// With materialization off (the default), Open never consults iCloud.
func TestOpenICloudDisabled(t *testing.T) {
	forceICloudSettings(t, false, time.Second)
	f := installFakeICloud(t)
	o, p := newICloudTestObject(t, "plain")
	f.setEvicted(p, -1)

	rc, err := o.Open(context.Background())
	require.NoError(t, err)
	_, isWrapped := rc.(*icloudEvictOnClose)
	assert.False(t, isWrapped)
	require.NoError(t, rc.Close())
	assert.Empty(t, f.downloads)
	assert.Equal(t, 0, f.evictionCount())
}

// Enabled, but the file is already local: no download, no eviction.
func TestOpenICloudEnabledNotEvicted(t *testing.T) {
	forceICloudSettings(t, true, time.Second)
	f := installFakeICloud(t)
	o, _ := newICloudTestObject(t, "local already")

	rc, err := o.Open(context.Background())
	require.NoError(t, err)
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "local already", string(b))
	require.NoError(t, rc.Close())
	assert.Empty(t, f.downloads)
	assert.Equal(t, 0, f.evictionCount())
}

func TestOpenICloudEvicted(t *testing.T) {
	const content = "0123456789abcdef"
	for _, tc := range []struct {
		name    string
		options []fs.OpenOption
		want    string
	}{
		{name: "plain", want: content},
		{name: "seek", options: []fs.OpenOption{&fs.SeekOption{Offset: 10}}, want: content[10:]},
		{name: "range", options: []fs.OpenOption{&fs.RangeOption{Start: 2, End: 5}}, want: content[2:6]},
		{name: "hashes", options: []fs.OpenOption{&fs.HashesOption{Hashes: hash.NewHashSet(hash.MD5)}}, want: content},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forceICloudSettings(t, true, 10*time.Second)
			f := installFakeICloud(t)
			o, p := newICloudTestObject(t, content)
			// Evicted on the Open check plus one poll, then materialized.
			f.setEvicted(p, 2)

			rc, err := o.Open(context.Background(), tc.options...)
			require.NoError(t, err)
			_, isWrapped := rc.(*icloudEvictOnClose)
			require.True(t, isWrapped, "an evicted file's reader must evict again on close")
			assert.Equal(t, []string{p}, f.downloads)

			b, err := io.ReadAll(rc)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(b))
			assert.Equal(t, 0, f.evictionCount(), "evicted before the read finished")

			require.NoError(t, rc.Close())
			assert.Equal(t, 1, f.evictionCount())
		})
	}
}

func TestOpenICloudMaterializeFails(t *testing.T) {
	forceICloudSettings(t, true, 10*time.Second)
	f := installFakeICloud(t)
	o, p := newICloudTestObject(t, "x")
	f.setEvicted(p, -1)
	f.downloadErr = errors.New("offline")

	rc, err := o.Open(context.Background())
	require.Error(t, err)
	assert.Nil(t, rc)
	assert.Contains(t, err.Error(), "iCloud materialization failed for "+p)
	assert.Contains(t, err.Error(), "offline")
	assert.Equal(t, 0, f.evictionCount())
}

func TestOpenICloudMaterializeTimesOut(t *testing.T) {
	forceICloudSettings(t, true, 15*time.Millisecond)
	f := installFakeICloud(t)
	o, p := newICloudTestObject(t, "x")
	f.setEvicted(p, -1)

	_, err := o.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out after 15ms")
}

// The file vanishing during materialization: the re-stat failure is only
// logged, and the open itself then reports the missing file.
func TestOpenICloudFileVanishesDuringMaterialize(t *testing.T) {
	forceICloudSettings(t, true, 10*time.Second)
	f := installFakeICloud(t)
	o, p := newICloudTestObject(t, "x")
	f.setEvicted(p, 1)
	f.onDownload = func(path string) { _ = os.Remove(path) }

	_, err := o.Open(context.Background())
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err), "want not-exist, got %v", err)
	assert.Equal(t, 0, f.evictionCount())
}

// The Objective-C side always sets a message when it reports failure, but a
// missing one must still produce an error rather than a nil dereference.
func TestCErrWithoutMessage(t *testing.T) {
	assert.EqualError(t, cErr(nil), "unknown error")
}

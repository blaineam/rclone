package scryptcache

// Edge and failure paths of the scrypt key cache: missing configuration,
// unwritable or unreadable cache files, wrong-length entries, and the
// password-roll cases. Everything lives under t.TempDir().

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetSlimBuild(t *testing.T) {
	old := isSlimBuild
	t.Cleanup(func() { isSlimBuild = old })
	SetSlimBuild(true)
	assert.True(t, isSlimBuild)
	SetSlimBuild(false)
	assert.False(t, isSlimBuild)
}

func TestGetDebugState(t *testing.T) {
	dir := withState(t, "pw", false)
	state := GetDebugState()
	assert.Contains(t, state, "cacheDir='"+dir+"'")
	assert.Contains(t, state, "hasEncryptionPassword=true")
	assert.Contains(t, state, "cacheDirExists=false")

	_, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	state = GetDebugState()
	assert.Contains(t, state, "cacheDirExists=true")
	assert.Contains(t, state, ".key")
	// The state never includes key material or the password.
	assert.NotContains(t, state, "pw'")

	SetCacheDir("")
	SetEncryptionPassword("")
	state = GetDebugState()
	assert.Contains(t, state, "cacheDir=''")
	assert.Contains(t, state, "hasEncryptionPassword=false")
}

func TestValidateAndCleanCacheEdgeCases(t *testing.T) {
	dir := withState(t, "pw", false)

	// No cache subdirectory yet.
	valid, stale := ValidateAndCleanCache()
	assert.Equal(t, 0, valid+stale)

	// Directories and non-.key files are left alone.
	sub := filepath.Join(dir, "scrypt_cache")
	require.NoError(t, os.MkdirAll(filepath.Join(sub, "nested.key"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "notes.txt"), []byte("x"), 0o600))
	valid, stale = ValidateAndCleanCache()
	assert.Equal(t, 0, valid+stale)
	assert.DirExists(t, filepath.Join(sub, "nested.key"))
	assert.FileExists(t, filepath.Join(sub, "notes.txt"))

	// No cache dir configured at all.
	SetCacheDir("")
	valid, stale = ValidateAndCleanCache()
	assert.Equal(t, 0, valid+stale)
}

func TestLoadAndSaveWithoutConfiguration(t *testing.T) {
	dir := withState(t, "", false)
	file := filepath.Join(dir, "scrypt_cache", "x.key")

	assert.Nil(t, loadCachedKey("", 32), "no cache path: nothing to load")
	assert.NoError(t, saveCachedKey("", []byte("k")), "no cache path: saving is a no-op")

	assert.Nil(t, loadCachedKey(file, 32), "no password: cannot load")
	assert.Error(t, saveCachedKey(file, []byte("k")), "no password: cannot save")
	assert.NoFileExists(t, file)

	SetEncryptionPassword("pw")
	assert.Nil(t, loadCachedKey(file, 32), "missing file")

	// With no cache dir there is no cache path at all.
	SetCacheDir("")
	assert.Equal(t, "", getCacheFilePath([]byte("p"), []byte("s"), 16, 1, 1, 32))
}

// Without a password the key is still derived and returned; it just is not
// cached.
func TestKeyWithoutPasswordStillDerives(t *testing.T) {
	dir := withState(t, "", false)
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.Len(t, key, 32)
	assert.Empty(t, cacheFiles(t, dir))
}

// A cache entry that decrypts but has the wrong length is ignored (and the
// key re-derived) rather than handed to the cipher.
func TestWrongLengthCacheEntryIsIgnored(t *testing.T) {
	withState(t, "pw", false)
	file := getCacheFilePath([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, saveCachedKey(file, []byte("too-short")))
	assert.Nil(t, loadCachedKey(file, 32))

	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.Len(t, key, 32)
	assert.Equal(t, key, loadCachedKey(file, 32), "re-derived key replaced the bad entry")
}

func TestSaveCachedKeyFailures(t *testing.T) {
	dir := withState(t, "pw", false)

	// The cache dir's parent is a file: MkdirAll fails.
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	err := saveCachedKey(filepath.Join(blocker, "scrypt_cache", "k.key"), []byte("k"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create cache dir")

	// The target path is a directory: WriteFile fails.
	target := filepath.Join(dir, "scrypt_cache", "k.key")
	require.NoError(t, os.MkdirAll(target, 0o700))
	err = saveCachedKey(target, []byte("k"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write cache file")

	// And Key survives a failed save.
	SetCacheDir(blocker)
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.Len(t, key, 32)
}

// macOS has no FPE memory ceiling, so slim mode there derives on a miss.
// The cache dir is a /Users/ path that cannot be created, so nothing is
// written anywhere; the save just fails and the key is still returned.
func TestSlimModeOnMacOSDerives(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX paths")
	}
	withState(t, "pw", true)
	SetCacheDir("/Users/.es-scryptcache-test-does-not-exist/container")
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.Len(t, key, 32)
}

func TestSlimModeMissErrorText(t *testing.T) {
	withState(t, "pw", true)
	_, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "SCRYPT_CACHE_MISS"), err.Error())
}

func TestKeyPropagatesBadParameters(t *testing.T) {
	withState(t, "pw", false)
	_, err := Key([]byte("p"), []byte("s"), 1000, 8, 1, 32)
	assert.Error(t, err)
}

// The password roll must take effect even when there is nothing on disk to
// re-encrypt; otherwise keys derived after the roll are sealed under the old
// password and the FPE, already on the new one, discards them as stale.
func TestReencryptWithNoCacheSubdirStillRollsPassword(t *testing.T) {
	dir := withState(t, "old", false)
	n, err := ReencryptAllCaches("old", "new")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, "new", GetEncryptionPassword())

	// A key cached now is readable by a process holding only the new password.
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	files := cacheFiles(t, dir)
	require.Len(t, files, 1)
	ct, err := os.ReadFile(files[0])
	require.NoError(t, err)
	pt, err := decryptData(ct, "new")
	require.NoError(t, err)
	assert.Equal(t, key, pt)
}

func TestReencryptSkipsUnreadableAndUnwritable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs POSIX permissions and a non-root user")
	}
	dir := withState(t, "old", false)
	sub := filepath.Join(dir, "scrypt_cache")
	require.NoError(t, os.MkdirAll(filepath.Join(sub, "dir.key"), 0o700))

	good, err := encryptData([]byte("0123456789abcdef0123456789abcdef"), "old")
	require.NoError(t, err)
	unreadable := filepath.Join(sub, "unreadable.key")
	require.NoError(t, os.WriteFile(unreadable, good, 0o600))
	require.NoError(t, os.Chmod(unreadable, 0o000))
	readOnly := filepath.Join(sub, "readonly.key")
	require.NoError(t, os.WriteFile(readOnly, good, 0o600))
	require.NoError(t, os.Chmod(readOnly, 0o400))
	t.Cleanup(func() {
		_ = os.Chmod(unreadable, 0o600)
		_ = os.Chmod(readOnly, 0o600)
	})

	n, err := ReencryptAllCaches("old", "new")
	require.NoError(t, err)
	assert.Equal(t, 0, n, "neither file could be re-encrypted")
	assert.FileExists(t, unreadable, "a file that could not be read is not deleted")
	assert.FileExists(t, readOnly)
	assert.Equal(t, "new", GetEncryptionPassword())
}

func TestReencryptUnreadableCacheDir(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs POSIX permissions and a non-root user")
	}
	dir := withState(t, "old", false)
	sub := filepath.Join(dir, "scrypt_cache")
	require.NoError(t, os.MkdirAll(sub, 0o700))
	require.NoError(t, os.Chmod(sub, 0o000))
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) })

	_, err := ReencryptAllCaches("old", "new")
	require.Error(t, err)

	// Validation tolerates the same unreadable directory.
	valid, stale := ValidateAndCleanCache()
	assert.Equal(t, 0, valid+stale)
}

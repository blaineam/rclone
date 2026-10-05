package scryptcache

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/scrypt"
)

// withState points the package at a fresh cache dir and password for one
// test and restores the previous globals afterwards.
func withState(t *testing.T, password string, slim bool) string {
	t.Helper()
	cacheDirMu.RLock()
	oldDir := cacheDir
	cacheDirMu.RUnlock()
	oldPassword := GetEncryptionPassword()
	oldSlim := isSlimBuild
	t.Cleanup(func() {
		SetCacheDir(oldDir)
		SetEncryptionPassword(oldPassword)
		isSlimBuild = oldSlim
	})
	// t.TempDir is outside /Users/, so slim mode behaves as it does on iOS.
	dir := t.TempDir()
	SetCacheDir(dir)
	SetEncryptionPassword(password)
	isSlimBuild = slim
	return dir
}

func cacheFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "scrypt_cache", "*.key"))
	require.NoError(t, err)
	return matches
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestScryptKeyRFC7914Vectors(t *testing.T) {
	got, err := scryptKey([]byte(""), []byte(""), 16, 1, 1, 64)
	require.NoError(t, err)
	assert.Equal(t, mustHex(t, "77d6576238657b203b19ca42c18a0497f16b4844e3074ae8dfdffa3fede21442fcd0069ded0948f8326a753a0fc81f17e8d3e0fb2e0d3628cf35e20c38d18906"), got)

	got, err = scryptKey([]byte("password"), []byte("NaCl"), 1024, 8, 16, 64)
	require.NoError(t, err)
	assert.Equal(t, mustHex(t, "fdbabe1c9d3472007856e7190d01e9fe7c6ad7cbc8237830e77376634b3731622eaf30d92e22a3886ff109279d9830dac727afb94a83ee6d8360cbdfa2cc0640"), got)
}

func TestScryptKeyMatchesXCrypto(t *testing.T) {
	// rclone's crypt backend parameters.
	want, err := scrypt.Key([]byte("hunter2"), []byte("saltsaltsaltsalt"), 16384, 8, 1, 80)
	require.NoError(t, err)
	got, err := scryptKey([]byte("hunter2"), []byte("saltsaltsaltsalt"), 16384, 8, 1, 80)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestScryptKeyRejectsBadParameters(t *testing.T) {
	_, err := scryptKey([]byte("p"), []byte("s"), 1000, 8, 1, 32)
	assert.Error(t, err, "N must be a power of two")
	_, err = scryptKey([]byte("p"), []byte("s"), 1, 8, 1, 32)
	assert.Error(t, err)
	_, err = scryptKey([]byte("p"), []byte("s"), 16, 1<<20, 1<<11, 32)
	assert.Error(t, err)
}

func TestKeyDerivesThenServesFromEncryptedCache(t *testing.T) {
	dir := withState(t, "app-password", false)
	pw, salt := []byte("remote-pass"), []byte("remote-salt")

	first, err := Key(pw, salt, 1024, 8, 1, 32)
	require.NoError(t, err)
	want, err := scrypt.Key(pw, salt, 1024, 8, 1, 32)
	require.NoError(t, err)
	assert.Equal(t, want, first)

	files := cacheFiles(t, dir)
	require.Len(t, files, 1)
	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	assert.False(t, bytes.Contains(raw, first), "derived key must not be stored in the clear")
	info, err := os.Stat(files[0])
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

	// Slim mode (FPE on iOS) cannot derive; it must get the cached key.
	isSlimBuild = true
	second, err := Key(pw, salt, 1024, 8, 1, 32)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestKeyCacheIsPerParameterSet(t *testing.T) {
	dir := withState(t, "app-password", false)
	a, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	b, err := Key([]byte("p"), []byte("s"), 32, 1, 1, 32)
	require.NoError(t, err)
	c, err := Key([]byte("p"), []byte("t"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.NotEqual(t, a, b)
	assert.NotEqual(t, a, c)
	assert.Len(t, cacheFiles(t, dir), 3)
}

func TestSlimModeMissReturnsCacheMissError(t *testing.T) {
	dir := withState(t, "app-password", true)
	_, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SCRYPT_CACHE_MISS")
	assert.Empty(t, cacheFiles(t, dir), "a miss must not write anything")
}

func TestWrongAppPasswordDropsStaleCacheAndRederives(t *testing.T) {
	dir := withState(t, "old-password", false)
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)

	SetEncryptionPassword("new-password")
	isSlimBuild = true
	_, err = Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.Error(t, err, "slim mode cannot use a cache sealed under another password")
	assert.Empty(t, cacheFiles(t, dir), "undecryptable cache file is removed")

	isSlimBuild = false
	again, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	assert.Equal(t, key, again)
	assert.Len(t, cacheFiles(t, dir), 1)
}

func TestReencryptAllCachesFollowsPasswordRoll(t *testing.T) {
	dir := withState(t, "pw-1", false)
	key, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	stale := filepath.Join(dir, "scrypt_cache", "stale.key")
	require.NoError(t, os.WriteFile(stale, []byte("garbage-not-a-sealed-key"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "scrypt_cache", "ignored.txt"), []byte("x"), 0600))

	n, err := ReencryptAllCaches("pw-1", "pw-2")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "pw-2", GetEncryptionPassword())
	assert.NoFileExists(t, stale)
	assert.FileExists(t, filepath.Join(dir, "scrypt_cache", "ignored.txt"))

	isSlimBuild = true
	got, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err, "re-encrypted cache must open under the new password")
	assert.Equal(t, key, got)
}

func TestReencryptWithoutCacheDir(t *testing.T) {
	withState(t, "pw", false)
	SetCacheDir("")
	_, err := ReencryptAllCaches("a", "b")
	assert.Error(t, err)
}

func TestValidateAndCleanCache(t *testing.T) {
	dir := withState(t, "pw", false)
	_, err := Key([]byte("p"), []byte("s"), 16, 1, 1, 32)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "scrypt_cache", "bad.key"), []byte("short"), 0600))

	valid, stale := ValidateAndCleanCache()
	assert.Equal(t, 1, valid)
	assert.Equal(t, 1, stale)
	assert.Len(t, cacheFiles(t, dir), 1)

	SetEncryptionPassword("")
	valid, stale = ValidateAndCleanCache()
	assert.Equal(t, 0, valid+stale, "without a password nothing is judged or deleted")
	assert.Len(t, cacheFiles(t, dir), 1)
}

func TestEncryptDecryptRoundTripAndTamper(t *testing.T) {
	ct, err := encryptData([]byte("secret-key-bytes"), "pw")
	require.NoError(t, err)
	pt, err := decryptData(ct, "pw")
	require.NoError(t, err)
	assert.Equal(t, []byte("secret-key-bytes"), pt)

	_, err = decryptData(ct, "other")
	assert.Error(t, err)
	ct[len(ct)-1] ^= 1
	_, err = decryptData(ct, "pw")
	assert.Error(t, err)
	_, err = decryptData([]byte{1, 2}, "pw")
	assert.Error(t, err)
}

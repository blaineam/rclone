// Package scryptcache wraps golang.org/x/crypto/scrypt with disk-based key caching.
// This is critical for the File Provider Extension (FPE) which has a 20MB memory limit,
// while scrypt with rclone's default parameters requires 32MB.
//
// Security: Cached keys are stored in the App Group container, which is sandboxed
// to this app only. The iOS sandbox is the security boundary.
//
// Usage:
// - Main app (full build): Derives keys normally and caches them to disk
// - FPE (slim build): Loads cached keys from disk, avoiding scrypt memory spike
package scryptcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

var (
	// cacheDir is the directory where cached keys are stored
	// This should be set to the App Group container path
	cacheDir   string
	cacheDirMu sync.RWMutex

	// isSlimBuild indicates if this is the FPE slim build (load-only mode)
	isSlimBuild bool
)

// SetCacheDir sets the directory for key caching.
// This should be called during initialization with the App Group container path.
func SetCacheDir(dir string) {
	cacheDirMu.Lock()
	defer cacheDirMu.Unlock()
	cacheDir = dir
}

// SetEncryptionPassword is kept for API compatibility but no longer used.
// Keys are stored unencrypted since the App Group sandbox provides security.
func SetEncryptionPassword(password string) {
	// No-op - encryption removed, App Group sandbox provides security
}

// SetSlimBuild marks this as the FPE slim build (load-only mode).
// When true, Key() will only load cached keys and never derive new ones.
func SetSlimBuild(slim bool) {
	isSlimBuild = slim
}

// GetDebugState returns the current scrypt cache state as a debug string.
func GetDebugState() string {
	cacheDirMu.RLock()
	currentDir := cacheDir
	cacheDirMu.RUnlock()

	cacheSubDir := filepath.Join(currentDir, "scrypt_cache")
	cacheDirExists := false
	var cacheFiles []string
	if currentDir != "" {
		if info, err := os.Stat(cacheSubDir); err == nil && info.IsDir() {
			cacheDirExists = true
			if files, err := os.ReadDir(cacheSubDir); err == nil {
				for _, f := range files {
					cacheFiles = append(cacheFiles, f.Name())
				}
			}
		}
	}

	return fmt.Sprintf(
		"ScryptCache State: cacheDir='%s', isSlimBuild=%v, cacheDirExists=%v, cacheFiles=%v",
		currentDir, isSlimBuild, cacheDirExists, cacheFiles)
}

// getCacheFilePath returns the path to the cache file for given scrypt parameters.
// The filename is a hash of the password + salt + params to ensure uniqueness.
func getCacheFilePath(password, salt []byte, N, r, p, keyLen int) string {
	cacheDirMu.RLock()
	defer cacheDirMu.RUnlock()

	if cacheDir == "" {
		return ""
	}

	// Create a unique identifier from all inputs
	h := sha256.New()
	h.Write(password)
	h.Write(salt)
	h.Write([]byte(fmt.Sprintf("%d:%d:%d:%d", N, r, p, keyLen)))
	hash := hex.EncodeToString(h.Sum(nil))[:32]

	return filepath.Join(cacheDir, "scrypt_cache", hash+".key")
}

// loadCachedKey attempts to load a cached key from disk.
// Returns nil if no cache exists or on error.
func loadCachedKey(cacheFile string, expectedLen int) []byte {
	if cacheFile == "" {
		return nil
	}

	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return nil
	}

	// Verify length matches expected
	if len(data) != expectedLen {
		return nil
	}

	return data
}

// saveCachedKey saves a derived key to disk for future use.
func saveCachedKey(cacheFile string, key []byte) error {
	if cacheFile == "" {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(cacheFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create cache dir: %w", err)
	}

	// Write key to file with restricted permissions
	if err := os.WriteFile(cacheFile, key, 0600); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	return nil
}

// Key derives a key from the password, salt, and cost parameters, returning
// a byte slice of length keyLen that can be used as cryptographic key.
//
// This is a drop-in replacement for golang.org/x/crypto/scrypt.Key that adds
// disk-based caching to avoid repeated expensive key derivation.
//
// In slim build mode (FPE), this will ONLY load cached keys and return an error
// if no cache exists, preventing the 32MB scrypt memory spike that would crash
// the FPE (which has a 20MB limit).
func Key(password, salt []byte, N, r, p, keyLen int) ([]byte, error) {
	cacheFile := getCacheFilePath(password, salt, N, r, p, keyLen)

	// Try to load from cache first
	if cached := loadCachedKey(cacheFile, keyLen); cached != nil {
		return cached, nil
	}

	// No cached key found
	if isSlimBuild {
		// In slim build (FPE), we can't derive - it would crash due to memory
		return nil, fmt.Errorf("SCRYPT_CACHE_MISS: Encryption key not cached. Please open Enter Space app first to initialize this encrypted remote")
	}

	// Full build - derive the key using native scrypt implementation
	key, err := scryptKey(password, salt, N, r, p, keyLen)
	if err != nil {
		return nil, err
	}

	// Cache the derived key for future use (especially by FPE)
	_ = saveCachedKey(cacheFile, key)

	return key, nil
}

// scryptKey is our local implementation of scrypt key derivation.
// This is a copy of the algorithm to avoid circular import issues.
// Based on Colin Percival's scrypt paper and golang.org/x/crypto/scrypt.
func scryptKey(password, salt []byte, N, r, p, keyLen int) ([]byte, error) {
	if N <= 1 || N&(N-1) != 0 {
		return nil, fmt.Errorf("scrypt: N must be > 1 and a power of 2")
	}
	if uint64(r)*uint64(p) >= 1<<30 || r > maxInt/128/p || r > maxInt/256 || N > maxInt/128/r {
		return nil, fmt.Errorf("scrypt: parameters are too large")
	}

	xy := make([]uint32, 64*r)
	v := make([]uint32, 32*N*r)
	b := pbkdf2.Key(password, salt, 1, p*128*r, sha256.New)

	for i := 0; i < p; i++ {
		smix(b[i*128*r:], r, N, v, xy)
	}

	return pbkdf2.Key(password, b, 1, keyLen, sha256.New), nil
}

const maxInt = int(^uint(0) >> 1)

func smix(b []byte, r, N int, v, xy []uint32) {
	var tmp [16]uint32
	R := 32 * r
	x := xy
	y := xy[R:]

	j := 0
	for i := 0; i < R; i++ {
		x[i] = uint32(b[j]) | uint32(b[j+1])<<8 | uint32(b[j+2])<<16 | uint32(b[j+3])<<24
		j += 4
	}
	for i := 0; i < N; i += 2 {
		copy(v[i*R:], x)
		blockMix(&tmp, x, y, r)
		copy(v[(i+1)*R:], y)
		blockMix(&tmp, y, x, r)
	}
	for i := 0; i < N; i += 2 {
		j := int(x[R-16]) & (N - 1)
		for k := 0; k < R; k++ {
			x[k] ^= v[j*R+k]
		}
		blockMix(&tmp, x, y, r)
		j = int(y[R-16]) & (N - 1)
		for k := 0; k < R; k++ {
			y[k] ^= v[j*R+k]
		}
		blockMix(&tmp, y, x, r)
	}
	j = 0
	for _, xi := range x[:R] {
		b[j] = byte(xi)
		b[j+1] = byte(xi >> 8)
		b[j+2] = byte(xi >> 16)
		b[j+3] = byte(xi >> 24)
		j += 4
	}
}

func blockMix(tmp *[16]uint32, in, out []uint32, r int) {
	copy(tmp[:], in[(2*r-1)*16:])
	for i := 0; i < 2*r; i += 2 {
		salsaXOR(tmp, in[i*16:], out[i*8:])
		salsaXOR(tmp, in[i*16+16:], out[i*8+r*16:])
	}
}

func salsaXOR(tmp *[16]uint32, in, out []uint32) {
	w0 := tmp[0] ^ in[0]
	w1 := tmp[1] ^ in[1]
	w2 := tmp[2] ^ in[2]
	w3 := tmp[3] ^ in[3]
	w4 := tmp[4] ^ in[4]
	w5 := tmp[5] ^ in[5]
	w6 := tmp[6] ^ in[6]
	w7 := tmp[7] ^ in[7]
	w8 := tmp[8] ^ in[8]
	w9 := tmp[9] ^ in[9]
	w10 := tmp[10] ^ in[10]
	w11 := tmp[11] ^ in[11]
	w12 := tmp[12] ^ in[12]
	w13 := tmp[13] ^ in[13]
	w14 := tmp[14] ^ in[14]
	w15 := tmp[15] ^ in[15]

	x0, x1, x2, x3, x4, x5, x6, x7, x8 := w0, w1, w2, w3, w4, w5, w6, w7, w8
	x9, x10, x11, x12, x13, x14, x15 := w9, w10, w11, w12, w13, w14, w15

	for i := 0; i < 8; i += 2 {
		x4 ^= bits32RotateLeft(x0+x12, 7)
		x8 ^= bits32RotateLeft(x4+x0, 9)
		x12 ^= bits32RotateLeft(x8+x4, 13)
		x0 ^= bits32RotateLeft(x12+x8, 18)

		x9 ^= bits32RotateLeft(x5+x1, 7)
		x13 ^= bits32RotateLeft(x9+x5, 9)
		x1 ^= bits32RotateLeft(x13+x9, 13)
		x5 ^= bits32RotateLeft(x1+x13, 18)

		x14 ^= bits32RotateLeft(x10+x6, 7)
		x2 ^= bits32RotateLeft(x14+x10, 9)
		x6 ^= bits32RotateLeft(x2+x14, 13)
		x10 ^= bits32RotateLeft(x6+x2, 18)

		x3 ^= bits32RotateLeft(x15+x11, 7)
		x7 ^= bits32RotateLeft(x3+x15, 9)
		x11 ^= bits32RotateLeft(x7+x3, 13)
		x15 ^= bits32RotateLeft(x11+x7, 18)

		x1 ^= bits32RotateLeft(x0+x3, 7)
		x2 ^= bits32RotateLeft(x1+x0, 9)
		x3 ^= bits32RotateLeft(x2+x1, 13)
		x0 ^= bits32RotateLeft(x3+x2, 18)

		x6 ^= bits32RotateLeft(x5+x4, 7)
		x7 ^= bits32RotateLeft(x6+x5, 9)
		x4 ^= bits32RotateLeft(x7+x6, 13)
		x5 ^= bits32RotateLeft(x4+x7, 18)

		x11 ^= bits32RotateLeft(x10+x9, 7)
		x8 ^= bits32RotateLeft(x11+x10, 9)
		x9 ^= bits32RotateLeft(x8+x11, 13)
		x10 ^= bits32RotateLeft(x9+x8, 18)

		x12 ^= bits32RotateLeft(x15+x14, 7)
		x13 ^= bits32RotateLeft(x12+x15, 9)
		x14 ^= bits32RotateLeft(x13+x12, 13)
		x15 ^= bits32RotateLeft(x14+x13, 18)
	}

	out[0] = w0 + x0
	out[1] = w1 + x1
	out[2] = w2 + x2
	out[3] = w3 + x3
	out[4] = w4 + x4
	out[5] = w5 + x5
	out[6] = w6 + x6
	out[7] = w7 + x7
	out[8] = w8 + x8
	out[9] = w9 + x9
	out[10] = w10 + x10
	out[11] = w11 + x11
	out[12] = w12 + x12
	out[13] = w13 + x13
	out[14] = w14 + x14
	out[15] = w15 + x15

	copy(tmp[:], out[:16])
}

func bits32RotateLeft(x uint32, n int) uint32 {
	return (x << n) | (x >> (32 - n))
}

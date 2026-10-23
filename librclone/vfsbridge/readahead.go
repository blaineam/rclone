package vfsbridge

import (
	"sync"

	"github.com/rclone/rclone/fs"
)

// Per-handle adaptive read-ahead.
//
// With CacheModeFull every read is served from the VFS cache, and the cache
// downloader fetches ReadAhead bytes PAST each read before it stops. One fixed
// value cannot suit every workload this volume sees:
//
//   - Streaming. A sequential read of a crypt-wrapped remote ran at 2.0 MB/s
//     with 128KiB of read-ahead and 10.0 MB/s with 128MiB (see docs/FSKIT.md
//     §15): on a high-latency backend the downloader stalls at the end of
//     its window and every restart costs a round trip, so the window has to
//     be large to keep the stream moving.
//   - Probing. Sami opens hundreds of films on the volume and reads a few
//     KiB of header from each. With 128MiB of read-ahead every one of those
//     probes started a 128MiB download, hundreds of them at once, and the
//     reads behind them waited past BridgeClient's 10s timeout.
//   - Parsing. AVFoundation reads a film's metadata as a few hundred small
//     reads walking through the first 15 MiB or so. Grown by read count that
//     looked like a stream, and the window hit the ceiling before the parse
//     was a megabyte in: every film loaded cost 128MiB of download for
//     15MiB of use, which over a 10 MB/s link is the hour it took Sami to
//     load 227 of them (docs/FSKIT.md §25).
//   - Seeking in bulk. An export reads a whole film with its audio and video
//     cursors leapfrogging. Each cursor's reads look like seeks to the other,
//     yet the handle reads everything.
//
// So the read-ahead is sized by what the handle has consumed, not by how
// its reads were shaped: half the bytes read through it so far, between
// readAheadMin and readAheadMax. A probe costs the probe plus readAheadMin.
// A parse that reads 15 MiB has fetched at most 7.5 MiB past what it used.
// A stream's window doubles with every doubling of what it has read and
// reaches the ceiling by 256 MiB. A reader that seeks keeps the window its
// consumption has earned, because the window never depended on the seek.
const (
	// Read-ahead floor: the first read on a handle gets this much.
	readAheadMin = 2 * fs.Mebi
	// Ceiling, reached once the handle has read twice this much.
	readAheadMax = 128 * fs.Mebi
	// The read-ahead is the bytes consumed so far divided by this.
	readAheadConsumedDivisor = 2
)

// readAhead is the per-handle consumption tracker.
type readAhead struct {
	mu        sync.Mutex
	totalRead int64 // bytes read through this handle
}

// next records a read of n bytes at off and returns the read-ahead that
// read should use.
func (p *readAhead) next(off, n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.totalRead += n
	ra := p.totalRead / readAheadConsumedDivisor
	return min(max(ra, int64(readAheadMin)), int64(readAheadMax))
}

// readAheadSetter is implemented by vfs.RWFileHandle (fork addition). Handles
// of a CacheModeOff remote do not implement it and get no read-ahead at all.
type readAheadSetter interface {
	SetReadAhead(n int64)
}

// cachedBufferSize is the in-memory buffer between the backend and the cache
// file on a cached remote (rclone's --buffer-size, 16MiB by default). It is
// not the read-ahead: the downloader writes to the cache file up to its
// read-ahead and then blocks, but the reader behind it keeps pulling from the
// backend until this buffer is full. So it is part of what a probe costs, and
// it is kept small. Streaming does not need it big, because the downloader
// drains it to disk far faster than any backend fills it.
var cachedBufferSize = fs.SizeSuffix(2 * fs.Mebi)

// fastFingerprint is whether cached files are fingerprinted by size and
// modtime only (true) or also by a hash the backend computes (false,
// rclone's default). See AddRemote. Only a test sets it to false, to show
// what the hash costs.
var fastFingerprint = true

// readAheadAdaptive switches the per-read policy off, so every read uses
// readAheadMax exactly as the fixed 128MiB setting did. Only the measurement
// test flips it, to produce the "before" numbers in the same binary.
var readAheadAdaptive = true

package vfsbridge

import (
	"sync"

	"github.com/rclone/rclone/fs"
)

// Per-handle adaptive read-ahead.
//
// With CacheModeFull every read is served from the VFS cache, and the cache
// downloader fetches ReadAhead bytes PAST each read before it stops. One fixed
// value cannot suit both workloads this volume sees:
//
//   - Streaming. A sequential read of a crypt-wrapped remote ran at 2.0 MB/s
//     with 128KiB of read-ahead and 10.0 MB/s with 128MiB (see docs/FSKIT.md
//     §15): every read must land in cache that is already fetched, or it
//     waits on its own backend round trip.
//   - Probing. Sami opens hundreds of films on the volume and reads a few
//     KiB of header from each. With 128MiB of read-ahead every one of those
//     probes started a 128MiB download, hundreds of them at once, and the
//     reads behind them waited past BridgeClient's 10s timeout.
//
// So the read-ahead is decided per read, from what the handle has done so
// far: a fresh handle, or one that just seeked, gets readAheadMin; each run
// of readAheadStep sequential reads multiplies it by four, up to
// readAheadMax. A probe costs the probe plus readAheadMin; a stream reaches
// the full window within its first few MiB.
//
// Sequential means the read starts within readAheadNear of where the
// previous one ended, in either direction. The kernel issues a stream's
// reads slightly out of order and sometimes re-reads a page, and neither is
// a seek.
const (
	// Read-ahead for the first read on a handle and after a seek.
	readAheadMin = 2 * fs.Mebi
	// Ceiling, reached after 3 growth steps: 2 → 8 → 32 → 128 MiB.
	readAheadMax = 128 * fs.Mebi
	// Sequential reads per growth step.
	readAheadStep = 4
	// A read starting within this distance of the previous read's end, in
	// either direction, continues the sequential run.
	readAheadNear = int64(4 * fs.Mebi)
)

// readAhead is the per-handle access-pattern tracker.
type readAhead struct {
	mu      sync.Mutex
	started bool
	lastEnd int64 // furthest byte the sequential run has reached
	seq     int   // consecutive sequential reads so far
}

// next records a read of n bytes at off and returns the read-ahead that
// read should use.
func (p *readAhead) next(off, n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started && off >= p.lastEnd-readAheadNear && off <= p.lastEnd+readAheadNear {
		p.seq++
		p.lastEnd = max(p.lastEnd, off+n)
	} else {
		p.seq = 0
		p.lastEnd = off + n
	}
	p.started = true
	ra := int64(readAheadMin)
	for s := p.seq; s >= readAheadStep && ra < int64(readAheadMax); s -= readAheadStep {
		ra *= 4
	}
	return min(ra, int64(readAheadMax))
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

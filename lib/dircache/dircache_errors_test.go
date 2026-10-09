package dircache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/unicode/norm"
)

// FlushDir is keyed the same way as the cache: an NFD path flushes the
// entries an NFC path stored, and their subdirectories, and nothing else.
func TestFlushDirNormalizationInsensitive(t *testing.T) {
	m := newMockDirCacher(t)
	dc := New("", "root", m)
	dc.Put(nfcName, "id-cafe")
	dc.Put(nfcName+"/"+nfcSub, "id-sub")
	dc.Put(plain, "id-plain")

	dc.FlushDir(nfdName)

	_, ok := dc.Get(nfcName)
	assert.False(t, ok, "the directory itself was not flushed")
	_, ok = dc.Get(nfcName + "/" + nfcSub)
	assert.False(t, ok, "its subdirectory was not flushed")
	_, ok = dc.GetInv("id-sub")
	assert.False(t, ok, "inverse entry left behind")
	id, ok := dc.Get(plain)
	assert.True(t, ok, "an unrelated directory was flushed")
	assert.Equal(t, "id-plain", id)

	// Flushing something not cached is harmless.
	dc.FlushDir("never-cached")
	_, ok = dc.Get(plain)
	assert.True(t, ok)

	// Flushing "" resets everything to the root.
	dc.FlushDir("")
	_, ok = dc.Get(plain)
	assert.False(t, ok)
}

// errDirCacher fails FindLeaf for chosen leaves and CreateDir on demand.
type errDirCacher struct {
	failLeaf   map[string]bool
	failCreate bool
	found      map[string]string // leaf -> id, byte-wise
}

var errBackend = errors.New("backend failure")

func (e *errDirCacher) FindLeaf(ctx context.Context, pathID, leaf string) (string, bool, error) {
	if e.failLeaf[leaf] {
		return "", false, errBackend
	}
	id, ok := e.found[leaf]
	return id, ok, nil
}

func (e *errDirCacher) CreateDir(ctx context.Context, pathID, leaf string) (string, error) {
	if e.failCreate {
		return "", errBackend
	}
	return "new-" + leaf, nil
}

func TestFindDirPropagatesErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("FindLeaf fails", func(t *testing.T) {
		dc := New("", "root", &errDirCacher{failLeaf: map[string]bool{"a": true}})
		_, err := dc.FindDir(ctx, "a", false)
		assert.ErrorIs(t, err, errBackend)
	})

	t.Run("parent lookup fails", func(t *testing.T) {
		dc := New("", "root", &errDirCacher{failLeaf: map[string]bool{"a": true}})
		_, err := dc.FindDir(ctx, "a/b/c", false)
		assert.ErrorIs(t, err, errBackend)
	})

	t.Run("alternate normalization lookup fails", func(t *testing.T) {
		// The verbatim NFD lookup misses; the NFC retry errors.
		dc := New("", "root", &errDirCacher{failLeaf: map[string]bool{norm.NFC.String(nfdName): true}})
		_, err := dc.FindDir(ctx, nfdName, false)
		assert.ErrorIs(t, err, errBackend)
	})

	t.Run("CreateDir fails", func(t *testing.T) {
		dc := New("", "root", &errDirCacher{failCreate: true})
		_, err := dc.FindDir(ctx, "new", true)
		require.Error(t, err)
		assert.ErrorIs(t, err, errBackend)
		assert.Contains(t, err.Error(), "failed to make directory")
	})

	t.Run("create succeeds and is cached", func(t *testing.T) {
		dc := New("", "root", &errDirCacher{})
		id, err := dc.FindDir(ctx, "x/y", true)
		require.NoError(t, err)
		assert.Equal(t, "new-y", id)
		got, ok := dc.Get("x")
		assert.True(t, ok)
		assert.Equal(t, "new-x", got)
	})
}

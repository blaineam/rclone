package dircache

import (
	"context"
	"fmt"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nfcName = "caf\u00e9"    // café precomposed (NFC) - as most servers store it
	nfdName = "cafe\u0301"   // café decomposed (NFD) - as Apple clients send it
	nfcSub  = "s\u00e9ance"  // NFC subdirectory
	nfdSub  = "se\u0301ance" // NFD subdirectory
	plain   = "plain"        // no accents
	missing = "not-here"     // doesn't exist
)

// mockDirCacher is a DirCacher which matches names BYTE-WISE against
// the server-stored names, emulating a naive backend FindLeaf talking
// to a server which stores one particular Unicode normalization form.
type mockDirCacher struct {
	t        *testing.T
	nextID   int
	children map[string]map[string]string // parentID -> serverName -> id
	creates  int                          // number of CreateDir calls
}

func newMockDirCacher(t *testing.T) *mockDirCacher {
	return &mockDirCacher{
		t:        t,
		children: map[string]map[string]string{"root": {}},
	}
}

// mkdir inserts a directory server-side without going through CreateDir
func (m *mockDirCacher) mkdir(parentID, serverName string) string {
	m.nextID++
	id := fmt.Sprintf("id-%d", m.nextID)
	if m.children[parentID] == nil {
		m.children[parentID] = map[string]string{}
	}
	m.children[parentID][serverName] = id
	m.children[id] = map[string]string{}
	return id
}

// FindLeaf does a deliberately byte-wise comparison
func (m *mockDirCacher) FindLeaf(ctx context.Context, pathID, leaf string) (string, bool, error) {
	for serverName, id := range m.children[pathID] {
		if serverName == leaf {
			return id, true, nil
		}
	}
	return "", false, nil
}

func (m *mockDirCacher) CreateDir(ctx context.Context, pathID, leaf string) (string, error) {
	m.creates++
	return m.mkdir(pathID, leaf), nil
}

func TestFindDirUnicodeNormalization(t *testing.T) {
	for _, test := range []struct {
		name       string
		serverName string // form stored on the server
		queryName  string // form the client asks for
		wantFound  bool
	}{
		{"NFDQueryFindsNFCServer", nfcName, nfdName, true},
		{"NFCQueryFindsNFDServer", nfdName, nfcName, true},
		{"NFCQueryFindsNFCServer", nfcName, nfcName, true},
		{"NFDQueryFindsNFDServer", nfdName, nfdName, true},
		{"PlainUnaffected", plain, plain, true},
		{"MissingStillMissing", nfcName, missing, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			m := newMockDirCacher(t)
			wantID := m.mkdir("root", test.serverName)
			dc := New("", "root", m)

			id, err := dc.FindDir(ctx, test.queryName, false)
			if test.wantFound {
				require.NoError(t, err)
				assert.Equal(t, wantID, id)
			} else {
				assert.Equal(t, fs.ErrorDirNotFound, err)
			}
		})
	}
}

func TestFindDirUnicodeNormalizationNested(t *testing.T) {
	ctx := context.Background()
	m := newMockDirCacher(t)
	parentID := m.mkdir("root", nfcName)
	subID := m.mkdir(parentID, nfcSub)
	dc := New("", "root", m)

	// Mixed-form nested path: NFD parent / NFD sub against NFC server
	id, err := dc.FindDir(ctx, nfdName+"/"+nfdSub, false)
	require.NoError(t, err)
	assert.Equal(t, subID, id)
}

func TestFindDirNoDuplicateCreate(t *testing.T) {
	ctx := context.Background()
	m := newMockDirCacher(t)
	wantID := m.mkdir("root", nfcName)
	dc := New("", "root", m)

	// Mkdir (create=true) of the NFD form of an existing NFC
	// directory must reuse it, not create an NFC/NFD twin
	id, err := dc.FindDir(ctx, nfdName, true)
	require.NoError(t, err)
	assert.Equal(t, wantID, id)
	assert.Equal(t, 0, m.creates, "created a duplicate directory")

	// But a genuinely missing directory is still created
	id, err = dc.FindDir(ctx, missing, true)
	require.NoError(t, err)
	assert.NotEqual(t, wantID, id)
	assert.Equal(t, 1, m.creates)
}

func TestFindDirNoUnicodeNormalizationFlag(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.NoUnicodeNormalization = true
	m := newMockDirCacher(t)
	m.mkdir("root", nfcName)
	dc := New("", "root", m)

	// With --no-unicode-normalization the fallback is disabled
	_, err := dc.FindDir(ctx, nfdName, false)
	assert.Equal(t, fs.ErrorDirNotFound, err)
}

func TestCacheKeyNormalization(t *testing.T) {
	m := newMockDirCacher(t)
	dc := New("", "root", m)

	// A path Put in NFC (eg from a List which caches server names)
	// must be found when Get with the NFD form and vice versa
	dc.Put(nfcName, "id-nfc")
	id, ok := dc.Get(nfdName)
	assert.True(t, ok)
	assert.Equal(t, "id-nfc", id)

	// The inverse cache preserves the original bytes
	path, ok := dc.GetInv("id-nfc")
	assert.True(t, ok)
	assert.Equal(t, nfcName, path)

	// FlushDir with the alternate form flushes the entry and children
	dc.Put(nfcName+"/"+nfcSub, "id-sub")
	dc.FlushDir(nfdName)
	_, ok = dc.Get(nfcName)
	assert.False(t, ok)
	_, ok = dc.Get(nfcName + "/" + nfcSub)
	assert.False(t, ok)
}

func TestNameEqual(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want bool
	}{
		{nfcName, nfdName, true},
		{nfdName, nfcName, true},
		{nfcName, nfcName, true},
		{plain, plain, true},
		{nfcName, plain, false},
		{"", "", true},
		{"Caf\u00e9", nfdName, false}, // still case-sensitive
	} {
		assert.Equal(t, test.want, NameEqual(test.a, test.b), "NameEqual(%q, %q)", test.a, test.b)
	}
}

func TestSplitPath(t *testing.T) {
	for _, test := range []struct {
		path, wantDir, wantLeaf string
	}{
		{"", "", ""},
		{"a", "", "a"},
		{"a/b", "a", "b"},
		{"a/b/c", "a/b", "c"},
	} {
		gotDir, gotLeaf := SplitPath(test.path)
		assert.Equal(t, test.wantDir, gotDir)
		assert.Equal(t, test.wantLeaf, gotLeaf)
	}
}

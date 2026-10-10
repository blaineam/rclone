package internxt

import (
	"context"
	"testing"

	"github.com/rclone/rclone/lib/dircache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Enter Space fork: Unicode normalization insensitive name matching.
// Kept in its own file so upstream changes to internxt_internal_test.go
// merge without conflicts.

const (
	nfc = "café"  // precomposed, as the Internxt API stores names
	nfd = "café" // decomposed, as Apple clients send names
)

// The FindLeaf name matching relies on dircache.NameEqual being Unicode
// normalization insensitive so an NFD path from an Apple client resolves
// against NFC server names
func TestNameMatchingNormalizationInsensitive(t *testing.T) {
	assert.True(t, dircache.NameEqual(nfc, nfd))
	assert.True(t, dircache.NameEqual(nfd, nfc))
	assert.True(t, dircache.NameEqual("plain", "plain"))
	assert.False(t, dircache.NameEqual(nfc, "other"))
}

func TestNormVariants(t *testing.T) {
	for _, test := range []struct {
		name string
		want []string
	}{
		{"plain", []string{"plain"}},
		{nfc, []string{nfc, nfd}},
		{nfd, []string{nfd, nfc}},
	} {
		assert.Equal(t, test.want, normVariants(test.name), "normVariants(%q)", test.name)
	}
}

// findFile must resolve a name sent in the other normalization form than
// the one the server stored, or NewObject reports the file missing and an
// upload creates an NFC/NFD twin.
func TestFindFileNormalizationInsensitive(t *testing.T) {
	for _, test := range []struct {
		what   string
		stored string
		leaf   string
	}{
		{"NFD leaf, NFC stored", nfc, nfd + ".txt"},
		{"NFC leaf, NFD stored", nfd, nfc + ".txt"},
		{"same form", nfc, nfc + ".txt"},
	} {
		t.Run(test.what, func(t *testing.T) {
			server := existenceServer(t, []storedFile{{"uuid-n", test.stored, "txt"}})
			defer server.Close()
			file, err := testFs(t, server).findFile(context.Background(), test.leaf, "dir-uuid")
			require.NoError(t, err)
			require.NotNil(t, file)
			assert.Equal(t, "uuid-n", file.UUID)
		})
	}
}

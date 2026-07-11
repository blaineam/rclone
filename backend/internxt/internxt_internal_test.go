package internxt

import (
	"testing"

	"github.com/rclone/rclone/lib/dircache"
	"github.com/stretchr/testify/assert"
)

const (
	nfc = "caf\u00e9"  // precomposed, as the Internxt API stores names
	nfd = "cafe\u0301" // decomposed, as Apple clients send names
)

// The FindLeaf and NewObject name matching relies on
// dircache.NameEqual being Unicode normalization insensitive so an
// NFD path from an Apple client resolves against NFC server names
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

package fs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Enter Space ships release builds of this fork, and the app shows
// fs.Version to users (rcVersion / "About"). Upstream's tree carries
// VersionSuffix = "DEV", which an upstream merge can quietly restore; this
// pins the fork's value so that shows up as a test failure, not as
// "v1.75.1-DEV" in a shipped app.
func TestEnterSpaceVersionHasNoDevSuffix(t *testing.T) {
	assert.Equal(t, "", VersionSuffix, "Enter Space release builds carry no pre-release label")
	assert.Equal(t, VersionTag, Version, "with no suffix, Version is exactly the tag")
	assert.False(t, strings.Contains(Version, "-DEV"), "Version %q has a -DEV suffix", Version)
	assert.True(t, strings.HasPrefix(Version, "v"), "Version %q", Version)
}

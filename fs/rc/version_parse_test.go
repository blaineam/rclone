package rc

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// core/version must answer whatever fs.Version holds. Upstream failed the
// whole call when the version did not parse as semver; the fork reports
// 0.0.0 for "decomposed" instead, because a custom build string is not a
// reason to break the app's version screen.
func TestRcVersionParsing(t *testing.T) {
	old := fs.Version
	t.Cleanup(func() { fs.Version = old })

	for _, tc := range []struct {
		version    string
		decomposed []int64
		isBeta     bool
		isGit      bool
	}{
		{"v1.75.1", []int64{1, 75, 1}, false, false},
		{"v1.76.0-beta.9000.abcdef", []int64{1, 76, 0}, true, false},
		{"v1.76.0-DEV", []int64{1, 76, 0}, true, true},
		{"vnot.a.version", []int64{0, 0, 0}, false, false},
		{"enterspace-custom", []int64{0, 0, 0}, false, false},
		{"", []int64{0, 0, 0}, false, false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			fs.Version = tc.version
			out, err := rcVersion(context.Background(), Params{})
			require.NoError(t, err)
			assert.Equal(t, tc.version, out["version"])
			assert.Equal(t, tc.decomposed, out["decomposed"])
			assert.Equal(t, tc.isBeta, out["isBeta"])
			assert.Equal(t, tc.isGit, out["isGit"])
			for _, k := range []string{"os", "osVersion", "osKernel", "arch", "goVersion"} {
				assert.NotEmpty(t, out[k], k)
			}
		})
	}
}

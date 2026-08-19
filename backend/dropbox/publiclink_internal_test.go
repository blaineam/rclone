package dropbox

import (
	"context"
	"testing"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/sharing"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sharingStub answers just the two sharing calls PublicLink makes. Everything
// else on the interface is left nil - reaching it is a test bug, and a panic
// says so loudly.
type sharingStub struct {
	sharing.Client

	// one entry per CreateSharedLinkWithSettings call, nil meaning "succeed"
	createErrs []error
	// the settings each call was made with, snapshotted because PublicLink
	// edits the same struct in place between attempts
	createCalls []*sharing.SharedLinkSettings

	listCalls int
	listLinks []sharing.IsSharedLinkMetadata
}

func (s *sharingStub) CreateSharedLinkWithSettings(arg *sharing.CreateSharedLinkWithSettingsArg) (sharing.IsSharedLinkMetadata, error) {
	var snapshot *sharing.SharedLinkSettings
	if arg.Settings != nil {
		copied := *arg.Settings
		snapshot = &copied
	}
	s.createCalls = append(s.createCalls, snapshot)
	if i := len(s.createCalls) - 1; i < len(s.createErrs) && s.createErrs[i] != nil {
		return nil, s.createErrs[i]
	}
	return &sharing.FileLinkMetadata{
		SharedLinkMetadata: sharing.SharedLinkMetadata{Url: "https://www.dropbox.com/s/created/file?dl=0"},
	}, nil
}

func (s *sharingStub) ListSharedLinks(arg *sharing.ListSharedLinksArg) (*sharing.ListSharedLinksResult, error) {
	s.listCalls++
	return &sharing.ListSharedLinksResult{Links: s.listLinks}, nil
}

// apiErr builds an error shaped like the SDK's, whose message is the
// error_summary Dropbox sends back.
func apiErr(summary string) error {
	return sharing.CreateSharedLinkWithSettingsAPIError{
		APIError: dropbox.APIError{ErrorSummary: summary},
	}
}

const (
	invalidSettings = "settings_error/invalid_settings/"
	notAuthorized   = "settings_error/not_authorized/"
	alreadyExists   = "shared_link_already_exists/..."
)

func testFs(t *testing.T, stub *sharingStub) *Fs {
	t.Helper()
	ctx := context.Background()
	return &Fs{
		slashRoot: "/root",
		sharing:   stub,
		pacer: fs.NewPacer(ctx, pacer.NewDefault(
			pacer.MinSleep(0), pacer.MaxSleep(0), pacer.DecayConstant(1))),
	}
}

// A link that Dropbox is happy with is made in one call, with the settings we
// actually want.
func TestPublicLinkNoFallbackWhenAccepted(t *testing.T) {
	stub := &sharingStub{}
	link, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Equal(t, "https://www.dropbox.com/s/created/file?dl=0", link)
	require.Len(t, stub.createCalls, 1)
	assert.Equal(t, sharing.LinkAudiencePublic, stub.createCalls[0].Audience.Tag)
	assert.Equal(t, sharing.RequestedVisibilityPublic, stub.createCalls[0].RequestedVisibility.Tag)
	assert.Zero(t, stub.listCalls)
}

// An item inside a shared folder that hands out members-only links rejects the
// public audience we ask for. Asking for less has to get the link made, the way
// the web UI manages to.
func TestPublicLinkFallsBackWhenSettingsRejected(t *testing.T) {
	stub := &sharingStub{createErrs: []error{apiErr(invalidSettings), apiErr(invalidSettings)}}
	link, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Equal(t, "https://www.dropbox.com/s/created/file?dl=0", link)

	require.Len(t, stub.createCalls, 3)
	// the deprecated visibility field goes first
	assert.Nil(t, stub.createCalls[1].RequestedVisibility)
	assert.NotNil(t, stub.createCalls[1].Audience)
	// then the audience, which leaves nothing to send at all
	assert.Nil(t, stub.createCalls[2])
	assert.Zero(t, stub.listCalls)
}

// Expiry survives the audience being given up, and is only dropped once it is
// the last thing left that Dropbox could be objecting to.
func TestPublicLinkKeepsExpiryUntilLast(t *testing.T) {
	stub := &sharingStub{createErrs: []error{
		apiErr(invalidSettings), apiErr(invalidSettings), apiErr(invalidSettings),
	}}
	_, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.Duration(24*time.Hour), false)
	require.NoError(t, err)

	require.Len(t, stub.createCalls, 4)
	require.NotNil(t, stub.createCalls[2])
	assert.NotNil(t, stub.createCalls[2].Expires, "expiry dropped too early")
	assert.Nil(t, stub.createCalls[2].Audience)
	assert.Nil(t, stub.createCalls[3])
}

// The pre-existing not_authorized path is untouched: plans that can't set an
// expiry get the link without one, and no audience is given up to get it.
func TestPublicLinkDropsExpiryWhenNotAuthorized(t *testing.T) {
	stub := &sharingStub{createErrs: []error{apiErr(notAuthorized)}}
	_, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.Duration(24*time.Hour), false)
	require.NoError(t, err)

	require.Len(t, stub.createCalls, 2)
	assert.Nil(t, stub.createCalls[1].Expires)
	assert.NotNil(t, stub.createCalls[1].Audience, "audience given up for an expiry problem")
}

// Dropbox checks the settings before it checks for an existing link, so a
// settings rejection can be hiding one.
func TestPublicLinkFindsExistingLinkAfterRejection(t *testing.T) {
	existing := &sharing.FileLinkMetadata{
		SharedLinkMetadata: sharing.SharedLinkMetadata{Url: "https://www.dropbox.com/s/existing/file?dl=0"},
	}
	for _, summary := range []string{invalidSettings, alreadyExists} {
		stub := &sharingStub{
			createErrs: []error{apiErr(summary), apiErr(summary), apiErr(summary), apiErr(summary)},
			listLinks:  []sharing.IsSharedLinkMetadata{existing},
		}
		link, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.DurationOff, false)
		require.NoError(t, err, summary)
		assert.Equal(t, "https://www.dropbox.com/s/existing/file?dl=0", link, summary)
		assert.Equal(t, 1, stub.listCalls, summary)
	}
}

// With nothing to fall back on, the user sees what Dropbox actually said rather
// than a claim about a link that was never there.
func TestPublicLinkReportsCreateErrorWhenNothingToFallBackOn(t *testing.T) {
	stub := &sharingStub{createErrs: []error{
		apiErr(invalidSettings), apiErr(invalidSettings), apiErr(invalidSettings),
	}}
	_, err := testFs(t, stub).PublicLink(context.Background(), "file.mp4", fs.DurationOff, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), invalidSettings)
	assert.Equal(t, 1, stub.listCalls)
}

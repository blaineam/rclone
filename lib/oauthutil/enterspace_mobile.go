package oauthutil

import (
	"github.com/rclone/rclone/fs"
)

// Enter Space drives the OAuth flow from a native mobile UI rather than a
// browser rclone launched itself: it needs the auth URL to hand to
// ASWebAuthenticationSession, and it needs to abort the flow when the user
// dismisses that sheet.
//
// Upstream grew the same capability in v1.75.0 as the config/oauthstatus and
// config/oauthstop rc endpoints, backed by oauthCancelFn/oauthURL. These are
// direct-call wrappers over that same state for the gomobile binding, which
// links the package rather than going over the rc socket. They live in their
// own file so upstream's oauthutil.go stays byte-identical and future merges
// don't conflict here.

// GetPendingAuthURL returns the auth URL of the OAuth flow currently in
// progress, or "" when there is none.
func GetPendingAuthURL() string {
	oauthCancelMu.Lock()
	defer oauthCancelMu.Unlock()
	return oauthURL
}

// SetPendingAuthURL overrides the auth URL reported for the current flow.
//
// configSetup publishes the URL itself, so this is only for callers driving a
// flow that does not go through it.
func SetPendingAuthURL(url string) {
	oauthCancelMu.Lock()
	defer oauthCancelMu.Unlock()
	oauthURL = url
}

// ClearPendingAuthURL forgets the pending auth URL without cancelling the
// flow. Enter Space calls this to dismiss the auth sheet once it has the code
// but while the config flow is still running.
func ClearPendingAuthURL() {
	oauthCancelMu.Lock()
	defer oauthCancelMu.Unlock()
	oauthURL = ""
}

// CancelPendingAuth aborts the OAuth flow in progress, unblocking configSetup
// and releasing the callback port. Returns whether there was one to cancel.
//
// This is the direct-call equivalent of the config/oauthstop rc command.
func CancelPendingAuth() bool {
	oauthCancelMu.Lock()
	cancel := oauthCancelFn
	oauthCancelFn = nil
	oauthURL = ""
	oauthCancelMu.Unlock()

	if cancel == nil {
		fs.Debugf(nil, "CancelPendingAuth: no OAuth flow in progress")
		return false
	}
	fs.Logf(nil, "Cancelling pending OAuth flow")
	cancel()
	return true
}

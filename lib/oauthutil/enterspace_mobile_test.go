package oauthutil

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetPendingAuth clears the shared OAuth state before and after a test.
func resetPendingAuth(t *testing.T) {
	t.Helper()
	reset := func() {
		oauthCancelMu.Lock()
		oauthCancelFn = nil
		oauthURL = ""
		oauthCancelMu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

func TestPendingAuthURLAccessors(t *testing.T) {
	resetPendingAuth(t)

	assert.Equal(t, "", GetPendingAuthURL(), "no flow: no URL")

	SetPendingAuthURL("http://127.0.0.1:53682/auth?state=abc")
	assert.Equal(t, "http://127.0.0.1:53682/auth?state=abc", GetPendingAuthURL())

	// Clearing forgets the URL but must NOT cancel the flow: Enter Space
	// dismisses the sheet once it has the code while configSetup still runs.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oauthCancelMu.Lock()
	oauthCancelFn = cancel
	oauthCancelMu.Unlock()

	ClearPendingAuthURL()
	assert.Equal(t, "", GetPendingAuthURL())
	assert.NoError(t, ctx.Err(), "ClearPendingAuthURL cancelled the flow")
	oauthCancelMu.Lock()
	assert.NotNil(t, oauthCancelFn, "ClearPendingAuthURL dropped the cancel func")
	oauthCancelMu.Unlock()
}

func TestCancelPendingAuthWithNoFlow(t *testing.T) {
	resetPendingAuth(t)
	SetPendingAuthURL("http://stale")
	assert.False(t, CancelPendingAuth())
	// Even with nothing to cancel, a stale URL is not left behind.
	assert.Equal(t, "", GetPendingAuthURL())
}

func TestCancelPendingAuthCancelsOnce(t *testing.T) {
	resetPendingAuth(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oauthCancelMu.Lock()
	oauthCancelFn = cancel
	oauthURL = "http://127.0.0.1:53682/auth?state=xyz"
	oauthCancelMu.Unlock()

	assert.True(t, CancelPendingAuth())
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Equal(t, "", GetPendingAuthURL())

	// The second call finds nothing in progress.
	assert.False(t, CancelPendingAuth())
}

// TestCancelPendingAuthUnblocksConfigSetup drives the real configSetup:
// the auth URL it publishes must be visible through GetPendingAuthURL, and
// CancelPendingAuth must unblock it and release the callback port.
//
// configSetup binds the fixed loopback callback address; if something else on
// this machine holds it the test skips rather than flakes.
func TestCancelPendingAuthUnblocksConfigSetup(t *testing.T) {
	resetPendingAuth(t)
	probe, err := net.Listen("tcp", bindAddress)
	if err != nil {
		t.Skipf("callback address %s busy: %v", bindAddress, err)
	}
	require.NoError(t, probe.Close())

	m := configmap.Simple{config.ConfigAuthNoBrowser: "true"}
	oauthConfig := &Config{
		ClientID: "id", ClientSecret: "secret",
		AuthURL:     "http://127.0.0.1:1/authorize",
		TokenURL:    "http://127.0.0.1:1/token",
		RedirectURL: RedirectURL,
	}

	type result struct {
		code string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		code, err := configSetup(context.Background(), "id", "test", m, oauthConfig, nil)
		done <- result{code, err}
	}()

	var url string
	deadline := time.Now().Add(10 * time.Second)
	for url == "" && time.Now().Before(deadline) {
		url = GetPendingAuthURL()
		if url == "" {
			time.Sleep(5 * time.Millisecond)
		}
	}
	require.NotEmpty(t, url, "configSetup never published its auth URL")
	assert.True(t, strings.HasPrefix(url, "http://"+bindAddress+"/auth?state="), url)

	require.True(t, CancelPendingAuth())
	select {
	case r := <-done:
		require.Error(t, r.err)
		assert.Contains(t, r.err.Error(), "cancelled")
		assert.Empty(t, r.code)
	case <-time.After(10 * time.Second):
		t.Fatal("configSetup did not return after CancelPendingAuth")
	}
	assert.Equal(t, "", GetPendingAuthURL())

	// The callback port must be free again.
	var l net.Listener
	for i := 0; i < 100; i++ {
		if l, err = net.Listen("tcp", bindAddress); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err, "callback port still held after cancel")
	require.NoError(t, l.Close())
}

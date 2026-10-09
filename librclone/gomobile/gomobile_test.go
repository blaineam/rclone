package rclone

// Tests for the gomobile shims Enter Space calls: the VFS bridge lifecycle
// (start, add a remote to a running bridge, remove one, generation, stop) and
// the RPC wrapper. Remotes are alias remotes over temp dirs, the config file
// and VFS cache live in t.TempDir(), and the bridge listens on loopback.

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/rclone/rclone/backend/alias"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupConfig writes a throwaway rclone.conf with alias remotes rem_a and
// rem_b over temp dirs, points rclone's config and cache at temp locations,
// and initializes the library against it.
func setupConfig(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	conf := ""
	for _, name := range []string{"rem_a", "rem_b"} {
		dir := filepath.Join(tmp, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		conf += "[" + name + "]\ntype = alias\nremote = " + dir + "\n\n"
	}
	confPath := filepath.Join(tmp, "rclone.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0o600))
	require.NoError(t, config.SetConfigPath(confPath))

	prevCache := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(filepath.Join(tmp, "cache")))
	t.Cleanup(func() { _ = config.SetCacheDir(prevCache) })

	Initialize() // installs the config file set above
	t.Cleanup(func() {
		StopVFSBridge()
		Finalize()
	})
}

func TestRPCWrapper(t *testing.T) {
	setupConfig(t)

	res := RPC("rc/noop", `{"a":"b"}`)
	require.Equal(t, 200, res.Status, res.Output)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(res.Output), &out))
	assert.Equal(t, "b", out["a"])

	res = RPC("no/such/method", "{}")
	assert.Equal(t, 404, res.Status)

	res = RPC("rc/noop", "{not json")
	assert.Equal(t, 400, res.Status)
}

func TestVFSBridgeLifecycle(t *testing.T) {
	setupConfig(t)

	// Nothing running: generation is zero, remove is a no-op success.
	assert.Equal(t, int64(0), VFSBridgeGeneration())
	assert.Equal(t, "", RemoveVFSBridgeRemote("rem_a"))
	StopVFSBridge() // no-op when not running

	addr := StartVFSBridge("rem_a", 0)
	require.False(t, strings.HasPrefix(addr, "error:"), addr)
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	assert.NotEqual(t, "0", port)
	assert.True(t, host == "127.0.0.1" || host == "::1", "bridge must listen on loopback, got %q", host)

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	gen1 := VFSBridgeGeneration()
	assert.Greater(t, gen1, int64(0))

	// A second start adds to the running bridge and returns the same address.
	assert.Equal(t, addr, StartVFSBridge("rem_b", 0))
	gen2 := VFSBridgeGeneration()
	assert.Greater(t, gen2, gen1)

	// Re-adding an exposed remote is a no-op.
	assert.Equal(t, addr, StartVFSBridge("rem_b", 0))
	assert.Equal(t, gen2, VFSBridgeGeneration())

	// Adding an unconfigured remote to a running bridge reports an error and
	// leaves the bridge up.
	res := StartVFSBridge("not_configured", 0)
	assert.True(t, strings.HasPrefix(res, "error: "), res)
	assert.Equal(t, gen2, VFSBridgeGeneration())

	// Remove one; removing it again succeeds without moving the generation.
	assert.Equal(t, "", RemoveVFSBridgeRemote("rem_b"))
	gen3 := VFSBridgeGeneration()
	assert.Greater(t, gen3, gen2)
	assert.Equal(t, "", RemoveVFSBridgeRemote("rem_b"))
	assert.Equal(t, gen3, VFSBridgeGeneration())

	StopVFSBridge()
	assert.Equal(t, int64(0), VFSBridgeGeneration())
	if c, err := net.Dial("tcp", addr); err == nil {
		_ = c.Close()
		t.Fatal("bridge still listening after StopVFSBridge")
	}
}

// A first start whose remote cannot be created must not leave a half-built
// bridge behind: the next start begins from scratch.
func TestStartVFSBridgeBadRemoteLeavesNothingRunning(t *testing.T) {
	setupConfig(t)

	res := StartVFSBridge("not_configured", 0)
	require.True(t, strings.HasPrefix(res, "error: "), res)
	assert.Equal(t, int64(0), VFSBridgeGeneration())

	addr := StartVFSBridge("rem_a", 0)
	require.False(t, strings.HasPrefix(addr, "error:"), addr)
}

// A port that is already taken is reported, and leaves nothing running.
func TestStartVFSBridgePortInUse(t *testing.T) {
	setupConfig(t)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	port := l.Addr().(*net.TCPAddr).Port

	res := StartVFSBridge("rem_a", port)
	require.True(t, strings.HasPrefix(res, "error: "), res)
	assert.Equal(t, int64(0), VFSBridgeGeneration())
}

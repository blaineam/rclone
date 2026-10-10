//go:build unix

package nfsmount

import (
	"os"
	"testing"

	"github.com/rclone/rclone/cmd/serve/nfs"
)

// TestMain keeps these tests away from the machine's real rpcbind: every
// mount starts an NFS server, which registers its port with the portmapper.
// Nothing listens on loopback port 1, so registration is skipped.
func TestMain(m *testing.M) {
	restore := nfs.SetPortmapperAddrForTest("127.0.0.1:1")
	code := m.Run()
	restore()
	os.Exit(code)
}

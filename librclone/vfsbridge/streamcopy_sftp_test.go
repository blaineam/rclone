package vfsbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/rc"
)

// The harness's sequential-copy case at bridge level, against an sftp server
// configured as the harness configures the stock one (cache mode writes, no
// write-back delay): plain, with the size set first, with a second open for
// writing mid-copy, and both. Each copy must land whole on the server.
func TestSequentialCopyOverSFTPInHarnessConfiguration(t *testing.T) {
	served := t.TempDir()
	call := rc.Calls.Get("serve/start")
	out, err := call.Fn(t.Context(), rc.Params{
		"type": "sftp", "fs": served, "addr": "127.0.0.1:0", "user": "u", "pass": "p",
		"vfs_cache_mode": "writes", "vfs_write_back": "0s",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rc.Calls.Get("serve/stop").Fn(t.Context(), rc.Params{"id": out["id"]}) })
	_, port, _ := netSplitHostPort(out["addr"].(string))
	conf := fmt.Sprintf("[sftp_a]\ntype = sftp\nhost = 127.0.0.1\nport = %s\nuser = u\npass = %s\nknown_hosts_file = none\nshell_type = none\n", port, obscure.MustObscure("p"))
	confPath := filepath.Join(t.TempDir(), "rclone.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	config.SetConfigPath(confPath)
	configfile.Install()
	cache.Clear()
	useTempCacheDir(t)
	s := NewServer()
	s.SetWriteThrough("sftp_a", true)
	if err := s.AddRemote("sftp_a"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	dir := remoteRootID(t, s, "sftp_a")
	part := payloadOf(1<<20, 'p')
	for _, variant := range []string{"plain", "truncate-first", "second-open", "both"} {
		t.Run(variant, func(t *testing.T) {
			name := "copy-" + variant + ".bin"
			r := mustOK(t, s.handleRequest(req(t, 1, "create", map[string]any{"dirId": dir, "name": name, "isDir": false})), "create")
			id := r.Result.(ItemInfo).ID
			mustOK(t, s.handleRequest(req(t, 2, "open", map[string]any{"itemId": id, "write": true})), "open")
			if variant == "truncate-first" || variant == "both" {
				resp := s.handleRequest(req(t, 8, "setattr", map[string]any{"itemId": id, "size": 4 << 20}))
				t.Logf("setattr size first: ok=%v errno=%d", resp.Ok, resp.Error)
			}
			for i := 0; i < 4; i++ {
				mustOK(t, s.handleRequest(req(t, 3, "write", map[string]any{"itemId": id, "offset": i * len(part), "data": part})), fmt.Sprintf("write %d", i))
				if i == 1 && (variant == "second-open" || variant == "both") {
					mustOK(t, s.handleRequest(req(t, 6, "open", map[string]any{"itemId": id, "write": true})), "second open")
					mustOK(t, s.handleRequest(req(t, 7, "close", map[string]any{"itemId": id})), "second close")
				}
			}
			resp := s.handleRequest(req(t, 4, "close", map[string]any{"itemId": id}))
			if !resp.Ok {
				t.Fatalf("%s: close failed errno=%d", variant, resp.Error)
			}
			st, err := os.Stat(filepath.Join(served, name))
			if err != nil || st.Size() != 4<<20 {
				t.Fatalf("%s: on the server: %v %d", variant, err, st.Size())
			}
		})
	}
}

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzLoadConfigFile feeds arbitrary bytes to the --config JSON loader; it must never
// panic (bad config must be an error).
func FuzzLoadConfigFile(f *testing.F) {
	f.Add(`{"id":1,"raft":"a","grpc":"b","shards":2,"peers":{"1":{"raft":"a","grpc":"b"}}}`)
	f.Add(`{"gossip":true,"peers":{"x":{"raft":"notanumber"}}}`)
	f.Add(`{`)
	f.Add(`{"shards":-1,"oidcIssuer":"http://x"}`)
	f.Fuzz(func(t *testing.T, content string) {
		path := filepath.Join(t.TempDir(), "cfg.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Skip()
		}
		_, _ = loadConfigFile(path)
	})
}

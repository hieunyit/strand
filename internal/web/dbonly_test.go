package web

import (
	"os"
	"strings"
	"testing"
)

// The DB copy is the source of truth: the registry parses the stored YAML, so a missing or stale materialised
// file (deleted volume, edit outside the web) neither breaks the pipeline view nor changes what the web shows.
func TestRegistryReadsConfigFromDatabaseNotDisk(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	if p == nil || p.CfgErr != "" {
		t.Fatalf("precondition: %+v", p)
	}
	if err := os.Remove(p.ConfigPath); err != nil {
		t.Fatal(err)
	}
	e.srv.reg.Refresh(t.Context())
	got := e.srv.reg.Get("orders")
	if got == nil || got.CfgErr != "" || got.Cfg == nil || got.Cfg.Name != "orders" {
		t.Fatalf("pipeline view broke when its materialised file vanished: %+v", got)
	}
	// An out-of-band edit on disk is not what the web shows or edits.
	if err := os.WriteFile(p.ConfigPath, []byte("version: 1\nname: orders\n# edited on disk\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if b := string(e.srv.getOriginalConfig(got)); strings.Contains(b, "edited on disk") || !strings.Contains(b, "admin_socket") {
		t.Fatalf("config editor reads the disk file instead of the database copy:\n%s", b)
	}
}

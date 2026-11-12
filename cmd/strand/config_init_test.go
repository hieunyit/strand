package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/config"
)

// Mọi tổ hợp nguồn/đích hợp lệ: file sinh ra phải nạp được bằng bộ đọc cấu hình strict (không key lạ, đủ
// trường bắt buộc) và mang đúng kind/host/include.
func TestConfigInitRendersLoadableConfigs(t *testing.T) {
	t.Setenv("SRC_PW", "s")
	t.Setenv("DST_PW", "d")
	for _, c := range []struct{ from, to, srcKind, dstKind string }{
		{"mariadb", "mysql", "mysql", "mysql"},
		{"mysql", "mysql", "mysql", "mysql"},
		{"mysql", "postgres", "mysql", "postgres"},
		{"postgres", "postgres", "postgres", "postgres"},
	} {
		for _, systemd := range []bool{false, true} {
			s, err := renderInitConfig(initOpts{Name: "shop-db", From: c.from, To: c.to, SourceHost: "10.1.1.1", TargetHost: "10.2.2.2", Database: "shop", Systemd: systemd})
			if err != nil {
				t.Fatalf("%s->%s: %v", c.from, c.to, err)
			}
			p := filepath.Join(t.TempDir(), "p.yaml")
			if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.LoadFile(p)
			if err != nil {
				t.Fatalf("%s->%s systemd=%v: generated config does not load: %v\n%s", c.from, c.to, systemd, err, s)
			}
			if cfg.Name != "shop-db" || cfg.Source.Kind != c.srcKind || cfg.Target.Kind != c.dstKind ||
				len(cfg.Tables.Include) != 1 || cfg.Tables.Include[0] != "shop.*" ||
				!strings.Contains(cfg.Source.DSN, "10.1.1.1") || !strings.Contains(cfg.Target.DSN, "10.2.2.2") {
				t.Fatalf("%s->%s: unexpected config %+v", c.from, c.to, cfg)
			}
			if systemd != strings.HasPrefix(cfg.Resources.Spool.Path, "/var/lib/strand/shop-db") {
				t.Fatalf("systemd=%v spool=%s", systemd, cfg.Resources.Spool.Path)
			}
		}
	}
}

func TestConfigInitRejectsUnsupportedPairs(t *testing.T) {
	for _, o := range []initOpts{
		{Name: "x", From: "mariadb", To: "postgres", Database: "app"},
		{Name: "x", From: "postgres", To: "mysql", Database: "app"},
		{Name: "x", From: "oracle", To: "mysql", Database: "app"},
		{Name: "Bad Name", From: "mysql", To: "mysql", Database: "app"},
		{Name: "x", From: "mysql", To: "mysql", Database: "a*"},
	} {
		if _, err := renderInitConfig(o); err == nil {
			t.Errorf("%+v: want error", o)
		}
	}
}

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
			s, err := renderInitConfig(initOpts{Name: "shop-db", From: c.from, To: c.to, SourceHost: "10.1.1.1", TargetHost: "10.2.2.2", Databases: []string{"shop"}, PGSchema: "public", Systemd: systemd})
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
				len(cfg.Tables.Include) != 1 || cfg.Tables.Include[0] != map[bool]string{true: "shop.*", false: "public.*"}[c.srcKind == "mysql"] ||
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
		{Name: "x", From: "mariadb", To: "postgres", Databases: []string{"app"}},
		{Name: "x", From: "postgres", To: "mysql", Databases: []string{"app"}},
		{Name: "x", From: "oracle", To: "mysql", Databases: []string{"app"}},
		{Name: "Bad Name", From: "mysql", To: "mysql", Databases: []string{"app"}},
		{Name: "x", From: "mysql", To: "mysql", Databases: []string{"a?"}},
		{Name: "x", From: "mysql", To: "mysql"},
		{Name: "x", From: "postgres", To: "postgres", Databases: []string{"a", "b"}, PGSchema: "public"},
	} {
		if _, err := renderInitConfig(o); err == nil {
			t.Errorf("%+v: want error", o)
		}
	}
}

// Nhiều database trong một pipeline và "*" = mọi database người dùng.
func TestConfigInitMultipleAndAllDatabases(t *testing.T) {
	t.Setenv("SRC_PW", "s")
	t.Setenv("DST_PW", "d")
	for _, c := range []struct {
		dbs   []string
		want  []string
		grant string
	}{
		{[]string{"app", "shop", "crm"}, []string{"app.*", "shop.*", "crm.*"}, "ON app.*, shop.*, crm.*"},
		{[]string{"*"}, []string{"*.*"}, "ON *.*"},
		{[]string{"all"}, []string{"*.*"}, "ON *.*"},
	} {
		s, err := renderInitConfig(initOpts{Name: "m", From: "mariadb", To: "mysql", SourceHost: "h1", TargetHost: "h2", Databases: c.dbs})
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := config.LoadYAMLStrict([]byte(os.ExpandEnv(s)))
		if err != nil {
			t.Fatalf("%v: %v\n%s", c.dbs, err, s)
		}
		if strings.Join(cfg.Tables.Include, ",") != strings.Join(c.want, ",") || !strings.Contains(s, c.grant) {
			t.Fatalf("%v: include=%v\n%s", c.dbs, cfg.Tables.Include, s)
		}
	}
}

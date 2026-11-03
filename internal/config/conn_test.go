package config

import (
	"strings"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"
)

func mustDSN(t *testing.T, dsn string) *drivermysql.Config {
	t.Helper()
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("result %q is not a driver DSN: %v", dsn, err)
	}
	return c
}

func TestConnURLAndSplitFields(t *testing.T) {
	t.Setenv("SRC_PW", "p@ss:w/rd")
	yml := `
version: 1
name: p
source: { kind: mysql, host: 10.0.0.11, user: strand, password_env: SRC_PW }
target: { kind: mysql, dsn: "mysql://strand:t%40p%2Fw@10.0.0.21/strand?tls=skip-verify&timeout=5s" }
tables: { include: ["app.*"] }
`
	cfg, err := LoadYAMLStrict([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	s := mustDSN(t, cfg.Source.DSN)
	if s.User != "strand" || s.Passwd != "p@ss:w/rd" || s.Addr != "10.0.0.11:3306" || s.DBName != "" || s.Net != "tcp" {
		t.Fatalf("source: %+v", s)
	}
	d := mustDSN(t, cfg.Target.DSN)
	if d.User != "strand" || d.Passwd != "t@p/w" || d.Addr != "10.0.0.21:3306" || d.DBName != "strand" || d.TLSConfig != "skip-verify" || d.Timeout.String() != "5s" {
		t.Fatalf("target: %+v", d)
	}
}

func TestConnLegacyDSNUnchanged(t *testing.T) {
	yml := `
version: 1
name: p
source: { kind: mysql, dsn: "root:dev@tcp(127.0.0.1:3306)/" }
target: { kind: mysql, dsn: "root:dev@tcp(127.0.0.1:3307)/strand" }
tables: { include: ["app.*"] }
`
	cfg, err := LoadYAMLStrict([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.DSN != "root:dev@tcp(127.0.0.1:3306)/" || cfg.Target.DSN != "root:dev@tcp(127.0.0.1:3307)/strand" {
		t.Fatalf("legacy DSN changed: %q %q", cfg.Source.DSN, cfg.Target.DSN)
	}
}

func TestConnErrors(t *testing.T) {
	base := "version: 1\nname: p\ntables: { include: [\"app.*\"] }\ntarget: { kind: mysql, dsn: \"mysql://u:p@h/strand\" }\n"
	cases := map[string]string{
		"both":        `source: { kind: mysql, dsn: "mysql://u:p@h/", host: h, user: u }`,
		"missing env": `source: { kind: mysql, host: h, user: u, password_env: NOPE_NOT_SET_123 }`,
		"no user":     `source: { kind: mysql, host: h }`,
		"pw twice":    `source: { kind: mysql, host: h, user: u, password: x, password_env: HOME }`,
		"bad param":   `source: { kind: mysql, dsn: "mysql://u:p@h/?timeout=notaduration" }`,
		"no host":     `source: { kind: mysql, dsn: "mysql://u:p@/db" }`,
	}
	for name, src := range cases {
		if _, err := LoadYAMLStrict([]byte(base + src + "\n")); err == nil || !strings.Contains(err.Error(), "source") {
			t.Errorf("%s: want a source error, got %v", name, err)
		}
	}
}

func TestConnPortAndEnvOverride(t *testing.T) {
	t.Setenv("STRAND_SOURCE_PORT", "3310")
	yml := `
version: 1
name: p
source: { kind: mysql, host: db1, user: u, password: x, database: app }
target: { kind: mysql, host: db2, port: 3307, user: u, password: y, database: strand }
tables: { include: ["app.*"] }
`
	cfg, err := LoadYAMLStrict([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	if s := mustDSN(t, cfg.Source.DSN); s.Addr != "db1:3310" || s.DBName != "app" {
		t.Fatalf("source %+v", s)
	}
	if d := mustDSN(t, cfg.Target.DSN); d.Addr != "db2:3307" || d.Passwd != "y" {
		t.Fatalf("target %+v", d)
	}
}

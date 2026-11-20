package config

import (
	"strings"
	"testing"
)

const mongoYAML = `
version: 1
name: m1
source:
  kind: mongodb
  uri_env: TEST_MONGO_SRC
  user: app
  password_env: TEST_MONGO_PASS
  database: app
target:
  kind: mongodb
  dsn: "mongodb://t1:27017,t2:27017/?replicaSet=rs1"
tables:
  include: ["app.*"]
bootstrap:
  logical: { max_parallel: 4, chunk_rows: "500" }
mongodb:
  updates: delta
  indexes: replicate
  lease_ttl: 20s
`

func TestMongoDBConfig(t *testing.T) {
	t.Setenv("TEST_MONGO_SRC", "mongodb://s1:27017,s2:27017/?replicaSet=rs0&authSource=admin")
	t.Setenv("TEST_MONGO_PASS", "p@ss:w/rd")
	cfg, err := LoadYAMLStrict([]byte(mongoYAML))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.IsMongoDB() || cfg.Target.Database != "app" || cfg.MongoDB.Indexes != "replicate" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if !strings.HasPrefix(cfg.Source.DSN, "mongodb://app:p%40ss%3Aw%2Frd@s1:27017,s2:27017/") {
		t.Fatalf("credentials not injected: %s", cfg.Source.DSN)
	}
	for name, mut := range map[string]func(string) string{
		"sql target": func(s string) string {
			return strings.Replace(s, "target:\n  kind: mongodb", "target:\n  kind: postgres", 1)
		},
		"no database": func(s string) string { return strings.Replace(s, "  database: app\n", "", 1) },
		"dsn and uri_env": func(s string) string {
			return strings.Replace(s, "  uri_env: TEST_MONGO_SRC", "  uri_env: TEST_MONGO_SRC\n  dsn: mongodb://x", 1)
		},
		"host field":      func(s string) string { return strings.Replace(s, "  user: app", "  user: app\n  host: h", 1) },
		"rename":          func(s string) string { return s + "\n" + `tables_rename_placeholder` },
		"bad updates":     func(s string) string { return strings.Replace(s, "updates: delta", "updates: fast", 1) },
		"bad indexes":     func(s string) string { return strings.Replace(s, "indexes: replicate", "indexes: all", 1) },
		"short lease":     func(s string) string { return strings.Replace(s, "lease_ttl: 20s", "lease_ttl: 1s", 1) },
		"no include":      func(s string) string { return strings.Replace(s, `include: ["app.*"]`, `include: []`, 1) },
		"start_at":        func(s string) string { return strings.Replace(s, "bootstrap:\n", "bootstrap:\n  start_at: x\n", 1) },
		"chunk_rows":      func(s string) string { return strings.Replace(s, `chunk_rows: "500"`, `chunk_rows: "lots"`, 1) },
		"uri_env missing": func(s string) string { return strings.Replace(s, "TEST_MONGO_SRC", "TEST_MONGO_UNSET", 1) },
		"not a mongo uri": func(s string) string { return strings.Replace(s, `"mongodb://t1`, `"mysql://t1`, 1) },
	} {
		y := mut(mongoYAML)
		if name == "rename" {
			y = strings.Replace(mongoYAML, `include: ["app.*"]`, "include: [\"app.*\"]\n  rename: { app.a: app.b }", 1)
		}
		if _, err := LoadYAMLStrict([]byte(y)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The Web UI stores MongoDB pipelines with uri: "${SRC_URI}" and mongodb.snapshot_batch /
// stream_batch; secrets may come from the per-pipeline environment.
func TestMongoDBConfigWebKeys(t *testing.T) {
	y := `
version: 1
name: web-m
source: { kind: mongodb, uri: "${SRC_URI}", database: app }
target: { kind: mongodb, uri: "${DST_URI}", database: app_copy }
tables: { include: ["app.*"], exclude: ["app.tmp_*"] }
mongodb: { snapshot_batch: 500, stream_batch: 1000, lease_ttl: 30s, overwrite_target: true }
`
	cfg, err := LoadYAMLStrictWithEnv([]byte(y), map[string]string{"SRC_URI": "mongodb://u:p@s1/?replicaSet=a", "DST_URI": "mongodb://t1/?replicaSet=b"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.DSN != "mongodb://u:p@s1/?replicaSet=a" || cfg.Target.Database != "app_copy" || cfg.MongoDB.SnapshotBatch != 500 || !cfg.MongoDB.OverwriteTarget {
		t.Fatalf("unexpected: %+v", cfg)
	}
	if _, err := LoadYAMLStrict([]byte(strings.Replace(y, `uri: "${SRC_URI}"`, `uri: "${SRC_URI}", dsn: "mongodb://x"`, 1))); err == nil {
		t.Fatal("uri and dsn together accepted")
	}
}

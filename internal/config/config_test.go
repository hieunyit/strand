package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/exitcode"
)

const sampleSpecYAML = `
version: 1
name: orders-sync
source: { kind: mysql, dsn: "root:secret@tcp(127.0.0.1:3306)/orders", server_id: 41001, read_from: replica }
target:
  kind: postgres
  dsn: "postgres://user:pass@127.0.0.1:5432/orders"
  write_mode: upsert
  commit_order: strict
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables:
  include: ["app.orders", "app.users"]
  rename: { "app.users": "public.app_users" }
bootstrap:
  default: logical
  per_table:
    "app.orders": { mode: seed, seed: { type: xtrabackup, uri: "s3://mysql-backup/orders-db/full-2026-10-30" } }
  catch_up: { binlog_archive: "s3://mysql-binlog/orders-db/" }
  logical: { chunk_rows: auto, max_parallel: 4, windows: ["22:00-06:00"] }
schema: { additive: auto, destructive: pause }
checkpoint: { store: target, flush_every: 500ms }
resources:
  memory: { soft: 1GiB, hard: 2GiB }
  spool:  { path: /var/lib/strand/orders-sync, soft: 20GiB, hard: 50GiB, overflow: "s3://strand-staging/orders-sync/" }
  source: { iops: { soft: 600, hard: 900 } }
  target: { iops: { soft: 1500, hard: 2000 } }
  workers: { min: 1, max: 32 }
governor:
  algorithm: grace-v2
  tick: 1s
  lag_slo: 5s
  probe: on
  breaker: { strand_query_p99: { max_ratio: 3.0 }, replica_lag: { max: 10s } }
verify: { delta: on, full: { cron: "0 2 * * 0", rate: 10MiB } }
observability: { metrics: ":9464", admin_socket: /run/strand/orders-sync.sock, log: { level: info, format: json } }
`

func TestLoadYAMLStrict_Valid(t *testing.T) {
	cfg, err := config.LoadYAMLStrict([]byte(sampleSpecYAML))
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
	if cfg.Name != "orders-sync" {
		t.Errorf("expected name orders-sync, got %s", cfg.Name)
	}
	if cfg.Source.Kind != "mysql" || cfg.Target.Kind != "postgres" {
		t.Errorf("unexpected source/target kind: %s / %s", cfg.Source.Kind, cfg.Target.Kind)
	}
	if cfg.Target.CommitOrder != "strict" {
		t.Errorf("expected commit_order strict, got %s", cfg.Target.CommitOrder)
	}
}

func TestLoadYAMLStrict_UnknownKey(t *testing.T) {
	invalidYAML := `
version: 1
name: invalid
unknown_field: true
source: { kind: mysql, dsn: "root@tcp(127.0.0.1:3306)/db" }
target: { kind: mysql, dsn: "root@tcp(127.0.0.1:3307)/db" }
`
	_, err := config.LoadYAMLStrict([]byte(invalidYAML))
	if err == nil {
		t.Fatalf("expected error on unknown key, got nil")
	}
	code := exitcode.GetExitCode(err)
	if code != exitcode.InvalidSyntax {
		t.Errorf("expected exit code %d for unknown key, got %d (err: %v)", exitcode.InvalidSyntax, code, err)
	}
}

func TestLoadFile_EnvExpansion(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "test.yaml")

	t.Setenv("TEST_SRC_DSN", "mysql://root:secret@127.0.0.1:3306/db")
	t.Setenv("TEST_DST_DSN", "postgres://user:pass@127.0.0.1:5432/db")

	yamlContent := `
version: 1
name: env-test
source: { kind: mysql, dsn: "${TEST_SRC_DSN}" }
target: { kind: postgres, dsn: "${TEST_DST_DSN}" }
`
	if err := os.WriteFile(confPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadFile(confPath)
	if err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}
	// mysql:// được mở rộng từ biến môi trường rồi chuẩn hóa sang DSN của driver.
	if cfg.Source.DSN != "root:secret@tcp(127.0.0.1:3306)/db" {
		t.Errorf("expected expanded and normalized DSN, got %s", cfg.Source.DSN)
	}
	if cfg.Target.DSN != "postgres://user:pass@127.0.0.1:5432/db" {
		t.Errorf("expected expanded DSN, got %s", cfg.Target.DSN)
	}
}

func TestJSONSchema_Valid(t *testing.T) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(config.JSONSchema), &m); err != nil {
		t.Fatalf("JSONSchema is not valid JSON: %v", err)
	}
	if _, ok := m["$schema"]; !ok {
		t.Errorf("expected $schema field in JSONSchema")
	}
	if _, ok := m["properties"]; !ok {
		t.Errorf("expected properties field in JSONSchema")
	}
}

// review R10: tables.rename đưa hai bảng nguồn về cùng một bảng đích phải bị từ chối khi nạp cấu hình (exit 2).
func TestR10_DuplicateRenameTargetRejected(t *testing.T) {
	yml := `
version: 1
name: dup-rename
source: { kind: mysql, dsn: "root:secret@tcp(127.0.0.1:3306)/a" }
target: { kind: postgres, dsn: "postgres://u:p@127.0.0.1:5432/b" }
tables:
  include: ["a.*"]
  rename: { "a.t1": "b.t", "a.t2": "b.t" }
`
	_, err := config.LoadYAMLStrict([]byte(yml))
	if exitcode.GetExitCode(err) != exitcode.InvalidSyntax || err == nil || !strings.Contains(err.Error(), "b.t") {
		t.Fatalf("want exit 2 naming the duplicate target, got %v", err)
	}
	if err := config.ValidateRename(map[string]string{"a.t1": "b.t1", "a.t2": "b.t2"}); err != nil {
		t.Fatalf("distinct targets must be accepted: %v", err)
	}
}

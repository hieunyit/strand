package config_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"strand/internal/config"
	"strand/internal/exitcode"
)

const minimalYAML = `
version: 1
name: env-test
source: { kind: mysql, dsn: "root@tcp(127.0.0.1:3306)/db", server_id: 41001 }
target:
  kind: mysql
  dsn: "root@tcp(127.0.0.1:3307)/db"
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables: { include: ["app.orders"] }
resources:
  memory: { soft: 1GiB, hard: 2GiB }
  spool: { path: /var/lib/strand/a, soft: 20GiB, hard: 50GiB }
  source: { iops: { soft: 600, hard: 900 } }
governor: { lag_slo: 5s, breaker: { strand_query_p99: { max_ratio: 3.0 } } }
`

// Bản đầy đủ theo ví dụ ở spec/design.md §4 (key mà 02 §10 rút gọn), ${VAR} đã thay bằng giá trị.
const designFullYAML = `
version: 1
name: orders-sync
source:
  kind: mysql
  dsn: "root@tcp(127.0.0.1:3306)/db"
  server_id: 41001
  read_from: standby
target:
  kind: postgres
  dsn: "postgres://u:p@127.0.0.1:5432/db"
  write_mode: upsert
  commit_order: strict
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables:
  include: ["app.orders", "app.users", "app.payment_*"]
  rename: { "app.users": "public.app_users" }
  mask:   { "app.users.phone": sha256 }
bootstrap:
  default: logical
  per_table:
    "app.orders":
      mode: seed
      seed:
        type: mydumper
        uri: s3://mysql-dump/orders-2026-10-30
        credentials: prof
        encryption_key: key
        load: { threads: 8, defer_indexes: true, bandwidth: 200MiB }
        verify_after: { sample_ranges: 1%, delta_window: 30m }
  catch_up:
    binlog_archive: s3://mysql-binlog/orders-db/
  logical:
    chunk_rows: auto
    max_parallel: 4
    windows: ["22:00-06:00"]
schema: { additive: auto, destructive: pause }
checkpoint: { store: target, flush_every: 1s }
resources:
  memory:  { soft: 1GiB, hard: 2GiB }
  spool:   { path: /var/lib/strand/orders-sync, soft: 20GiB, hard: 50GiB, overflow: s3://strand-staging/orders-sync/ }
  source:  { iops: { soft: 600, hard: 900 }, bandwidth: { soft: 40MiB, hard: 60MiB } }
  target:  { iops: { soft: 1500, hard: 2000 }, bandwidth: { soft: 80MiB, hard: 120MiB } }
  network: { backup_read: { soft: 150MiB, hard: 250MiB } }
  workers: { min: 1, max: 32 }
  schedule:
    - { cron: "0 8 * * 1-5", source.iops.soft: 300 }
    - { cron: "0 22 * * *",  source.iops.soft: 900 }
governor:
  algorithm: grace-v2
  tick: 1s
  lag_slo: 5s
  probe: on
  breaker: { strand_query_p99: { max_ratio: 3.0 }, replica_lag: { max: 10s } }
verify:   { delta: on, full: { cron: "0 2 * * 0", rate: 10MiB } }
observability: { metrics: ":9464", admin_socket: /run/strand/orders-sync.sock, log: { level: info, format: json } }
`

func mustLoad(t *testing.T, y string) *config.Config {
	t.Helper()
	cfg, err := config.LoadYAMLStrict([]byte(y))
	if err != nil {
		t.Fatalf("LoadYAMLStrict: %v", err)
	}
	return cfg
}

func TestEnvOverride_ScalarKinds(t *testing.T) {
	cfg := &config.Config{}
	env := []string{
		"STRAND_TARGET_DSN=root@tcp(10.0.0.1:3306)/x",            // string
		"STRAND_TARGET_BATCH_MAX_ROWS=500",                       // int
		"STRAND_SOURCE_SERVER_ID=42",                             // uint32
		"STRAND_GOVERNOR_LAG_SLO=750ms",                          // duration (chuỗi, key có "_")
		"STRAND_RESOURCES_SPOOL_PATH=/data/spool",                // key lồng ba cấp
		"STRAND_RESOURCES_MEMORY_SOFT=512MiB",                    // kích thước byte
		"STRAND_GOVERNOR_BREAKER_STRAND_QUERY_P99_MAX_RATIO=2.5", // float, nhiều "_"
		"STRAND_GOVERNOR_METRICS_SOURCE_URL=http://prom:9090",    // con trỏ nil được cấp phát
		"STRAND_RESOURCES_SOURCE_BANDWIDTH_SOFT=40MiB",           // key mới
	}
	if err := config.ApplyEnvOverrides(cfg, env); err != nil {
		t.Fatalf("ApplyEnvOverrides: %v", err)
	}
	if cfg.Target.DSN != "root@tcp(10.0.0.1:3306)/x" {
		t.Errorf("target.dsn = %q", cfg.Target.DSN)
	}
	if cfg.Target.Batch.MaxRows != 500 {
		t.Errorf("target.batch.max_rows = %d", cfg.Target.Batch.MaxRows)
	}
	if cfg.Source.ServerID != 42 {
		t.Errorf("source.server_id = %d", cfg.Source.ServerID)
	}
	if cfg.Governor.LagSLO != "750ms" {
		t.Errorf("governor.lag_slo = %q", cfg.Governor.LagSLO)
	}
	if cfg.Resources.Spool.Path != "/data/spool" {
		t.Errorf("resources.spool.path = %q", cfg.Resources.Spool.Path)
	}
	if cfg.Resources.Memory.Soft != "512MiB" {
		t.Errorf("resources.memory.soft = %q", cfg.Resources.Memory.Soft)
	}
	if cfg.Governor.Breaker.StrandQueryP99.MaxRatio != 2.5 {
		t.Errorf("max_ratio = %v", cfg.Governor.Breaker.StrandQueryP99.MaxRatio)
	}
	if cfg.Governor.MetricsSource == nil || cfg.Governor.MetricsSource.URL != "http://prom:9090" {
		t.Errorf("governor.metrics_source.url not set: %+v", cfg.Governor.MetricsSource)
	}
	if cfg.Resources.Source.Bandwidth.Soft != "40MiB" {
		t.Errorf("resources.source.bandwidth.soft = %q", cfg.Resources.Source.Bandwidth.Soft)
	}
}

func TestEnvOverride_MapScopedAndBadValue(t *testing.T) {
	// Key bool duy nhất (seed.load.defer_indexes) nằm trong map per_table nên không ghi đè qua env;
	// kiểu bool và time.Duration được kiểm ở env_internal_test.go.
	for _, k := range config.EnvOverrideKeys() {
		if strings.Contains(k, "DEFER_INDEXES") {
			t.Fatalf("map-scoped key must not be env-overridable: %s", k)
		}
	}
	cfg := &config.Config{}
	err := config.ApplyEnvOverrides(cfg, []string{"STRAND_TARGET_BATCH_MAX_ROWS=abc"})
	if err == nil || exitcode.GetExitCode(err) != exitcode.InvalidSyntax {
		t.Fatalf("expected exit %d for bad int, got %v", exitcode.InvalidSyntax, err)
	}
}

func TestEnvOverride_OverridesYAML(t *testing.T) {
	t.Setenv("STRAND_TARGET_DSN", "root@tcp(env-host:3306)/db")
	t.Setenv("STRAND_GOVERNOR_LAG_SLO", "9s")
	t.Setenv("STRAND_TARGET_BATCH_MAX_ROWS", "77")
	t.Setenv("STRAND_RESOURCES_SPOOL_SOFT", "1GiB")
	cfg := mustLoad(t, minimalYAML)
	if cfg.Target.DSN != "root@tcp(env-host:3306)/db" {
		t.Errorf("target.dsn = %q, want env value", cfg.Target.DSN)
	}
	if cfg.Governor.LagSLO != "9s" {
		t.Errorf("governor.lag_slo = %q, want 9s", cfg.Governor.LagSLO)
	}
	if cfg.Target.Batch.MaxRows != 77 {
		t.Errorf("max_rows = %d, want 77", cfg.Target.Batch.MaxRows)
	}
	if cfg.Resources.Spool.Soft != "1GiB" {
		t.Errorf("spool.soft = %q, want 1GiB", cfg.Resources.Spool.Soft)
	}
	// Key không bị ghi đè giữ giá trị YAML.
	if cfg.Resources.Spool.Hard != "50GiB" || cfg.Source.ServerID != 41001 {
		t.Errorf("YAML values lost: spool.hard=%q server_id=%d", cfg.Resources.Spool.Hard, cfg.Source.ServerID)
	}
}

func TestEnvOverride_BeforeValidation(t *testing.T) {
	// Thiếu target.dsn trong YAML nhưng có STRAND_TARGET_DSN: env áp trước kiểm tra bắt buộc.
	t.Setenv("STRAND_TARGET_DSN", "root@tcp(env-host:3306)/db")
	y := strings.Replace(minimalYAML, `  dsn: "root@tcp(127.0.0.1:3307)/db"`+"\n", "", 1)
	cfg := mustLoad(t, y)
	if cfg.Target.DSN != "root@tcp(env-host:3306)/db" {
		t.Errorf("target.dsn = %q", cfg.Target.DSN)
	}
}

func TestEnvOverride_LoadFile(t *testing.T) {
	t.Setenv("STRAND_NAME", "from-env")
	p := filepath.Join(t.TempDir(), "p.yaml")
	if err := os.WriteFile(p, []byte(minimalYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "from-env" {
		t.Errorf("name = %q, want from-env", cfg.Name)
	}
}

func TestEnvOverride_UnknownIgnored(t *testing.T) {
	t.Setenv("STRAND_S3_ENDPOINT", "http://minio:9000")
	t.Setenv("STRAND_S3_ACCESS_KEY", "x")
	t.Setenv("STRAND_S3_SECRET_KEY", "y")
	t.Setenv("STRAND_E2E_KEEP_DIR", "1")
	t.Setenv("STRAND_NO_SUCH_KEY", "z")
	t.Setenv("STRAND_GOVERNOR", "not-a-scalar") // trỏ vào khối, không phải key vô hướng
	cfg := mustLoad(t, minimalYAML)
	if cfg.Name != "env-test" {
		t.Errorf("name = %q", cfg.Name)
	}
}

func TestEnvOverrideKeys_PathsResolved(t *testing.T) {
	keys := config.EnvOverrideKeys()
	want := []string{
		"STRAND_TARGET_DSN",
		"STRAND_RESOURCES_SPOOL_PATH",
		"STRAND_GOVERNOR_LAG_SLO",
		"STRAND_CHECKPOINT_FLUSH_EVERY",
		"STRAND_VERIFY_POST_SEED_WINDOW",
		"STRAND_RESOURCES_NETWORK_BACKUP_READ_HARD",
	}
	set := map[string]bool{}
	for _, k := range keys {
		set[k] = true
	}
	for _, w := range want {
		if !set[w] {
			t.Errorf("missing env key %s", w)
		}
	}
	// Bảng dựng không lỗi (không có hai đường dẫn cho cùng một tên biến).
	if err := config.ApplyEnvOverrides(&config.Config{}, nil); err != nil {
		t.Fatalf("env index error: %v", err)
	}
}

func TestNewKeys_ParseFromYAML(t *testing.T) {
	cfg := mustLoad(t, designFullYAML)
	if cfg.Source.ReadFrom != "standby" {
		t.Errorf("read_from = %q", cfg.Source.ReadFrom)
	}
	if cfg.Tables.Mask["app.users.phone"] != "sha256" {
		t.Errorf("tables.mask = %v", cfg.Tables.Mask)
	}
	if b := cfg.Resources.Source.Bandwidth; b.Soft != "40MiB" || b.Hard != "60MiB" {
		t.Errorf("resources.source.bandwidth = %+v", b)
	}
	if b := cfg.Resources.Target.Bandwidth; b.Soft != "80MiB" || b.Hard != "120MiB" {
		t.Errorf("resources.target.bandwidth = %+v", b)
	}
	if b := cfg.Resources.Network.BackupRead; b.Soft != "150MiB" || b.Hard != "250MiB" {
		t.Errorf("resources.network.backup_read = %+v", b)
	}
	if len(cfg.Resources.Schedule) != 2 || cfg.Resources.Schedule[0].Cron != "0 8 * * 1-5" ||
		cfg.Resources.Schedule[0].Overrides["source.iops.soft"] != 300 {
		t.Errorf("resources.schedule = %+v", cfg.Resources.Schedule)
	}
	seed := cfg.Bootstrap.PerTable["app.orders"].Seed
	if seed == nil || seed.Credentials != "prof" || seed.EncryptionKey != "key" ||
		seed.Load == nil || seed.Load.Threads != 8 || !seed.Load.DeferIndexes || seed.Load.Bandwidth != "200MiB" ||
		seed.VerifyAfter == nil || seed.VerifyAfter.SampleRanges != "1%" || seed.VerifyAfter.DeltaWindow != "30m" {
		t.Errorf("seed = %+v", seed)
	}

	got := cfg.UnimplementedKeys()
	want := []string{
		"source.read_from=standby",
		"tables.mask",
		"resources.source.bandwidth",
		"resources.target.bandwidth",
		"resources.network.backup_read",
		"resources.schedule",
		"resources.workers.min (adaptive worker count; workers.max is used)",
		`bootstrap.per_table."app.orders".seed.credentials`,
		`bootstrap.per_table."app.orders".seed.encryption_key`,
		`bootstrap.per_table."app.orders".seed.load`,
		`bootstrap.per_table."app.orders".seed.verify_after`,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("UnimplementedKeys =\n %v\nwant\n %v", got, want)
	}
	if w := config.UnimplementedWarning("tables.mask"); w != "config key tables.mask is accepted but not implemented in this version" {
		t.Errorf("warning text = %q", w)
	}
}

// Ví dụ 02 §10 đặt workers {min: 1, max: 32}: số worker tự co giãn chưa có nên phải được cảnh báo,
// ngoài ra không còn key nào chưa hỗ trợ.
func TestUnimplementedKeys_SpecExampleOnlyWorkersMin(t *testing.T) {
	cfg := mustLoad(t, sampleSpecYAML)
	k := cfg.UnimplementedKeys()
	if len(k) != 1 || k[0] != "resources.workers.min (adaptive worker count; workers.max is used)" {
		t.Errorf("unimplemented keys for 02 §10 example: %v", k)
	}
}

func TestNewKeys_Schedule_RequiresKnownShape(t *testing.T) {
	// Key lạ ở cấp resources vẫn bị từ chối (KnownFields), inline map chỉ áp trong từng mục schedule.
	y := strings.Replace(minimalYAML, "resources:\n", "resources:\n  bogus: 1\n", 1)
	if _, err := config.LoadYAMLStrict([]byte(y)); err == nil {
		t.Fatal("expected unknown key resources.bogus to be rejected")
	}
}

// --- Kiểm file cấu hình theo JSON Schema (bộ kiểm tối giản: type, enum, required,
// properties, additionalProperties, items, $ref nội bộ) ---

func loadSchema(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(config.JSONSchema), &m); err != nil {
		t.Fatalf("schema JSON: %v", err)
	}
	return m
}

func schemaCheck(root, s map[string]any, v any, path string) []string {
	if ref, ok := s["$ref"].(string); ok {
		name := strings.TrimPrefix(ref, "#/definitions/")
		s = root["definitions"].(map[string]any)[name].(map[string]any)
	}
	var errs []string
	if typ, ok := s["type"]; ok && !typeMatches(typ, v) {
		return []string{fmt.Sprintf("%s: type %v, got %T", path, typ, v)}
	}
	if enum, ok := s["enum"].([]any); ok {
		found := false
		for _, e := range enum {
			if fmt.Sprint(e) == fmt.Sprint(v) {
				found = true
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("%s: %v not in enum %v", path, v, enum))
		}
	}
	switch val := v.(type) {
	case map[string]any:
		props, _ := s["properties"].(map[string]any)
		if req, ok := s["required"].([]any); ok {
			for _, r := range req {
				if _, ok := val[r.(string)]; !ok {
					errs = append(errs, fmt.Sprintf("%s: missing required %v", path, r))
				}
			}
		}
		for k, child := range val {
			if ps, ok := props[k].(map[string]any); ok {
				errs = append(errs, schemaCheck(root, ps, child, path+"."+k)...)
				continue
			}
			switch ap := s["additionalProperties"].(type) {
			case bool:
				if !ap {
					errs = append(errs, fmt.Sprintf("%s: unknown key %q", path, k))
				}
			case map[string]any:
				errs = append(errs, schemaCheck(root, ap, child, path+"."+k)...)
			}
		}
	case []any:
		if items, ok := s["items"].(map[string]any); ok {
			for i, child := range val {
				errs = append(errs, schemaCheck(root, items, child, fmt.Sprintf("%s[%d]", path, i))...)
			}
		}
	}
	return errs
}

func typeMatches(typ any, v any) bool {
	if list, ok := typ.([]any); ok {
		for _, t := range list {
			if typeMatches(t, v) {
				return true
			}
		}
		return false
	}
	switch typ {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "integer":
		_, ok := v.(int)
		return ok
	case "number":
		switch v.(type) {
		case int, float64:
			return true
		}
		return false
	}
	return true
}

func yamlToAny(t *testing.T, y string) any {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(y), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestJSONSchema_AcceptsSpecAndDesignExamples(t *testing.T) {
	root := loadSchema(t)
	for name, y := range map[string]string{"02§10": sampleSpecYAML, "design§4": designFullYAML} {
		if errs := schemaCheck(root, root, yamlToAny(t, y), "$"); len(errs) > 0 {
			t.Errorf("%s rejected by JSON Schema: %v", name, errs)
		}
	}
}

func TestJSONSchema_ReadFromStandby(t *testing.T) {
	root := loadSchema(t)
	for _, rf := range []string{"primary", "replica", "standby"} {
		y := strings.Replace(sampleSpecYAML, "read_from: replica", "read_from: "+rf, 1)
		if errs := schemaCheck(root, root, yamlToAny(t, y), "$"); len(errs) > 0 {
			t.Errorf("read_from=%s rejected: %v", rf, errs)
		}
	}
	y := strings.Replace(sampleSpecYAML, "read_from: replica", "read_from: bogus", 1)
	if errs := schemaCheck(root, root, yamlToAny(t, y), "$"); len(errs) == 0 {
		t.Error("read_from=bogus should be rejected by schema")
	}
	// Bộ kiểm phải bắt được key lạ, nếu không bài test trên vô nghĩa.
	y = strings.Replace(sampleSpecYAML, "tables:\n", "tables:\n  bogus: 1\n", 1)
	if errs := schemaCheck(root, root, yamlToAny(t, y), "$"); len(errs) == 0 {
		t.Error("tables.bogus should be rejected by schema")
	}
}

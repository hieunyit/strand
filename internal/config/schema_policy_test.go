package config_test

import (
	"strings"
	"testing"

	"strand/internal/config"
)

// P3-01: schema.additive mặc định auto, schema.destructive mặc định pause; giá trị lạ bị từ chối.
func TestSchemaPolicyDefaultsAndValidation(t *testing.T) {
	base := `version: 1
name: sp
source: { kind: mysql, dsn: "root@tcp(127.0.0.1:3306)/db" }
target: { kind: mysql, dsn: "root@tcp(127.0.0.1:3307)/db" }
tables: { include: ["app.orders"] }
resources: { spool: { path: /var/lib/strand/a } }
`
	cfg := mustLoad(t, base)
	if cfg.Schema.Additive != "auto" || cfg.Schema.Destructive != "pause" {
		t.Fatalf("defaults = %q/%q, want auto/pause", cfg.Schema.Additive, cfg.Schema.Destructive)
	}
	cfg = mustLoad(t, base+"schema: { additive: PAUSE, destructive: approve }\n")
	if cfg.Schema.Additive != "pause" || cfg.Schema.Destructive != "approve" {
		t.Fatalf("got %q/%q", cfg.Schema.Additive, cfg.Schema.Destructive)
	}
	for _, bad := range []string{"schema: { additive: yes }\n", "schema: { destructive: auto }\n"} {
		if _, err := config.LoadYAMLStrict([]byte(base + bad)); err == nil || !strings.Contains(err.Error(), "schema.") {
			t.Fatalf("%q: want validation error, got %v", bad, err)
		}
	}
	// Không còn cảnh báo "chưa triển khai" cho schema.*.
	for _, k := range mustLoad(t, base+"schema: { additive: auto, destructive: approve }\n").UnimplementedKeys() {
		if strings.HasPrefix(k, "schema.") {
			t.Fatalf("schema.* is implemented but still reported: %s", k)
		}
	}
}

// P3-02: schema.online_ddl.tools mặc định [gh-ost, pt-osc]; `tools: []` tắt; giá trị lạ / pattern thiếu {table} bị từ chối.
func TestSchemaOnlineDDLConfig(t *testing.T) {
	base := `version: 1
name: sp
source: { kind: mysql, dsn: "root@tcp(127.0.0.1:3306)/db" }
target: { kind: mysql, dsn: "root@tcp(127.0.0.1:3307)/db" }
tables: { include: ["app.orders"] }
resources: { spool: { path: /var/lib/strand/a } }
`
	if got := mustLoad(t, base).Schema.OnlineDDL.Tools; len(got) != 2 || got[0] != "gh-ost" || got[1] != "pt-osc" {
		t.Fatalf("default tools = %v", got)
	}
	off := mustLoad(t, base+"schema: { online_ddl: { tools: [] } }\n").Schema.OnlineDDL.Tools
	if off == nil || len(off) != 0 {
		t.Fatalf("tools: [] must stay empty (disabled), got %#v", off)
	}
	one := mustLoad(t, base+"schema: { online_ddl: { tools: [GH-OST], extra_patterns: [\"_{table}_tmp\"] } }\n").Schema.OnlineDDL
	if len(one.Tools) != 1 || one.Tools[0] != "gh-ost" || len(one.ExtraPatterns) != 1 {
		t.Fatalf("got %+v", one)
	}
	for _, bad := range []string{"schema: { online_ddl: { tools: [osc] } }\n", "schema: { online_ddl: { extra_patterns: [\"_tmp\"] } }\n"} {
		if _, err := config.LoadYAMLStrict([]byte(base + bad)); err == nil || !strings.Contains(err.Error(), "online_ddl") {
			t.Fatalf("%q: want validation error, got %v", bad, err)
		}
	}
}

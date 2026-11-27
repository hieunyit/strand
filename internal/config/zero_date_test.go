package config_test

import (
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/exitcode"
)

// tables.zero_date / zero_date_tables (MySQL → PostgreSQL): giá trị hợp lệ, ghi đè theo bảng, và từ chối
// cấu hình sai hoặc cặp engine không áp dụng.
func TestZeroDateConfig(t *testing.T) {
	load := func(kinds, tables string) (*config.Config, error) {
		src, dst := "mysql", "postgres"
		if kinds != "" {
			src, dst, _ = strings.Cut(kinds, ">")
		}
		y := "version: 1\nname: zd\n" +
			"source: { kind: " + src + ", dsn: \"" + map[string]string{"mysql": "root@tcp(127.0.0.1:3306)/app", "postgres": "postgres://u@h/db"}[src] + "\" }\n" +
			"target: { kind: " + dst + ", dsn: \"" + map[string]string{"mysql": "root@tcp(127.0.0.1:3307)/strand", "postgres": "postgres://u@h2/db"}[dst] + "\" }\n" +
			"tables:\n  include: [\"app.*\"]\n" + tables
		return config.LoadYAMLStrict([]byte(y))
	}

	cfg, err := load("", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Tables.ZeroDateFor("app", "t"); got != config.ZeroDateReject || cfg.Tables.ZeroDateUsed() {
		t.Fatalf("default: ZeroDateFor=%q used=%v, want reject/false", got, cfg.Tables.ZeroDateUsed())
	}

	cfg, err = load("", "  zero_date: epoch\n  zero_date_tables:\n    app.Audit: SET_NULL\n    app.inv: reject\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ schema, table, want string }{
		{"app", "t", config.ZeroDateEpoch},
		{"app", "audit", config.ZeroDateSetNull}, // khóa so không phân biệt hoa thường
		{"app", "inv", config.ZeroDateReject},
	} {
		if got := cfg.Tables.ZeroDateFor(c.schema, c.table); got != c.want {
			t.Errorf("ZeroDateFor(%s.%s) = %q, want %q", c.schema, c.table, got, c.want)
		}
	}

	for _, c := range []struct{ kinds, tables, want string }{
		// YAML đọc `null` không nháy thành rỗng; "null" có nháy không phải giá trị hợp lệ.
		{"", "  zero_date: \"null\"\n", "want reject | set_null | epoch"},
		{"", "  zero_date: zero\n", "invalid tables.zero_date"},
		{"", "  zero_date_tables:\n    audit: epoch\n", "must be schema.table"},
		{"", "  zero_date_tables:\n    app.audit: nul\n", "zero_date_tables[app.audit]"},
		{"mysql>mysql", "  zero_date: epoch\n", "only apply to MySQL → PostgreSQL"},
		{"postgres>postgres", "  zero_date_tables:\n    app.t: set_null\n", "only apply to MySQL → PostgreSQL"},
	} {
		_, err := load(c.kinds, c.tables)
		if err == nil || exitcode.GetExitCode(err) != exitcode.InvalidSyntax || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s %q: want exit %d containing %q, got %v", c.kinds, c.tables, exitcode.InvalidSyntax, c.want, err)
		}
	}
	// reject tường minh hợp lệ với mọi cặp.
	if _, err := load("mysql>mysql", "  zero_date: reject\n"); err != nil {
		t.Fatalf("explicit reject on MySQL → MySQL: %v", err)
	}
}

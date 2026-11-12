package mysqlver

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		raw          string
		maj, min, pt int
		flavor       Flavor
	}{
		{"5.7.44-log", 5, 7, 44, FlavorMySQL},
		{"5.7.44", 5, 7, 44, FlavorMySQL},
		{"5.7.44-48-log", 5, 7, 44, FlavorMySQL},
		{"5.7.44-48-percona-log", 5, 7, 44, FlavorPercona},
		{"8.0.36-0ubuntu0.22.04.1", 8, 0, 36, FlavorMySQL},
		{"8.4.0", 8, 4, 0, FlavorMySQL},
		{"9.7.0-debug", 9, 7, 0, FlavorMySQL},
		{"5.6.51-log", 5, 6, 51, FlavorMySQL},
		{"10.6.12-MariaDB-1:10.6.12+maria~ubu2004", 10, 6, 12, FlavorMariaDB},
		{"5.5.5-10.3.0-MariaDB", 10, 3, 0, FlavorMariaDB},
		{"8.0", 8, 0, 0, FlavorMySQL},
	}
	for _, c := range cases {
		v, err := Parse(c.raw)
		if err != nil {
			t.Fatalf("%q: %v", c.raw, err)
		}
		if v.Major != c.maj || v.Minor != c.min || v.Patch != c.pt || v.Flavor != c.flavor {
			t.Errorf("%q: got %+v", c.raw, v)
		}
	}
	for _, bad := range []string{"", "abc", "x.y.z"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
}

func mustV(t *testing.T, s string) Version {
	t.Helper()
	v, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestAtLeast(t *testing.T) {
	v := mustV(t, "8.0.17")
	for _, c := range []struct {
		a, b, c int
		want    bool
	}{{8, 0, 17, true}, {8, 0, 18, false}, {8, 0, 16, true}, {7, 9, 99, true}, {8, 1, 0, false}, {9, 0, 0, false}} {
		if got := v.AtLeast(c.a, c.b, c.c); got != c.want {
			t.Errorf("8.0.17 AtLeast(%d,%d,%d)=%v", c.a, c.b, c.c, got)
		}
	}
}

func TestFeatureMatrix(t *testing.T) {
	type f struct{ rvo, rm, pad, exp, clone, sro, castD bool }
	cases := map[string]f{
		"5.7.44-log": {false, false, false, false, false, true, false},
		"5.7.7":      {false, false, false, false, false, false, false},
		"8.0.0":      {false, false, true, false, false, true, false},
		"8.0.2":      {false, true, true, true, false, true, false},
		"8.0.3":      {true, true, true, true, false, true, false},
		"8.0.17":     {true, true, true, true, true, true, true},
		"8.4.0":      {true, true, true, true, true, true, true},
		"9.7.0":      {true, true, true, true, true, true, true},
	}
	for raw, want := range cases {
		v := mustV(t, raw)
		got := f{v.HasBinlogRowValueOptions(), v.HasBinlogRowMetadata(), v.HasPadAttribute(),
			v.HasBinlogExpireLogsSeconds(), v.HasClone(), v.HasSuperReadOnly(), v.HasCastAsDouble()}
		if got != want {
			t.Errorf("%s: got %+v want %+v", raw, got, want)
		}
	}
}

func TestLegacy57(t *testing.T) {
	if !mustV(t, "5.7.44").Legacy57() || mustV(t, "8.0.36").Legacy57() || mustV(t, "10.6.1-MariaDB").Legacy57() {
		t.Fatal("Legacy57 wrong")
	}
}

func TestCheckSupported(t *testing.T) {
	// 5.5/5.6 được hỗ trợ (chế độ vị trí binlog khi không có GTID); dưới 5.5 bị từ chối. MariaDB: chỉ 10.6.
	for _, ok := range []string{"5.5.62", "5.6.51-log", "5.7.0", "5.7.44-log", "8.0.36", "8.4.0", "9.7.0",
		"10.6.3-MariaDB-1:10.6.3+maria~focal-log", "5.5.5-10.6.12-MariaDB"} {
		if err := CheckSupported(mustV(t, ok)); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"5.1.73", "5.0.96"} {
		err := CheckSupported(mustV(t, bad))
		if err == nil || !strings.Contains(err.Error(), "MySQL source 5.5 or later is required") {
			t.Errorf("%s: err=%v", bad, err)
		}
	}
	for _, bad := range []string{"5.5.5-10.3.0-MariaDB", "10.5.20-MariaDB", "10.11.6-MariaDB", "11.4.2-MariaDB"} {
		err := CheckSupported(mustV(t, bad))
		if err == nil || !strings.Contains(err.Error(), "only MariaDB 10.6 is supported") {
			t.Errorf("%s: err=%v", bad, err)
		}
	}
}

// MariaDB 10.6 có số phiên bản > 8.x nhưng không có các tính năng riêng của MySQL 8: các cờ phải tắt,
// binlog đọc bằng flavor mariadb, đích chỉ MySQL.
func TestMariaDBFeatureFlags(t *testing.T) {
	v := mustV(t, "10.6.3-MariaDB-log")
	if v.HasSuperReadOnly() || v.HasClone() || v.HasPadAttribute() || v.HasBinlogRowValueOptions() || v.HasBinlogRowMetadata() || v.Legacy57() {
		t.Fatalf("MariaDB feature flags wrong: %+v", v)
	}
	if v.SyncerFlavor() != "mariadb" || mustV(t, "8.0.36").SyncerFlavor() != "mysql" || mustV(t, "5.6.51").SyncerFlavor() != "mysql" {
		t.Fatal("SyncerFlavor wrong")
	}
	if a := v.PersistAdvice("x", "1"); strings.Contains(a, "PERSIST") {
		t.Fatalf("MariaDB has no SET PERSIST: %s", a)
	}
	if err := CheckTargetForSource(v, "postgres"); err == nil {
		t.Fatal("MariaDB -> postgres must be rejected")
	}
	if err := CheckTargetForSource(v, "mysql"); err != nil {
		t.Fatal(err)
	}
	if err := CheckTargetForSource(mustV(t, "5.7.44"), "postgres"); err != nil {
		t.Fatal(err)
	}
}

func TestCheckClone(t *testing.T) {
	if err := CheckClone(mustV(t, "5.7.44")); err == nil || !strings.Contains(err.Error(), "8.0.17") {
		t.Fatalf("5.7: %v", err)
	}
	if err := CheckClone(mustV(t, "8.0.16")); err == nil {
		t.Fatal("8.0.16 must be rejected")
	}
	if err := CheckClone(mustV(t, "8.0.17")); err != nil {
		t.Fatal(err)
	}
}

func TestQueries(t *testing.T) {
	old, nw := mustV(t, "5.7.44"), mustV(t, "8.4.0")
	if q := BinlogSettingsQuery(old); strings.Contains(q, "binlog_row_value_options") {
		t.Errorf("5.7 query must not read binlog_row_value_options: %s", q)
	}
	if q := BinlogSettingsQuery(nw); !strings.Contains(q, "@@GLOBAL.binlog_row_value_options") {
		t.Errorf("8.x query must read binlog_row_value_options: %s", q)
	}
	if q := BinlogRetentionQuery(old); strings.Contains(q, "binlog_expire_logs_seconds") || !strings.Contains(q, "expire_logs_days") {
		t.Errorf("5.7 retention: %s", q)
	}
	if q := BinlogRetentionQuery(nw); q != "SELECT @@GLOBAL.binlog_expire_logs_seconds" {
		t.Errorf("8.x retention: %s", q)
	}
}

func TestPersistAdvice(t *testing.T) {
	if got := mustV(t, "8.0.36").PersistAdvice("binlog_row_metadata", "FULL"); got != "SET PERSIST binlog_row_metadata = FULL" {
		t.Errorf("8.x: %s", got)
	}
	got := mustV(t, "5.7.44").PersistAdvice("x", "1")
	if strings.Contains(got, "SET PERSIST") || !strings.Contains(got, "SET GLOBAL x = 1") || !strings.Contains(got, "my.cnf") {
		t.Errorf("5.7: %s", got)
	}
}

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{
		"utf8_general_ci": "utf8mb3_general_ci", "UTF8_BIN": "utf8mb3_BIN", "utf8mb3_general_ci": "utf8mb3_general_ci",
		"utf8mb4_general_ci": "utf8mb4_general_ci", "latin1_swedish_ci": "latin1_swedish_ci", "": "", "C": "C",
	} {
		if got := NormalizeCollation(in); got != want {
			t.Errorf("collation %q: %q want %q", in, got, want)
		}
	}
	if NormalizeCharset("utf8") != "utf8mb3" || NormalizeCharset("utf8mb4") != "utf8mb4" || NormalizeCharset("UTF8") != "utf8mb3" {
		t.Error("charset")
	}
}

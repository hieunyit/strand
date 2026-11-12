package schemagen

import (
	"strings"
	"testing"
)

// Tập collation giả của đích MySQL 8.4 (đủ cho các ca dưới đây).
func fakeTarget(names ...string) func(string) bool {
	m := map[string]bool{}
	for _, n := range names {
		m[n] = true
	}
	return func(c string) bool { return m[strings.ToLower(c)] }
}

var mysql84 = fakeTarget("utf8mb4_general_ci", "utf8mb4_bin", "utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci",
	"utf8mb4_0900_ai_ci", "utf8mb4_0900_as_ci", "utf8mb4_0900_as_cs", "utf8mb4_0900_bin",
	"utf8mb3_general_ci", "utf8mb3_unicode_ci", "utf8mb3_bin", "latin1_swedish_ci", "latin1_bin")

func TestMapMariaDBCollation(t *testing.T) {
	cases := []struct {
		src, to string
		exact   bool
		ok      bool
	}{
		{"utf8mb4_general_ci", "utf8mb4_general_ci", true, true}, // đích có sẵn
		{"utf8mb4_nopad_bin", "utf8mb4_0900_bin", true, true},    // tương đương hoàn toàn
		{"utf8mb4_general_nopad_ci", "utf8mb4_general_ci", false, true},
		{"utf8mb4_unicode_nopad_ci", "utf8mb4_unicode_ci", false, true},
		{"utf8mb4_unicode_520_nopad_ci", "utf8mb4_unicode_520_ci", false, true},
		{"utf8mb3_nopad_bin", "utf8mb3_bin", false, true},
		{"latin1_nopad_bin", "latin1_bin", false, true},
		{"latin1_swedish_nopad_ci", "latin1_swedish_ci", false, true},
		{"utf8mb4_uca1400_ai_ci", "utf8mb4_0900_ai_ci", false, true},
		{"utf8mb4_uca1400_as_ci", "utf8mb4_0900_as_ci", false, true},
		{"utf8mb4_uca1400_as_cs", "utf8mb4_0900_as_cs", false, true},
		{"utf8mb4_uca1400_ai_cs", "utf8mb4_0900_as_cs", false, true},
		{"utf8mb4_uca1400_nopad_ai_ci", "utf8mb4_0900_ai_ci", false, true},
		{"utf8mb4_uca1400_czech_ai_ci", "utf8mb4_0900_ai_ci", false, true},
		{"utf8mb4_uca1400_spanish2_nopad_as_cs", "utf8mb4_0900_as_cs", false, true},
		{"utf8mb3_uca1400_ai_ci", "utf8mb3_unicode_ci", false, true},
		{"utf8mb3_uca1400_as_cs", "utf8mb3_bin", false, true},
		{"ucs2_uca1400_ai_ci", "", false, false}, // charset không có ánh xạ
		{"utf16_nopad_bin", "", false, false},
		{"utf8mb4_totally_unknown", "", false, false},
	}
	for _, c := range cases {
		m, ok := MapMariaDBCollation(c.src, mysql84, true)
		if ok != c.ok || ok && (m.To != c.to || m.Exact != c.exact) {
			t.Errorf("%s: got %+v ok=%v, want to=%s exact=%v ok=%v", c.src, m, ok, c.to, c.exact, c.ok)
		}
	}
	// Đích MySQL 8.0 cũ hơn không có ánh xạ nếu đích không có collation đó.
	if _, ok := MapMariaDBCollation("utf8mb4_nopad_bin", fakeTarget("utf8mb4_general_ci"), true); ok {
		t.Error("không có 0900_bin trên đích thì không được ánh xạ")
	}
}

func TestPlanCollations(t *testing.T) {
	// Cột thường dùng collation gần đúng: ánh xạ kèm cảnh báo nêu cột và collation đích.
	var p *CollationPlan
	var err error
	p, err = planCollations("app.t", mysql84, true, []colColl{
		{name: "name", coll: "utf8mb4_uca1400_ai_ci"},
		{name: "code", coll: "utf8mb4_nopad_bin", key: true},
		{name: "ok", coll: "utf8mb4_general_ci", key: true},
	}, "utf8mb4_uca1400_ai_ci")
	if err != nil {
		t.Fatal(err)
	}
	if p.Rewrite["utf8mb4_uca1400_ai_ci"] != "utf8mb4_0900_ai_ci" || p.Rewrite["utf8mb4_nopad_bin"] != "utf8mb4_0900_bin" || len(p.Rewrite) != 2 {
		t.Errorf("rewrite = %v", p.Rewrite)
	}
	joined := strings.Join(p.Warnings, "\n")
	if !strings.Contains(joined, "app.t column name: source collation utf8mb4_uca1400_ai_ci mapped to utf8mb4_0900_ai_ci") ||
		!strings.Contains(joined, "app.t table default") || strings.Contains(joined, "column code") {
		t.Errorf("warnings = %q", joined)
	}

	// Cột khóa với collation gần đúng: từ chối, nêu cột.
	for _, key := range []string{"utf8mb4_uca1400_ai_ci", "utf8mb4_general_nopad_ci"} {
		_, err = planCollations("app.t", mysql84, true, []colColl{{name: "pk", coll: key, key: true}}, "utf8mb4_general_ci")
		if err == nil || !strings.Contains(err.Error(), "app.t.pk uses "+key) || !strings.Contains(err.Error(), "PRIMARY KEY or UNIQUE") {
			t.Errorf("key %s: err = %v", key, err)
		}
	}
	// Collation không có ánh xạ: từ chối kể cả cột thường.
	_, err = planCollations("app.t", mysql84, true, []colColl{{name: "u", coll: "ucs2_uca1400_ai_ci"}}, "")
	if err == nil || !strings.Contains(err.Error(), "not available on the target and without a mapping") {
		t.Errorf("unmapped: err = %v", err)
	}
	// utf8 (MariaDB < 10.6) là utf8mb3 trên đích: không cần ánh xạ.
	p, err = planCollations("app.t", mysql84, true, []colColl{{name: "u", coll: "utf8_general_ci", key: true}}, "utf8_general_ci")
	if err != nil || len(p.Rewrite) != 0 || len(p.Warnings) != 0 {
		t.Errorf("utf8_general_ci: %+v %v", p, err)
	}
}

// Cột kế thừa mặc định của bảng: bảng ánh xạ gần đúng thì cột khóa ngầm dùng collation đó cũng phải bị từ chối
// (planCollations đọc collation hiệu lực từ information_schema.COLUMNS, không suy từ DDL).
func TestPlanCollationsImplicitKeyColumn(t *testing.T) {
	_, err := planCollations("app.t", mysql84, true, []colColl{{name: "name", coll: "utf8mb4_uca1400_ai_ci", key: true}}, "utf8mb4_uca1400_ai_ci")
	if err == nil {
		t.Fatal("khóa kế thừa uca1400 mặc định của bảng phải bị từ chối")
	}
}

func TestRewriteCollationsDDL(t *testing.T) {
	in := "CREATE TABLE `t` (\n" +
		"  `code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL,\n" +
		"  `name` varchar(20) DEFAULT 'COLLATE utf8mb4_nopad_bin',\n" +
		"  `l` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
		"  PRIMARY KEY (`code`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci"
	want := strings.NewReplacer("COLLATE utf8mb4_nopad_bin NOT NULL", "COLLATE utf8mb4_0900_bin NOT NULL",
		"COLLATE=utf8mb4_uca1400_ai_ci", "COLLATE=utf8mb4_0900_ai_ci").Replace(in)
	got, err := rewriteCollations(in, map[string]string{"utf8mb4_nopad_bin": "utf8mb4_0900_bin", "utf8mb4_uca1400_ai_ci": "utf8mb4_0900_ai_ci"})
	if err != nil || got != want {
		t.Fatalf("got:\n%s\nwant:\n%s\n(%v)", got, want, err)
	}
}

// Mặc định thật của charset trên nguồn được ghi vào DDL: 11.x là uca1400_ai_ci, không phải general_ci.
func TestPinUTF8MB4CollationRealDefault(t *testing.T) {
	in := "CREATE TABLE `t` (\n  `a` varchar(10) CHARACTER SET utf8mb4 DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	got, err := pinUTF8MB4Collation(in, "utf8mb4_uca1400_ai_ci")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci DEFAULT") || !strings.HasSuffix(got, "CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci") {
		t.Fatalf("got %s", got)
	}
}

func TestMariaDBHasPeriod(t *testing.T) {
	if !mariaDBHasPeriod("CREATE TABLE `t` (`id` int, `s` date, `e` date, PERIOD FOR `p` (`s`,`e`)) ENGINE=InnoDB") {
		t.Error("PERIOD FOR không được nhận ra")
	}
	if !mariaDBHasPeriod("CREATE TABLE `t` (`id` int, UNIQUE KEY `u` (`id`, `p` WITHOUT OVERLAPS)) ENGINE=InnoDB") {
		t.Error("WITHOUT OVERLAPS không được nhận ra")
	}
	if mariaDBHasPeriod("CREATE TABLE `t` (`period` int DEFAULT 'PERIOD FOR x', `for` int) ENGINE=InnoDB") {
		t.Error("nhận nhầm tên cột/chuỗi")
	}
}

// MariaDB 10.3/10.4: nopad_bin coi 'a' = 'a\x00' (lỗi đã sửa ở 10.5) nên không tương đương hoàn toàn với 0900_bin:
// cột khóa bị từ chối, cột thường ánh xạ kèm cảnh báo. Hồi quy: trước đây luôn coi là exact.
func TestPlanCollationsNopadBinOldMariaDB(t *testing.T) {
	m, ok := MapMariaDBCollation("utf8mb4_nopad_bin", mysql84, false)
	if !ok || m.To != "utf8mb4_0900_bin" || m.Exact {
		t.Fatalf("exactPeers=false: %+v ok=%v", m, ok)
	}
	if _, err := planCollations("app.t", mysql84, false, []colColl{{name: "pk", coll: "utf8mb4_nopad_bin", key: true}}, ""); err == nil {
		t.Fatal("nopad_bin trên khóa của MariaDB < 10.5 phải bị từ chối")
	}
	p, err := planCollations("app.t", mysql84, false, []colColl{{name: "c", coll: "utf8mb4_nopad_bin"}}, "")
	if err != nil || len(p.Warnings) != 1 {
		t.Fatalf("cột thường: %v %+v", err, p)
	}
}

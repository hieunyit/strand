package snapshot

import (
	"strings"
	"testing"

	"strand/internal/mysqlver"
)

func TestPKColumnQueryByVersion(t *testing.T) {
	v57, _ := mysqlver.Parse("5.7.44-log")
	v84, _ := mysqlver.Parse("8.4.0")
	if q := pkColumnQuery(v57); strings.Contains(q, "PAD_ATTRIBUTE") || strings.Contains(q, "COLLATIONS") {
		t.Errorf("5.7 query must not touch COLLATIONS.PAD_ATTRIBUTE: %s", q)
	}
	if q := pkColumnQuery(v84); !strings.Contains(q, "co.PAD_ATTRIBUTE") {
		t.Errorf("8.x query must read PAD_ATTRIBUTE: %s", q)
	}
}

// Nguồn 5.7 báo utf8_general_ci, đích 8.x báo utf8mb3_general_ci: SameOrder phải coi là cùng collation.
func TestSameOrderUtf8Alias(t *testing.T) {
	a, err := MySQLPKColumn("k", "varchar", "varchar(20)", "utf8", "utf8_general_ci", "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := MySQLPKColumn("k", "varchar", "varchar(20)", "utf8mb3", "utf8mb3_general_ci", "PAD SPACE")
	if err != nil {
		t.Fatal(err)
	}
	a.Collation, b.Collation = "utf8_general_ci", "utf8mb3_general_ci"
	if err := (&PKCodec{Cols: []PKColumn{a}}).SameOrder(&PKCodec{Cols: []PKColumn{b}}); err != nil {
		t.Fatalf("utf8 vs utf8mb3: %v", err)
	}
	b.Collation = "utf8mb3_bin"
	if err := (&PKCodec{Cols: []PKColumn{a}}).SameOrder(&PKCodec{Cols: []PKColumn{b}}); err == nil {
		t.Fatal("different collation must differ")
	}
}

// MariaDB utf8mb4_nopad_bin ↔ MySQL utf8mb4_0900_bin là cặp tương đương hoàn toàn (khóa dùng được); các
// collation chỉ gần đúng (general_nopad_ci ↔ general_ci) thì không. Hồi quy: trước đây SameOrder so tên nên
// bảng PK nopad_bin bị từ chối snapshot dù schemagen đã ánh xạ sang 0900_bin.
func TestSameOrderMariaDBExactPeer(t *testing.T) {
	mk := func(typ, cs, coll, pad string) PKColumn {
		c, err := MySQLPKColumn("k", "varchar", typ, cs, coll, pad)
		if err != nil {
			t.Fatal(err)
		}
		c.Collation = coll
		return c
	}
	a := mk("varchar(20)", "utf8mb4", "utf8mb4_nopad_bin", "NO PAD")
	b := mk("varchar(20)", "utf8mb4", "utf8mb4_0900_bin", "NO PAD")
	if err := (&PKCodec{Cols: []PKColumn{a}}).SameOrder(&PKCodec{Cols: []PKColumn{b}}); err != nil {
		t.Fatalf("nopad_bin vs 0900_bin: %v", err)
	}
	a2 := mk("varchar(20)", "utf8mb4", "utf8mb4_general_nopad_ci", "NO PAD")
	b2 := mk("varchar(20)", "utf8mb4", "utf8mb4_general_ci", "PAD SPACE")
	if err := (&PKCodec{Cols: []PKColumn{a2}}).SameOrder(&PKCodec{Cols: []PKColumn{b2}}); err == nil {
		t.Fatal("general_nopad_ci vs general_ci là gần đúng, không được coi là cùng thứ tự")
	}
}

package main

import (
	"database/sql"
	"testing"

	"strand/internal/model"
	"strand/internal/testutil"
)

// Review DBA: repair PG phải chỉ chạm chính relation như writer. Bảng cha inheritance thường: SELECT nguồn
// không được lấy row của bảng con, DELETE ở target không được xóa row bảng con trùng khóa.
func TestRepairPGInheritanceParentTouchesOnlyTheParent(t *testing.T) {
	db, err := sql.Open("pgx", testutil.NewPostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, s := range []string{"src", "dst"} {
		exec("CREATE SCHEMA " + s)
		exec("CREATE TABLE " + s + ".p (id bigint PRIMARY KEY, v text)")
		exec("CREATE TABLE " + s + ".c () INHERITS (" + s + ".p)")
	}
	exec("INSERT INTO src.p VALUES (1,'new')")
	exec("INSERT INTO src.c VALUES (5,'child-src')") // id 5 chỉ có ở bảng con nguồn
	exec("INSERT INTO dst.p VALUES (1,'old'),(5,'stale')")
	exec("INSERT INTO dst.c VALUES (5,'child-dst')") // row bảng con target trùng khóa 5

	if err := repairRows(ctx, db, db, model.EnginePG, model.EnginePG,
		model.TableID{Schema: "src", Table: "p"}, model.TableID{Schema: "dst", Table: "p"}, "id", []string{"1", "5"}); err != nil {
		t.Fatal(err)
	}
	got := func(q string) string {
		t.Helper()
		var s string
		if err := db.QueryRowContext(ctx, q).Scan(&s); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return s
	}
	if v := got("SELECT coalesce(string_agg(id || '=' || v, ',' ORDER BY id), '') FROM ONLY dst.p"); v != "1=new" {
		t.Fatalf("target parent after repair = %q, want 1=new (id 5 exists only in the source child)", v)
	}
	if v := got("SELECT coalesce(string_agg(id || '=' || v, ','), '') FROM dst.c"); v != "5=child-dst" {
		t.Fatalf("target child after repair = %q, want 5=child-dst untouched", v)
	}
}

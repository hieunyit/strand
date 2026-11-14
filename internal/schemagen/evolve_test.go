package schemagen

import (
	"strings"
	"testing"

	"strand/internal/model"
)

func col(name, typ string, nullable bool) ColumnDef {
	return ColumnDef{Name: name, Type: typ, Nullable: nullable, Def: "`" + name + "` " + typ}
}

func TestEvolveTypeRelMySQL(t *testing.T) {
	cases := []struct {
		old, new string
		want     rel
	}{
		{"varchar(10)", "varchar(20)", relWiden},
		{"varchar(10)", "varchar(10)", relSame},
		{"varchar(10)", "varchar(5)", relNarrow},
		{"varbinary(8)", "varbinary(16)", relWiden},
		{"binary(8)", "binary(16)", relOther},
		{"char(8)", "char(16)", relOther},
		{"varchar(10)", "text", relOther},
		{"int", "bigint", relWiden},
		{"int(11)", "int", relSame}, // độ rộng hiển thị
		{"int(11)", "bigint(20)", relWiden},
		{"tinyint(1)", "smallint", relWiden},
		{"smallint", "tinyint", relNarrow},
		{"int unsigned", "bigint unsigned", relWiden},
		{"int unsigned", "bigint", relOther}, // khác dấu: phá hủy
		{"int", "bigint unsigned", relOther},
		{"bigint", "int", relNarrow},
		{"int", "varchar(10)", relOther},
		{"decimal(10,2)", "decimal(12,2)", relWiden},
		{"decimal(10,2)", "decimal(12,3)", relOther},
		{"decimal(10,2)", "decimal(8,2)", relNarrow},
		{"decimal(10,2) unsigned", "decimal(12,2)", relOther},
		{"text", "mediumtext", relWiden},
		{"longtext", "text", relNarrow},
		{"tinyblob", "blob", relWiden},
		{"text", "blob", relOther},
		{"datetime", "datetime(6)", relOther},
		{"enum('a','b')", "enum('a','b','c')", relOther},
		{"enum('a','b')", "enum('a','b')", relSame},
		{"float", "double", relOther},
		{"int zerofill", "bigint zerofill", relWiden},
		{"int(5) zerofill", "int(6) zerofill", relOther},
	}
	for _, c := range cases {
		if got := typeRel(c.old, c.new, false); got != c.want {
			t.Errorf("mysql %q -> %q: got %d want %d", c.old, c.new, got, c.want)
		}
	}
}

func TestEvolveTypeRelPG(t *testing.T) {
	cases := []struct {
		old, new string
		want     rel
	}{
		{"smallint", "integer", relWiden},
		{"integer", "bigint", relWiden},
		{"bigint", "integer", relNarrow},
		{"bigint", "numeric(20,0)", relWiden},
		{"bigint", "numeric(20,2)", relOther},
		{"varchar(10)", "varchar(20)", relWiden},
		{"varchar(20)", "varchar(10)", relNarrow},
		{"varchar(10)", "text", relWiden},
		{"text", "varchar(10)", relNarrow},
		{"numeric(10,2)", "numeric(12,2)", relWiden},
		{"numeric(10,2)", "numeric(12,3)", relOther},
		{"timestamp", "timestamptz", relOther},
		{"integer", "integer", relSame},
		{"text[]", "text[]", relSame},
		{"bytea", "text", relOther},
	}
	for _, c := range cases {
		if got := typeRel(c.old, c.new, true); got != c.want {
			t.Errorf("pg %q -> %q: got %d want %d", c.old, c.new, got, c.want)
		}
	}
}

func TestEvolveNormalizePGType(t *testing.T) {
	for in, want := range map[string]string{
		"character varying(50)":          "varchar(50)",
		"character(3)":                   "char(3)",
		"timestamp without time zone":    "timestamp",
		"timestamp(3) without time zone": "timestamp(3)",
		"timestamp with time zone":       "timestamptz",
		"timestamp(6) with time zone":    "timestamptz(6)",
		"time(2) without time zone":      "time(2)",
		"numeric(10,2)":                  "numeric(10,2)",
		"double precision":               "double precision",
		"text[]":                         "text[]",
		"bit varying(8)":                 "bit varying(8)",
	} {
		if got := NormalizePGType(in); got != want {
			t.Errorf("NormalizePGType(%q) = %q want %q", in, got, want)
		}
	}
}

func pk(sig string) TableDef { return TableDef{PK: sig} }

func with(td TableDef, cols ...ColumnDef) TableDef { td.Columns = cols; return td }

func TestEvolveDiffClassifyMySQL(t *testing.T) {
	base := with(pk("id"), col("id", "bigint", false), col("v", "varchar(20)", false))
	tgt := model.TableID{Schema: "app", Table: "t"}
	type tc struct {
		name     string
		src      TableDef
		additive bool
		empty    bool
		stmt     string // chuỗi con mong đợi trong DDL (nếu có)
	}
	nullable := func(c ColumnDef) ColumnDef { c.Nullable = true; return c }
	withDef := func(c ColumnDef) ColumnDef { c.HasDefault = true; c.Def += " DEFAULT 0"; return c }
	idx := func(td TableDef, i ...IndexDef) TableDef { td.Indexes = i; return td }
	cases := []tc{
		{name: "identical", src: base, empty: true},
		{name: "add nullable column", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(20)", false), nullable(col("w", "int", false))),
			additive: true, stmt: "ADD COLUMN `w` int AFTER `v`"},
		{name: "add column with default", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(20)", false), withDef(col("w", "int", false))),
			additive: true, stmt: "ADD COLUMN `w` int DEFAULT 0 AFTER `v`"},
		{name: "add NOT NULL column without default", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(20)", false), col("w", "int", false)), additive: false},
		{name: "add column first", src: with(pk("id"), nullable(col("z", "int", false)), col("id", "bigint", false), col("v", "varchar(20)", false)), additive: true, stmt: "FIRST"},
		{name: "add generated column", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(20)", false),
			func() ColumnDef { c := nullable(col("g", "int", false)); c.Unsupported = "generated"; return c }()), additive: false},
		{name: "drop column", src: with(pk("id"), col("id", "bigint", false)), additive: false, stmt: "DROP COLUMN `v`"},
		{name: "rename column = drop+add", src: with(pk("id"), col("id", "bigint", false), nullable(col("v2", "varchar(20)", false))), additive: false},
		{name: "widen varchar", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(40)", false)), additive: true, stmt: "MODIFY COLUMN `v` varchar(40)"},
		{name: "narrow varchar", src: with(pk("id"), col("id", "bigint", false), col("v", "varchar(5)", false)), additive: false, stmt: "MODIFY COLUMN `v` varchar(5)"},
		{name: "NOT NULL to NULL", src: with(pk("id"), col("id", "bigint", false), nullable(col("v", "varchar(20)", false))), additive: true},
		{name: "widen and relax null", src: with(pk("id"), col("id", "bigint", false), nullable(col("v", "varchar(30)", false))), additive: true},
		{name: "widen plus drop other column is destructive", src: with(pk("id"), col("id", "bigint", false)), additive: false},
		{name: "change type family", src: with(pk("id"), col("id", "bigint", false), col("v", "int", false)), additive: false},
		{name: "pk type widen is still additive column change but pk same", src: with(pk("id"), col("id", "decimal(30,0)", false), col("v", "varchar(20)", false)), additive: false},
		{name: "change pk", src: with(pk("id,v"), col("id", "bigint", false), col("v", "varchar(20)", false)), additive: false},
		{name: "add non-unique index", src: idx(base, IndexDef{Name: "iv", Sig: "n|v", Cols: "`v`"}), additive: true, stmt: "CREATE INDEX `iv` ON `app`.`t` (`v`)"},
		{name: "add unique index", src: idx(base, IndexDef{Name: "uv", Unique: true, Sig: "u|v", Cols: "`v`"}), additive: false, stmt: "CREATE UNIQUE INDEX"},
		{name: "add prefix index manual", src: idx(base, IndexDef{Name: "pv", Sig: "n|v(5)", Cols: "`v`(5)", Manual: "prefix"}), additive: false},
		{name: "charset change", src: with(pk("id"), col("id", "bigint", false), func() ColumnDef {
			c := col("v", "varchar(20)", false)
			c.Charset, c.Collation = "latin1", "latin1_swedish_ci"
			return c
		}()), additive: false},
		{name: "column order change", src: with(pk("id"), col("v", "varchar(20)", false), col("id", "bigint", false)), additive: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := base
			if c.name == "charset change" {
				d := col("v", "varchar(20)", false)
				d.Charset, d.Collation = "utf8mb4", "utf8mb4_general_ci"
				dst = with(pk("id"), col("id", "bigint", false), d)
			}
			ev := Diff(c.src, dst, DiffOptions{Target: tgt})
			if ev.Empty() != c.empty {
				t.Fatalf("empty = %v, want %v (%s)", ev.Empty(), c.empty, ev.Describe())
			}
			if c.empty {
				return
			}
			if ev.Additive() != c.additive {
				t.Fatalf("additive = %v, want %v: %s", ev.Additive(), c.additive, ev.Describe())
			}
			if c.stmt != "" && !strings.Contains(strings.Join(ev.Statements(), "\n"), c.stmt) {
				t.Fatalf("statements %q lack %q", ev.Statements(), c.stmt)
			}
		})
	}
}

func TestEvolveDiffDropIndexAndChangeIndexMySQL(t *testing.T) {
	tgt := model.TableID{Schema: "app", Table: "t"}
	cols := []ColumnDef{col("id", "bigint", false), col("v", "varchar(20)", false)}
	src := TableDef{PK: "id", Columns: cols}
	dst := TableDef{PK: "id", Columns: cols, Indexes: []IndexDef{{Name: "iv", Sig: "n|v", Cols: "`v`"}}}
	ev := Diff(src, dst, DiffOptions{Target: tgt})
	if ev.Additive() || len(ev.Statements()) != 1 || ev.Statements()[0] != "DROP INDEX `iv` ON `app`.`t`" {
		t.Fatalf("drop index: %v %s", ev.Statements(), ev.Describe())
	}
	// Cùng tên, khác cột: DROP rồi CREATE, phá hủy.
	src.Indexes = []IndexDef{{Name: "iv", Sig: "n|id,v", Cols: "`id`, `v`"}}
	ev = Diff(src, dst, DiffOptions{Target: tgt})
	st := ev.Statements()
	if ev.Additive() || len(st) != 2 || !strings.HasPrefix(st[0], "DROP INDEX") || !strings.HasPrefix(st[1], "CREATE INDEX") {
		t.Fatalf("change index: %v", st)
	}
	// Có tuỳ chọn bỏ qua index thừa ở target (PG): không còn khác biệt.
	src.Indexes = nil
	if ev = Diff(src, dst, DiffOptions{Target: tgt, IgnoreExtraIndexes: true}); !ev.Empty() {
		t.Fatalf("ignore extra indexes: %s", ev.Describe())
	}
}

func TestEvolveDiffOrderOfStatements(t *testing.T) {
	tgt := model.TableID{Schema: "app", Table: "t"}
	dst := TableDef{PK: "id", Columns: []ColumnDef{col("id", "bigint", false), col("old", "int", true)},
		Indexes: []IndexDef{{Name: "io", Sig: "n|old", Cols: "`old`"}}}
	n := col("new", "int", true)
	src := TableDef{PK: "id", Columns: []ColumnDef{col("id", "bigint", false), n},
		Indexes: []IndexDef{{Name: "inew", Sig: "n|new", Cols: "`new`"}}}
	st := Diff(src, dst, DiffOptions{Target: tgt}).Statements()
	want := []string{"DROP INDEX", "DROP COLUMN", "ADD COLUMN", "CREATE INDEX"}
	if len(st) != len(want) {
		t.Fatalf("statements: %v", st)
	}
	for i, w := range want {
		if !strings.Contains(st[i], w) {
			t.Fatalf("statement %d = %q, want %q (all: %v)", i, st[i], w, st)
		}
	}
}

func TestEvolveDiffForceManualAndManualStatements(t *testing.T) {
	tgt := model.TableID{Schema: "app", Table: "t"}
	src := with(pk("id"), col("id", "bigint", false), col("w", "int", true))
	dst := with(pk("id"), col("id", "bigint", false))
	ev := Diff(src, dst, DiffOptions{Target: tgt, ForceManual: "MariaDB"})
	if ev.Additive() {
		t.Fatal("ForceManual must be destructive")
	}
	st := ev.Statements()
	if len(st) != 1 || !IsManualStatement(st[0]) {
		t.Fatalf("want one manual statement, got %v", st)
	}
	if IsManualStatement("ALTER TABLE x") {
		t.Fatal("not manual")
	}
}

func TestEvolveDiffPG(t *testing.T) {
	// Rename mapping: bảng đích khác tên bảng nguồn; DDL phải nhắm bảng đích.
	tgt := model.TableID{Schema: "public", Table: "app_users"}
	pgcol := func(name, typ string, nullable bool) ColumnDef {
		return ColumnDef{Name: name, Type: typ, Nullable: nullable, Def: `"` + name + `" ` + typ}
	}
	dst := TableDef{PK: "id", Columns: []ColumnDef{pgcol("id", "bigint", false), pgcol("v", "varchar(20)", false)}}
	opts := DiffOptions{PG: true, Target: tgt, LenientWiderTarget: true, IgnoreExtraIndexes: true}

	add := TableDef{PK: "id", Columns: []ColumnDef{pgcol("id", "bigint", false), pgcol("v", "varchar(20)", false), pgcol("w", "integer", true)}}
	ev := Diff(add, dst, opts)
	if !ev.Additive() || ev.Statements()[0] != `ALTER TABLE "public"."app_users" ADD COLUMN "w" integer` {
		t.Fatalf("pg add column: %v %s", ev.Statements(), ev.Describe())
	}
	widen := TableDef{PK: "id", Columns: []ColumnDef{pgcol("id", "bigint", false), pgcol("v", "varchar(40)", true)}}
	ev = Diff(widen, dst, opts)
	st := strings.Join(ev.Statements(), ";")
	if !ev.Additive() || !strings.Contains(st, `ALTER COLUMN "v" TYPE varchar(40)`) || !strings.Contains(st, `ALTER COLUMN "v" DROP NOT NULL`) {
		t.Fatalf("pg widen: %v", ev.Statements())
	}
	drop := TableDef{PK: "id", Columns: []ColumnDef{pgcol("id", "bigint", false)}}
	ev = Diff(drop, dst, opts)
	if ev.Additive() || ev.Statements()[0] != `ALTER TABLE "public"."app_users" DROP COLUMN "v"` {
		t.Fatalf("pg drop: %v", ev.Statements())
	}
	// Target rộng hơn typemap (DBA tự tạo): không phải thay đổi; không lenient thì là narrow = phá hủy.
	narrow := TableDef{PK: "id", Columns: []ColumnDef{pgcol("id", "bigint", false), pgcol("v", "varchar(5)", false)}}
	if ev = Diff(narrow, dst, opts); !ev.Empty() {
		t.Fatalf("lenient wider target: %s", ev.Describe())
	}
	opts.LenientWiderTarget = false
	if ev = Diff(narrow, dst, opts); ev.Additive() || ev.Empty() {
		t.Fatalf("strict narrow must be destructive: %s", ev.Describe())
	}
	// Thứ tự cột không được so ở PG (ADD COLUMN luôn nối cuối).
	opts.LenientWiderTarget = true
	swapped := TableDef{PK: "id", Columns: []ColumnDef{pgcol("v", "varchar(20)", false), pgcol("id", "bigint", false)}}
	if ev = Diff(swapped, dst, opts); !ev.Empty() {
		t.Fatalf("pg ignores order: %s", ev.Describe())
	}
	// Index mới trên bảng đã đổi tên, tên index theo schemagen; unique là phá hủy.
	ix := TableDef{PK: "id", Columns: dst.Columns, Indexes: []IndexDef{{Name: "app_users_iv", Sig: "n|v", Cols: `"v"`}}}
	ev = Diff(ix, dst, opts)
	if !ev.Additive() || ev.Statements()[0] != `CREATE INDEX "app_users_iv" ON "public"."app_users" ("v")` {
		t.Fatalf("pg index: %v", ev.Statements())
	}
	ix.Indexes[0].Unique, ix.Indexes[0].Sig = true, "u|v"
	if ev = Diff(ix, dst, opts); ev.Additive() {
		t.Fatal("unique index must be destructive")
	}
	// Narrow có USING cast.
	opts.LenientWiderTarget = false
	ev = Diff(narrow, dst, opts)
	if !strings.Contains(ev.Statements()[0], `TYPE varchar(5) USING "v"::varchar(5)`) {
		t.Fatalf("pg narrow stmt: %v", ev.Statements())
	}
}

func TestEvolveColumnDefsFromCreate(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `id` bigint NOT NULL,\n  `we``ird` varchar(10) CHARACTER SET latin1 DEFAULT 'a,b',\n  PRIMARY KEY (`id`),\n  KEY `k` (`id`)\n) ENGINE=InnoDB"
	got := columnDefsFromCreate(ddl)
	if got["id"] != "`id` bigint NOT NULL" || got["we`ird"] != "`we``ird` varchar(10) CHARACTER SET latin1 DEFAULT 'a,b'" || len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

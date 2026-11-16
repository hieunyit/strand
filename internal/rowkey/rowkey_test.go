package rowkey

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"strand/internal/model"
)

func ns(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

func row(idx, col, nullable string) MySQLIdxRow {
	return MySQLIdxRow{Index: idx, Column: ns(col), Nullable: nullable}
}

func TestMySQLPrimaryWins(t *testing.T) {
	info := BuildMySQL([]MySQLIdxRow{row("PRIMARY", "id", "NO"), row("uk", "a", "NO")})
	res := info.Resolve(nil)
	if res.Key == nil || !res.Key.IsPrimary() || res.Key.Label() != "PRIMARY (id)" {
		t.Fatalf("key=%+v", res.Key)
	}
}

func TestMySQLIneligibleCandidates(t *testing.T) {
	rows := []MySQLIdxRow{
		row("uk_null", "a", "YES"),
		{Index: "uk_prefix", Column: ns("code"), SubPart: sql.NullInt64{Int64: 10, Valid: true}, Nullable: "NO"},
		{Index: "uk_func", Column: sql.NullString{}, Nullable: ""},
		{Index: "uk_gen", Column: ns("g"), Nullable: "NO", Extra: "VIRTUAL GENERATED"},
		row("uk_ok2", "a", "NO"), row("uk_ok2", "b", "NO"),
	}
	res := BuildMySQL(rows).Resolve(nil)
	if res.Key == nil || res.Key.Name != "uk_ok2" {
		t.Fatalf("key=%+v rejected=%s", res.Key, Explain(res.Rejected))
	}
	if len(res.Rejected) != 4 {
		t.Fatalf("rejected=%d: %s", len(res.Rejected), Explain(res.Rejected))
	}
	for _, want := range []string{"nullable", "prefix", "expression", "generated"} {
		if !strings.Contains(Explain(res.Rejected), want) {
			t.Errorf("explain lacks %q: %s", want, Explain(res.Rejected))
		}
	}
	// DEFAULT_GENERATED (MySQL 8 expression default) is NOT a generated column.
	if r := BuildMySQL([]MySQLIdxRow{{Index: "u", Column: ns("c"), Nullable: "NO", Extra: "DEFAULT_GENERATED"}}).Resolve(nil); r.Key == nil {
		t.Fatal("DEFAULT_GENERATED column must be eligible")
	}
}

func TestChooseFewestColumnsThenNameThenType(t *testing.T) {
	cands := []Candidate{
		{Name: "z1", Columns: []string{"a"}},
		{Name: "a2", Columns: []string{"a", "b"}},
		{Name: "b1", Columns: []string{"b"}},
	}
	k, _ := Choose(cands, nil)
	if k.Name != "b1" {
		t.Fatalf("got %s, want b1 (1 column, name order b1<z1)", k.Name)
	}
	// unsupported type on the first choice: try the next
	k, rej := Choose(cands, func(cols []string) error {
		if cols[0] == "b" {
			return errors.New("blob")
		}
		return nil
	})
	if k.Name != "z1" || len(rej) != 1 || !strings.Contains(rej[0].Reason, "unsupported key type") {
		t.Fatalf("got %+v rej=%+v", k, rej)
	}
	// none usable
	k, _ = Choose(cands, func([]string) error { return errors.New("x") })
	if k != nil {
		t.Fatal("expected nil key")
	}
	// order independent of input order
	rev := []Candidate{cands[2], cands[1], cands[0]}
	if k, _ := Choose(rev, nil); k.Name != "b1" {
		t.Fatal("not deterministic")
	}
}

func TestKeyLabel(t *testing.T) {
	k := Key{Name: "uk_email", Columns: []string{"email"}}
	if k.Label() != "UNIQUE uk_email (email)" {
		t.Fatal(k.Label())
	}
}

func pgRow(idx string, col string, notNull bool) PGIdxRow {
	return PGIdxRow{Index: idx, Column: ns(col), NotNull: notNull, Immediate: true, Valid: true, RelReplIdent: "d"}
}

func TestPGKeyFromReplicaIdentityIndex(t *testing.T) {
	a := pgRow("uk", "a", true)
	a.Replident, a.RelReplIdent = true, "i"
	o := pgRow("other", "b", true)
	o.RelReplIdent = "i"
	res := BuildPG([]PGIdxRow{a, o}).Resolve(nil)
	if res.Key == nil || res.Key.Name != "uk" {
		t.Fatalf("key=%+v", res.Key)
	}
}

func TestPGNoReplicaIdentitySuggestsIndex(t *testing.T) {
	// DEFAULT identity and no PK: not a key, but an eligible index is suggested.
	partial := pgRow("p", "a", true)
	partial.Partial = true
	def := pgRow("d", "b", true)
	def.Immediate = false
	res := BuildPG([]PGIdxRow{partial, def, pgRow("nullable", "c", false), pgRow("z_good", "e", true)}).Resolve(nil)
	if res.Key != nil {
		t.Fatalf("DEFAULT replica identity without PK must not yield a key: %+v", res.Key)
	}
	if res.Suggested != "z_good" {
		t.Fatalf("suggested=%q rejected=%s", res.Suggested, Explain(res.Rejected))
	}
	for _, want := range []string{"partial", "deferrable", "nullable"} {
		if !strings.Contains(Explain(res.Rejected), want) {
			t.Errorf("explain lacks %q", want)
		}
	}
	// REPLICA IDENTITY FULL without PK is not a key either.
	f := pgRow("good", "e", true)
	f.RelReplIdent = "f"
	if r := BuildPG([]PGIdxRow{f}).Resolve(nil); r.Key != nil {
		t.Fatal("FULL identity must not yield a key")
	}
	// PK wins.
	p := pgRow("t_pkey", "id", true)
	p.Primary = true
	if r := BuildPG([]PGIdxRow{p}).Resolve(nil); r.Key == nil || !r.Key.IsPrimary() {
		t.Fatal("PK expected")
	}
}

func TestSuggestPerFlavor(t *testing.T) {
	tbl := model.TableID{Schema: "app", Table: "t"}
	cases := []struct {
		name string
		in   SuggestInput
		want []string
		not  []string
	}{
		{"mariadb10.6", SuggestInput{Flavor: "mariadb", Major: 10, Minor: 6, Table: tbl},
			[]string{"ADD COLUMN `_strand_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, ADD PRIMARY KEY (`_strand_id`)"}, []string{"WARNING"}},
		{"mariadb10.2", SuggestInput{Flavor: "mariadb", Major: 10, Minor: 2, Table: tbl},
			[]string{"WARNING", "AUTO_INCREMENT, ADD PRIMARY KEY"}, []string{"INVISIBLE,"}},
		{"mysql8.0.23", SuggestInput{Flavor: "mysql", Major: 8, Minor: 0, Patch: 23, Table: tbl},
			[]string{"`my_row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE"}, []string{"sql_generate_invisible_primary_key"}},
		{"mysql8.0.36", SuggestInput{Flavor: "mysql", Major: 8, Minor: 0, Patch: 36, Table: tbl},
			[]string{"INVISIBLE", "sql_generate_invisible_primary_key"}, nil},
		{"mysql8.0.22", SuggestInput{Flavor: "mysql", Major: 8, Minor: 0, Patch: 22, Table: tbl},
			[]string{"WARNING"}, []string{"INVISIBLE,"}},
		{"mysql5.7", SuggestInput{Flavor: "mysql", Major: 5, Minor: 7, Patch: 44, Table: tbl},
			[]string{"WARNING", "extra column"}, nil},
		{"pg-surrogate", SuggestInput{Flavor: "postgres", Table: tbl},
			[]string{`ALTER TABLE "app"."t" ADD COLUMN _strand_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;`}, []string{"USING INDEX \"uk"}},
		{"pg-ri", SuggestInput{Flavor: "postgres", Table: tbl, PGIndex: "uk_email"},
			[]string{`ALTER TABLE "app"."t" REPLICA IDENTITY USING INDEX "uk_email";`}, []string{"GENERATED ALWAYS"}},
		{"large-mysql", SuggestInput{Flavor: "mysql", Major: 8, Minor: 0, Patch: 36, Table: tbl, EstRows: 5_000_000},
			[]string{"pt-online-schema-change", "gh-ost", "maintenance window"}, nil},
		{"large-pg", SuggestInput{Flavor: "postgres", Table: tbl, EstRows: 5_000_000},
			[]string{"CREATE UNIQUE INDEX CONCURRENTLY"}, nil},
		{"small-mysql", SuggestInput{Flavor: "mysql", Major: 8, Minor: 0, Patch: 36, Table: tbl, EstRows: 10},
			nil, []string{"gh-ost"}},
	}
	for _, c := range cases {
		out := Suggest(c.in)
		for _, w := range c.want {
			if !strings.Contains(out, w) {
				t.Errorf("%s: missing %q in:\n%s", c.name, w, out)
			}
		}
		for _, w := range c.not {
			if strings.Contains(out, w) {
				t.Errorf("%s: unexpected %q in:\n%s", c.name, w, out)
			}
		}
	}
}

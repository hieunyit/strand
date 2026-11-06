package seed_test

import (
	"testing"

	"strand/internal/model"
	"strand/internal/seed"
)

// review N06: một cách hiểu "tables" của seed.json PG cho planner / apply / sync.
func TestN06_SeedCovers(t *testing.T) {
	id := func(s, n string) model.TableID { return model.TableID{Schema: s, Table: n} }
	cases := []struct {
		name   string
		tables []string
		id     model.TableID
		want   bool
	}{
		{"empty-means-all", nil, id("app", "orders"), true},
		{"star-means-all-schemas", []string{"*"}, id("app", "orders"), true},
		{"star-with-spaces", []string{" * "}, id("Weird", "T"), true},
		{"exact", []string{"public.orders"}, id("public", "orders"), true},
		{"exact-other-table", []string{"public.orders"}, id("public", "users"), false},
		{"bare-is-public", []string{"orders"}, id("public", "orders"), true},
		{"bare-not-other-schema", []string{"orders"}, id("app", "orders"), false},
		{"schema-glob", []string{"app.*"}, id("app", "orders"), true},
		{"schema-glob-other-schema", []string{"app.*"}, id("public", "orders"), false},
		{"public-star-not-other-schema", []string{"public.*"}, id("app", "orders"), false},
		{"qmark", []string{"public.t?"}, id("public", "t1"), true},
		{"qmark-one-char-only", []string{"public.t?"}, id("public", "t12"), false},
		{"unquoted-folds-lower", []string{"public.Orders"}, id("public", "orders"), true},
		{"unquoted-not-mixed-case-table", []string{"public.Orders"}, id("public", "Orders"), false},
		{"quoted-keeps-case", []string{`public."Orders"`}, id("public", "Orders"), true},
		{"quoted-not-lower", []string{`public."Orders"`}, id("public", "orders"), false},
		{"quoted-schema", []string{`"App"."Orders"`}, id("App", "Orders"), true},
		{"quoted-dot-is-literal", []string{`"a.b"`}, id("public", "a.b"), true},
		{"quoted-star-is-literal", []string{`public."t*"`}, id("public", "tx"), false},
		{"doubled-quote", []string{`public."a""b"`}, id("public", `a"b`), true},
		{"invalid-matches-nothing", []string{`public."orders`}, id("public", "orders"), false},
		{"empty-schema-id-is-public", []string{"public.orders"}, id("", "orders"), true},
		{"any-of-list", []string{"public.users", "app.*"}, id("app", "x"), true},
	}
	for _, c := range cases {
		if got := seed.SeedCovers(c.tables, c.id); got != c.want {
			t.Errorf("%s: SeedCovers(%q, %v) = %v, want %v", c.name, c.tables, c.id, got, c.want)
		}
	}
	for _, bad := range []string{"", `"x`, "a.b.c", "public.", ".t"} {
		if err := seed.ValidateTablePatterns([]string{bad}); err == nil {
			t.Errorf("ValidateTablePatterns(%q) = nil, want error", bad)
		}
	}
}

// review N06: seed vật lý không có --tables ghi "*" = cả cluster → mọi bảng của pipeline đã có trong seed.
func TestN06_PGPhysicalDefaultTablesCoverAll(t *testing.T) {
	for kind, plan := range pgPlanners(t) {
		t.Run(kind, func(t *testing.T) {
			m, err := plan(func(s *seed.SeedSource) { s.Tables = nil }, "16", "00000001000000000000000A")
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []model.TableID{{Schema: "public", Table: "orders"}, {Schema: "App", Table: "Users"}} {
				if !seed.SeedCovers(m.Tables, id) {
					t.Fatalf("tables %q do not cover %v after a full physical restore", m.Tables, id)
				}
			}
			// pattern sai cú pháp bị từ chối ở plan.
			if _, err := plan(func(s *seed.SeedSource) { s.Tables = []string{`public."orders`} }, "16", "00000001000000000000000A"); err == nil {
				t.Fatal("invalid --tables pattern accepted")
			}
		})
	}
}

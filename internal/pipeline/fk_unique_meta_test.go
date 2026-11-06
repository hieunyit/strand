package pipeline

import (
	"context"
	"testing"

	"strand/internal/model"
)

// invariant: CO-1 — review R11. Metadata thật của MySQL và PG: FK tham chiếu UNIQUE(code) của cha
// được gắn ParentIndex; FK nhiều cột khai báo khác thứ tự PK cha được sắp theo thứ tự PK cha.
func TestCO1_ForeignKeyMetadataResolvesReferencedKey(t *testing.T) {
	for _, engine := range []string{"mysql", "pg"} {
		t.Run(engine, func(t *testing.T) {
			db, _ := startTestDB(t, engine)
			ctx := context.Background()
			schema := "app"
			if engine == "pg" {
				schema = "public"
			}
			p2 := `CREATE TABLE p2 (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))`
			if engine == "mysql" {
				// MySQL 8.4 chỉ cho FK tham chiếu một index có các cột theo đúng thứ tự.
				p2 = `CREATE TABLE p2 (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b), UNIQUE KEY uk_ba (b, a))`
			}
			for _, q := range []string{
				`CREATE TABLE parent (id BIGINT PRIMARY KEY, code VARCHAR(20) NOT NULL, CONSTRAINT uk_code UNIQUE (code))`,
				`CREATE TABLE child (id BIGINT PRIMARY KEY, parent_code VARCHAR(20), CONSTRAINT fk_code FOREIGN KEY (parent_code) REFERENCES parent(code))`,
				p2,
				`CREATE TABLE c2 (id BIGINT PRIMARY KEY, cb BIGINT, ca BIGINT, CONSTRAINT fk_ab FOREIGN KEY (cb, ca) REFERENCES p2(b, a))`,
			} {
				if _, err := db.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			f, err := newTableFilter([]string{schema + ".*"}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			var metas map[model.TableID]*tableMeta
			if engine == "mysql" {
				metas, err = loadSourceTables(ctx, db, f)
			} else {
				metas, err = loadSourceTablesPG(ctx, db, f)
			}
			if err != nil {
				t.Fatal(err)
			}
			resolveForeignKeys(metas, nil)
			child := metas[model.TableID{Schema: schema, Table: "child"}]
			if child == nil || len(child.Keys.ForeignKeys) != 1 {
				t.Fatalf("child meta %+v", child)
			}
			if fk := child.Keys.ForeignKeys[0]; fk.ParentIndex != "uk_code" || fk.Columns[0] != "parent_code" {
				t.Fatalf("FK to UNIQUE not resolved: %+v", fk)
			}
			c2 := metas[model.TableID{Schema: schema, Table: "c2"}]
			if fk := c2.Keys.ForeignKeys[0]; fk.ParentIndex != "" || fk.Columns[0] != "ca" || fk.Columns[1] != "cb" {
				t.Fatalf("composite FK not reordered to parent PK order: %+v", fk)
			}
		})
	}
}

// review N01: unique index có cột prefix (SUB_PART) phải được đánh dấu Prefix để writeset dùng khóa hằng.
// Cần container MySQL.
func TestN01_MySQLPrefixUniqueIndexMarked(t *testing.T) {
	db, _ := startTestDB(t, "mysql")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE pfx (id BIGINT PRIMARY KEY, code VARCHAR(20) NOT NULL, x INT NOT NULL,
		UNIQUE KEY uk_pfx (x, code(3)), UNIQUE KEY uk_full (code))`); err != nil {
		t.Fatal(err)
	}
	f, err := newTableFilter([]string{"app.pfx"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	metas, err := loadSourceTables(ctx, db, f)
	if err != nil {
		t.Fatal(err)
	}
	m := metas[model.TableID{Schema: "app", Table: "pfx"}]
	if m == nil {
		t.Fatal("thiếu meta pfx")
	}
	got := map[string]bool{}
	for _, uk := range m.Keys.Unique {
		got[uk.Name] = uk.Prefix
	}
	if p, ok := got["uk_pfx"]; !ok || !p {
		t.Fatalf("uk_pfx phải Prefix=true: %+v", m.Keys.Unique)
	}
	if p, ok := got["uk_full"]; !ok || p {
		t.Fatalf("uk_full phải Prefix=false: %+v", m.Keys.Unique)
	}
}

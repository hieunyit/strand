package pipeline

import (
	"context"
	"testing"

	"strand/internal/model"
)

// P3-01b: cờ "thuộc replica identity" trong metadata phải khớp cách pgoutput đánh cờ key
// (logicalrep_write_attrs). Bản đầu tính theo PK cho mọi bảng nên bảng REPLICA IDENTITY FULL /
// USING INDEX luôn bị coi là "Relation khác" → barrier DDL giả mỗi session. Trên code cũ test này
// fail ở bảng full và idx.
func TestLoadSourceTablesPG_ReplicaIdentityFlags(t *testing.T) {
	db, _ := startTestDB(t, "pg")
	ctx := context.Background()
	for _, q := range []string{
		`CREATE SCHEMA s`,
		`CREATE TABLE s.dflt (id bigint PRIMARY KEY, a int, b text)`,
		`CREATE TABLE s.full (id bigint PRIMARY KEY, a int, b text)`,
		`ALTER TABLE s.full REPLICA IDENTITY FULL`,
		`CREATE TABLE s.idx (id bigint PRIMARY KEY, a int NOT NULL, b text)`,
		`CREATE UNIQUE INDEX idx_a ON s.idx (a)`,
		`ALTER TABLE s.idx REPLICA IDENTITY USING INDEX idx_a`,
		`CREATE TABLE s.none (id bigint PRIMARY KEY, a int, b text)`,
		`ALTER TABLE s.none REPLICA IDENTITY NOTHING`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	f, err := newTableFilter([]string{"s.*"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	metas, err := loadSourceTablesPG(ctx, db, f)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]uint8{
		"dflt": {"id": 1, "a": 0, "b": 0},
		"full": {"id": 1, "a": 1, "b": 1},
		"idx":  {"id": 0, "a": 1, "b": 0},
		"none": {"id": 0, "a": 0, "b": 0},
	}
	for tbl, cols := range want {
		m := metas[model.TableID{Schema: "s", Table: tbl}]
		if m == nil {
			t.Fatalf("table s.%s not loaded", tbl)
		}
		if len(m.PGColumns) != len(cols) {
			t.Fatalf("s.%s: %d PG columns, want %d", tbl, len(m.PGColumns), len(cols))
		}
		for _, c := range m.PGColumns {
			if c.Flags != cols[c.Name] {
				t.Errorf("s.%s.%s: flags %d, want %d (pgoutput key flag)", tbl, c.Name, c.Flags, cols[c.Name])
			}
		}
	}
}

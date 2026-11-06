package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"strand/internal/model"
)

// Review N07 (unit, không cần DB): truy vấn cột unique index chỉ được lấy cột khóa (indnkeyatts),
// không lấy cột INCLUDE.
func TestN07UniqueIndexQueryExcludesIncludeColumns(t *testing.T) {
	if !strings.Contains(pgUniqueIndexColumnsSQL, "k.ord <= i.indnkeyatts") {
		t.Fatalf("index column query does not restrict to key columns (indnkeyatts):\n%s", pgUniqueIndexColumnsSQL)
	}
}

// Review N07 (cần container PG): `PRIMARY KEY (id) INCLUDE (payload)` phải cho PK=(id), và unique index
// có INCLUDE chỉ gồm cột khóa.
func TestN07PGIncludeColumnsNotInKey(t *testing.T) {
	db, _ := startTestDB(t, "pg")
	ctx := context.Background()
	for _, q := range []string{
		`CREATE TABLE inc (id BIGINT NOT NULL, code TEXT NOT NULL, payload TEXT, note TEXT,
			PRIMARY KEY (id) INCLUDE (payload))`,
		`CREATE UNIQUE INDEX uk_inc_code ON inc (code) INCLUDE (note)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	f, err := newTableFilter([]string{"public.inc"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	metas, err := loadSourceTablesPG(ctx, db, f)
	if err != nil {
		t.Fatal(err)
	}
	m := metas[model.TableID{Schema: "public", Table: "inc"}]
	if m == nil {
		t.Fatalf("table not loaded: %+v", metas)
	}
	if !reflect.DeepEqual(m.PK, []string{"id"}) {
		t.Fatalf("PK = %v, want [id] (INCLUDE column leaked into key)", m.PK)
	}
	if !m.PKNumeric {
		t.Errorf("PKNumeric = false for single BIGINT PK")
	}
	if len(m.Keys.Unique) != 1 || !reflect.DeepEqual(m.Keys.Unique[0].Columns, []string{"code"}) {
		t.Fatalf("unique indexes = %+v, want uk_inc_code(code)", m.Keys.Unique)
	}
}

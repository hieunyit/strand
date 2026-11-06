package seed

import (
	"testing"

	"strand/internal/model"
)

// review N06: `strand export --tables '*'` chỉ dump bảng thấy qua search_path; seed.json không được
// ghi "*" (= mọi schema) cho trường hợp đó.
func TestN06_ExportManifestTablesStar(t *testing.T) {
	tables := exportManifestTables([]string{"*", "app.orders"})
	if SeedCovers(tables, model.TableID{Schema: "other", Table: "t"}) {
		t.Fatalf("export tables %q cover a table in a schema pg_dump -t '*' did not dump", tables)
	}
	if !SeedCovers(tables, model.TableID{Schema: "public", Table: "t"}) || !SeedCovers(tables, model.TableID{Schema: "app", Table: "orders"}) {
		t.Fatalf("export tables %q lost a dumped table", tables)
	}
}

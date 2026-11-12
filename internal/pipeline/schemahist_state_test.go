package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	mysqlreader "strand/internal/reader/mysql"
)

// State cũ (không có source_schemas) vẫn đọc được; lịch sử ghi rồi mở lại còn nguyên; Save sao chép map.
func TestLocalStateSchemaHistory(t *testing.T) {
	dir := t.TempDir()
	old := `{"pipeline_id":"p","snapshot_done":{"app.t":true}}`
	if err := os.WriteFile(filepath.Join(dir, "pipeline-state.json"), []byte(old), 0o640); err != nil {
		t.Fatal(err)
	}
	s, err := openLocalState(dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	if !s.IsDone("app.t") || len(s.LoadSchemaHistory()) != 0 {
		t.Fatal("state cũ phải đọc được, lịch sử rỗng")
	}
	m := map[string][]mysqlreader.SchemaVersion{"app.t": {{Info: &mysqlreader.TableInfo{
		Columns: []mysqlreader.ColumnInfo{{Name: "id", DataType: "int", Unsigned: true}}, PK: []string{"id"}}, From: "u:1-5", Until: "u:6"}}}
	if err := s.SaveSchemaHistory(m); err != nil {
		t.Fatal(err)
	}
	m["app.t"][0].Until = "đổi sau khi lưu"
	s2, err := openLocalState(dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	got := s2.LoadSchemaHistory()["app.t"]
	if len(got) != 1 || got[0].Until != "u:6" || !got[0].Info.Columns[0].Unsigned || !s2.IsDone("app.t") {
		t.Fatalf("%+v", got)
	}
}

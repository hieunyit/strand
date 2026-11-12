package verify_test

import (
	"context"
	"testing"

	"strand/internal/model"
	"strand/internal/mysqlver/mysql57test"
	"strand/internal/verify"
)

// Biểu thức hash canonical của cột float/double dùng CAST(x AS DOUBLE) (8.0.17+) làm cả câu lỗi cú pháp
// trên nguồn 5.7. HashChunk phải chạy được trên 5.7 kể cả với giá trị |x| >= 1e35.
func TestMySQL57_HashChunk_FloatColumns(t *testing.T) {
	_, db := mysql57test.Start(t, mysql57test.SourceArgs...)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE DATABASE app",
		"CREATE TABLE app.f (id BIGINT PRIMARY KEY, a FLOAT, b DOUBLE, c DECIMAL(10,2), d VARCHAR(10), e DATETIME(3), g TIMESTAMP NULL)",
		"INSERT INTO app.f VALUES (1, 1.5, 2.25, 10.50, 'x', '2026-11-12 10:00:00.123', '2026-11-12 10:00:00'), (2, 1e36, 1.5e300, 0, NULL, NULL, NULL), (3, -3.4e38, -1e100, -1.10, '', '2000-01-01', NULL), (4, 0, 0, 0, 'y', NULL, NULL)",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	cols := []verify.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "a", DataType: "float"}, {Name: "b", DataType: "double"},
		{Name: "c", DataType: "decimal"}, {Name: "d", DataType: "varchar"}, {Name: "e", DataType: "datetime"}, {Name: "g", DataType: "timestamp"}}
	h1, err := verify.HashChunk(ctx, db, model.EngineMySQL, model.TableID{Schema: "app", Table: "f"}, "id", verify.ChunkRange{ID: 1, Start: 1, End: 4}, cols)
	if err != nil {
		t.Fatalf("HashChunk on 5.7: %v", err)
	}
	if h1[:2] != "4:" {
		t.Errorf("hash %q: want 4 rows", h1)
	}
	var e1, e2 string
	if err := db.QueryRowContext(ctx, "SELECT CAST(a + 0e0 AS CHAR), CAST(b + 0e0 AS CHAR) FROM app.f WHERE id = 2").Scan(&e1, &e2); err != nil {
		t.Fatal(err)
	}
	// Giá trị này bằng đúng kết quả CAST(CAST(x AS DOUBLE) AS CHAR) của MySQL 8.4 (đã đối chiếu).
	if e1 != "9.999999616903162e35" || e2 != "1.5e300" {
		t.Errorf("double text: %s / %s", e1, e2)
	}
}

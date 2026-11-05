package snapshot_test

import (
	"context"
	"database/sql"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// Việc C: khóa VARBINARY rỗng ” làm biên dưới của bảng. []byte(nil) bind thành NULL → (b, x) >= (NULL, …)
// không khớp row nào: snapshot chép 0 row mà vẫn báo xong. Range [min, max] phải đếm đủ mọi row.
func TestGPK_EmptyBinaryKeyIsABoundNotNull(t *testing.T) {
	_, dstDSN := getMySQLDSNs(t)
	db, _ := sql.Open("mysql", dstDSN)
	defer db.Close()
	ctx := context.Background()
	db.Exec("DROP TABLE IF EXISTS dbg_bd")
	db.Exec("CREATE TABLE dbg_bd (b VARBINARY(6) NOT NULL, x DECIMAL(12,4) NOT NULL, v INT, PRIMARY KEY (b, x))")
	db.Exec("INSERT INTO dbg_bd VALUES ('', -3.5, 1), (X'00', 1.25, 2), (X'ff', 0, 3), ('', 7, 4)")
	var schema string
	db.QueryRow("SELECT DATABASE()").Scan(&schema)
	codec := mysqlCodec(t, db, "dbg_bd", []string{"b", "x"})
	cond, n, err := snapshot.KeyRangeDebugForTest(ctx, db, model.TableID{Schema: schema, Table: "dbg_bd"}, codec)
	if err != nil || n != 4 {
		t.Fatalf("rows in [min, max] = %d (err %v), want 4; cond %s", n, err, cond)
	}
}

package snapshot_test

// Gate 2 (cần container MySQL): WEIGHT_STRING theo lô (EncodeMany, một truy vấn nhiều giá trị) phải cho
// đúng khóa như Encode từng giá trị trên một codec mới, với collation NO PAD và PAD SPACE, kể cả chuỗi
// rỗng, space cuối, hoa/thường, có dấu.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestG2_MySQLEncodeManyMatchesEncode(t *testing.T) {
	_, dstDSN := getMySQLDSNs(t)
	db, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var schema string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	for _, coll := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "utf8mb4_unicode_ci"} {
		tbl := "g2_em_" + coll
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (k VARCHAR(16) CHARACTER SET utf8mb4 COLLATE %s NOT NULL PRIMARY KEY)", tbl, coll)); err != nil {
			t.Fatal(err)
		}
		id := model.TableID{Schema: schema, Table: tbl}
		var tuples [][]model.Value
		for _, v := range []string{"a", "A", "a ", "á", "Á ", "", " ", "ß", "ss", "_", "Z", "z", "é", "é"} {
			tuples = append(tuples, []model.Value{v})
		}
		for i := 0; i < 300; i++ { // > một lô (128)
			tuples = append(tuples, []model.Value{fmt.Sprintf("Kéy-%03d", i)})
		}
		batch, err := snapshot.MySQLPKCodec(ctx, db, id, []string{"k"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := batch.EncodeMany(tuples)
		if err != nil {
			t.Fatalf("%s: EncodeMany: %v", coll, err)
		}
		single, err := snapshot.MySQLPKCodec(ctx, db, id, []string{"k"})
		if err != nil {
			t.Fatal(err)
		}
		for i, tu := range tuples {
			want, err := single.Encode(tu)
			if err != nil {
				t.Fatal(err)
			}
			if got[i] != want {
				t.Errorf("%s: key of %q: EncodeMany %x, Encode %x", coll, tu[0], got[i], want)
			}
		}
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl)
	}
}

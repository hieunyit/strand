package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// Review F3: keyset walk / SELECT chunk của PK ghép dùng row constructor "(a, b) >= (?, ?)". Câu phải
// đọc bằng range trên index PK — số row InnoDB đọc cho một chunk ở cuối bảng phải cỡ kích thước chunk,
// không phải cỡ vị trí của chunk trong bảng (nếu không, snapshot bảng lớn là O(n²)).
func TestGPK_CompositeKeysetReadsOnlyTheChunk(t *testing.T) {
	_, dsn := getMySQLDSNs(t)
	db, err := sql.Open("mysql", dsn)
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
	for _, q := range []string{
		"DROP TABLE IF EXISTS gpk_range",
		"CREATE TABLE gpk_range (a INT NOT NULL, b VARCHAR(20) COLLATE utf8mb4_bin NOT NULL, v INT, PRIMARY KEY (a, b))",
		"INSERT INTO gpk_range (a, b, v) WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM s WHERE n < 59999) SELECT n DIV 3, CONCAT('k', n % 3), n FROM s",
		"ANALYZE TABLE gpk_range",
	} {
		if _, err := db.ExecContext(ctx, "SET SESSION cte_max_recursion_depth = 100000"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	tbl := model.TableID{Schema: schema, Table: "gpk_range"}
	codec, err := snapshot.MySQLPKCodec(ctx, db, tbl, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	q := snapshot.MySQLKeySQL{Table: tbl, Codec: codec}
	lo, err := codec.BoundOf([]model.Value{int64(19000), "k0"})
	if err != nil {
		t.Fatal(err)
	}
	hi, err := codec.BoundOf([]model.Value{int64(19999), "k2"})
	if err != nil {
		t.Fatal(err)
	}
	// Lấy khóa thứ 1000 tính từ lo — như mysqlKeysetEnd.
	cond, args, err := q.RangeCond(lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reads := func() int64 {
		var name string
		var v, sum int64
		rows, err := conn.QueryContext(ctx, "SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_first','Handler_read_key','Handler_read_next','Handler_read_rnd_next')")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&name, &v); err != nil {
				t.Fatal(err)
			}
			sum += v
		}
		return sum
	}
	before := reads()
	var a int64
	var b string
	query := fmt.Sprintf("SELECT a, b FROM `%s`.`gpk_range` WHERE %s ORDER BY a, b LIMIT 1 OFFSET 999", schema, cond)
	if err := conn.QueryRowContext(ctx, query, args...).Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	read := reads() - before
	var plan string
	_ = conn.QueryRowContext(ctx, "EXPLAIN FORMAT=TREE "+query, args...).Scan(&plan)
	t.Logf("keyset end (%d,%s); handler reads %d; plan:\n%s", a, b, read, plan)
	if a != 19333 || b != "k0" {
		t.Fatalf("keyset end = (%d,%s), want (19333,k0)", a, b)
	}
	if read > 5000 {
		t.Fatalf("chunk at the end of the table read %d rows (chunk is 1000): row-constructor range is not used", read)
	}
}

package mysql_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"strand/internal/reader"
	readermysql "strand/internal/reader/mysql"
)

// Review F1: BINARY(n) được MySQL đệm 0x00 tới n byte và SELECT trả đủ n byte; binlog ROW cắt các byte
// đệm cuối. Giá trị reader đưa ra phải trùng giá trị SELECT (cùng khóa ở range-state, cùng byte khi ghi
// sang bytea của PG).
func TestBinaryColumnStreamMatchesSelect(t *testing.T) {
	db, host, port, cleanup := startMySQLServer(t, 2101)
	defer cleanup()
	for _, q := range []string{
		"CREATE DATABASE bp",
		"CREATE TABLE bp.t (k BINARY(4) PRIMARY KEY, b BINARY(6), v VARBINARY(6))",
		"INSERT INTO bp.t VALUES (X'61000000', X'616200000000', X'6100'), (X'00000000', X'00', X'')",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	want := map[string][]any{}
	rows, err := db.Query("SELECT k, b, v FROM bp.t")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k, b, v []byte
		if err := rows.Scan(&k, &b, &v); err != nil {
			t.Fatal(err)
		}
		want[fmt.Sprintf("%x", k)] = []any{k, b, v}
	}
	rows.Close()

	cs, err := readermysql.LoadCollationCharsets(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	r, err := readermysql.New(readermysql.Config{Host: host, Port: uint16(port), User: "root", Password: "dev", ServerID: 3101, CollationCharsets: cs})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events, err := r.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for got < 2 {
		select {
		case ev := <-events:
			if ev.Type == reader.EventError {
				t.Fatal(ev.Err)
			}
			if ev.Type != reader.EventRows || ev.Table != "t" {
				continue
			}
			for _, row := range ev.Rows {
				bs := make([][]byte, 3)
				for i := range bs {
					switch x := row[i].(type) {
					case []byte:
						bs[i] = x
					case string:
						bs[i] = []byte(x)
					default:
						t.Fatalf("column %d: unexpected %T", i, row[i])
					}
				}
				// Khóa trong map theo giá trị SELECT; binlog có thể thiếu đệm nên tìm theo cả hai.
				w, ok := want[fmt.Sprintf("%x", bs[0])]
				if !ok {
					for kk, ww := range want {
						if bytes.HasPrefix(ww[0].([]byte), bs[0]) {
							w, ok = ww, true
							t.Logf("row %s: stream key %x is shorter than SELECT key", kk, bs[0])
						}
					}
				}
				if !ok {
					t.Fatalf("stream row with key %x not found", bs[0])
				}
				for i, name := range []string{"k BINARY(4)", "b BINARY(6)", "v VARBINARY(6)"} {
					if !bytes.Equal(bs[i], w[i].([]byte)) {
						t.Errorf("%s: stream %x, SELECT %x", name, bs[i], w[i])
					}
				}
				got++
			}
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
}

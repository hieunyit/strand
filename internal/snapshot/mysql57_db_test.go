package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/mysqlver/mysql57test"
	"strand/internal/snapshot"
)

// Nguồn MySQL 5.7: information_schema.COLLATIONS không có PAD_ATTRIBUTE (8.0+) nên truy vấn cũ của
// MySQLPKCodec lỗi "Unknown column". Codec dựng trên 5.7 phải cho thứ tự khóa == ORDER BY của chính 5.7
// với PK varchar/char nhiều collation (utf8mb4_general_ci mặc định của 5.7, utf8 = utf8mb3, bin, latin1).
func TestMySQL57_PKCodecKeyOrderMatchesOrderBy(t *testing.T) {
	_, db := mysql57test.Start(t, mysql57test.SourceArgs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS app57"); err != nil {
		t.Fatal(err)
	}
	var ver string
	if err := db.QueryRow("SELECT VERSION()").Scan(&ver); err != nil || !strings.HasPrefix(ver, "5.7.") {
		t.Fatalf("expected a MySQL 5.7 server, got %q (%v)", ver, err)
	}

	cases := []struct{ name, cols string }{
		{"gen_default", "k VARCHAR(16)"}, // collation mặc định của 5.7: utf8mb4_general_ci
		{"utf8_gen", "k VARCHAR(16) CHARACTER SET utf8 COLLATE utf8_general_ci"},
		{"utf8mb4_bin", "k VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
		{"latin1_char", "k CHAR(8) CHARACTER SET latin1 COLLATE latin1_swedish_ci"},
	}
	alphabet := []string{"a", "A", "b", "B", "e", "é", "E", " ", "z", "0"}
	r := rand.New(rand.NewSource(57))
	for _, c := range cases {
		tbl := "app57.pk_" + c.name
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (k)) DEFAULT CHARSET=utf8mb4", tbl, c.cols)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 300; i++ {
			var b strings.Builder
			for n := r.Intn(5); n >= 0; n-- {
				b.WriteString(alphabet[r.Intn(len(alphabet))])
			}
			if c.name == "latin1_char" {
				// ký tự ngoài latin1 không chèn được
				s := strings.NewReplacer("é", "e").Replace(b.String())
				b.Reset()
				b.WriteString(s)
			}
			if _, err := db.ExecContext(ctx, "INSERT IGNORE INTO "+tbl+" VALUES (?)", b.String()); err != nil {
				t.Fatalf("%s insert: %v", c.name, err)
			}
		}
		codec, err := snapshot.MySQLPKCodec(ctx, db, model.TableID{Schema: "app57", Table: "pk_" + c.name}, []string{"k"})
		if err != nil {
			t.Fatalf("%s: MySQLPKCodec on 5.7: %v", c.name, err)
		}
		// 5.7: mọi collation là PAD SPACE: _bin dùng PKPadBytes (không phải PKBytes của NO PAD), còn lại PKWeight.
		wantKind, wantPad := snapshot.PKWeight, true
		if strings.HasSuffix(c.name, "_bin") {
			wantKind, wantPad = snapshot.PKPadBytes, false
		}
		if col := codec.Cols[0]; col.Kind != wantKind || (wantKind == snapshot.PKWeight && col.PadSpace != wantPad) {
			t.Errorf("%s: kind %v padSpace %v, want %v", c.name, col.Kind, col.PadSpace, wantKind)
		}
		checkOrder(t, ctx, db, codec, "app57.pk_"+c.name, c.name)
	}
}

func checkOrder(t *testing.T, ctx context.Context, db *sql.DB, codec *snapshot.PKCodec, tbl, name string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT k FROM "+tbl+" ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var prev snapshot.Key
	n := 0
	for rows.Next() {
		var raw sql.RawBytes
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		k, err := codec.Encode([]model.Value{append([]byte(nil), raw...)})
		if err != nil {
			t.Fatalf("%s: encode %q: %v", name, raw, err)
		}
		if n > 0 && !(prev < k) {
			t.Fatalf("%s: row %d %q encodes <= previous row in ORDER BY order", name, n, raw)
		}
		prev = k
		n++
	}
	if n < 15 {
		t.Fatalf("%s: only %d keys", name, n)
	}
}

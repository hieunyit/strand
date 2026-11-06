package snapshot_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// review N04: với collation PAD SPACE ('a' = 'a ' theo DB), mã khóa trong Go (WEIGHT_STRING) phải bằng
// nhau đúng khi DB coi hai giá trị bằng nhau, và thứ tự mã phải trùng thứ tự DB — kể cả khi một giá trị
// là tiền tố của giá trị kia, khoảng trắng cuối, tab (< space) và ký tự lớn hơn space. Test cũ chỉ xét
// các khóa còn lại sau INSERT IGNORE nên không có cặp "bằng nhau nhưng khác biểu diễn".
func TestN04_MySQLPadSpaceKeyEqualityAndOrderMatchDB(t *testing.T) {
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

	cases := []struct{ charset, collation string }{
		{"utf8mb4", "utf8mb4_general_ci"}, // PAD SPACE
		{"utf8mb4", "utf8mb4_unicode_ci"}, // PAD SPACE
		{"latin1", "latin1_swedish_ci"},   // PAD SPACE
		{"utf8mb4", "utf8mb4_0900_ai_ci"}, // NO PAD (đối chứng)
		{"utf8mb4", "utf8mb4_0900_as_cs"}, // NO PAD
	}
	alphabet := []rune("aA b\t~")
	if testing.Short() {
		cases = cases[:2]
	}
	r := rand.New(rand.NewSource(4))
	gen := func() string {
		n := r.Intn(5)
		b := make([]rune, 0, n+2)
		for i := 0; i < n; i++ {
			b = append(b, alphabet[r.Intn(len(alphabet))])
		}
		if r.Intn(2) == 0 {
			for j := r.Intn(3); j >= 0; j-- {
				b = append(b, ' ')
			}
		}
		return string(b)
	}
	for _, c := range cases {
		tbl := "n04_" + c.collation
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (k VARCHAR(16) CHARACTER SET %s COLLATE %s NOT NULL, PRIMARY KEY (k))",
			tbl, c.charset, c.collation)); err != nil {
			t.Fatal(err)
		}
		codec, err := snapshot.MySQLPKCodec(ctx, db, model.TableID{Schema: schema, Table: tbl}, []string{"k"})
		if err != nil {
			t.Fatalf("%s: codec: %v", c.collation, err)
		}
		vals := []string{"a", "a ", "a  ", "a\t", "a~", "A", "A ", "", " ", "ab", "a b", "a  b"}
		for i := 0; i < 60; i++ {
			vals = append(vals, gen())
		}
		enc := map[string]snapshot.Key{}
		for _, v := range vals {
			k, err := codec.Encode([]model.Value{v})
			if err != nil {
				t.Fatalf("%s: encode %q: %v", c.collation, v, err)
			}
			enc[v] = k
		}
		mismatch := 0
		for _, a := range vals {
			for _, b := range vals {
				var cmp int
				q := fmt.Sprintf("SELECT STRCMP(CONVERT(? USING %[1]s) COLLATE %[2]s, CONVERT(? USING %[1]s) COLLATE %[2]s)", c.charset, c.collation)
				if err := db.QueryRowContext(ctx, q, a, b).Scan(&cmp); err != nil {
					t.Fatal(err)
				}
				got := bytes.Compare([]byte(enc[a]), []byte(enc[b]))
				if got != cmp {
					mismatch++
					if mismatch <= 10 {
						t.Errorf("%s: DB compares %q vs %q = %d, Go key order = %d", c.collation, a, b, cmp, got)
					}
				}
			}
		}
		if mismatch > 0 {
			t.Errorf("%s: %d pairs disagree", c.collation, mismatch)
		}
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl)
	}
}

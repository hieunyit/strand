package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/reader"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/snapshot"
)

// pkCase là một bảng thử: định nghĩa cột PK và bộ sinh giá trị (literal SQL).
type pkCase struct {
	name string
	cols string   // định nghĩa cột PK, ví dụ "k BIGINT"
	pk   []string // tên cột PK theo thứ tự
	gen  func(r *rand.Rand) []string
}

func mysqlString(r *rand.Rand, alphabet []rune, maxLen int) string {
	n := r.Intn(maxLen + 1)
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteRune(alphabet[r.Intn(len(alphabet))])
	}
	return b.String()
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

func mysqlPKCases() []pkCase {
	uni := []rune("aAbBzZ09 -_.~éÉèñßÆøĐđ中文😀 ")
	ascii := []rune("abcXYZ019 -_~!")
	return []pkCase{
		{"bigint", "k BIGINT", []string{"k"}, func(r *rand.Rand) []string {
			vs := []int64{math.MinInt64, math.MaxInt64, 0, -1, 1}
			return []string{fmt.Sprint(append(vs, r.Int63()-r.Int63())[r.Intn(len(vs)+1)])}
		}},
		{"bigint_unsigned", "k BIGINT UNSIGNED", []string{"k"}, func(r *rand.Rand) []string {
			vs := []uint64{0, 1, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64}
			return []string{fmt.Sprint(append(vs, r.Uint64())[r.Intn(len(vs)+1)])}
		}},
		{"decimal", "k DECIMAL(30,10)", []string{"k"}, func(r *rand.Rand) []string {
			sign := []string{"", "-"}[r.Intn(2)]
			return []string{fmt.Sprintf("%s%d.%010d", sign, r.Int63n(1_000_000_000_000)*int64(r.Intn(3)), r.Int63n(10_000_000_000))}
		}},
		{"datetime6", "k DATETIME(6)", []string{"k"}, func(r *rand.Rand) []string {
			t := time.Unix(r.Int63n(253402300799), r.Int63n(1e6)*1000).UTC()
			return []string{sqlQuote(t.Format("2006-01-02 15:04:05.000000"))}
		}},
		{"timestamp3", "k TIMESTAMP(3)", []string{"k"}, func(r *rand.Rand) []string {
			t := time.Unix(1+r.Int63n(2147483646), r.Int63n(1000)*1e6).UTC()
			return []string{sqlQuote(t.Format("2006-01-02 15:04:05.000"))}
		}},
		{"date", "k DATE", []string{"k"}, func(r *rand.Rand) []string {
			return []string{sqlQuote(time.Unix(r.Int63n(253402300799), 0).UTC().Format("2006-01-02"))}
		}},
		{"time6", "k TIME(6)", []string{"k"}, func(r *rand.Rand) []string {
			us := r.Int63n(2*838*3600*1_000_000) - 838*3600*1_000_000
			neg := ""
			if us < 0 {
				neg, us = "-", -us
			}
			return []string{sqlQuote(fmt.Sprintf("%s%d:%02d:%02d.%06d", neg, us/3_600_000_000, us/60_000_000%60, us/1_000_000%60, us%1_000_000))}
		}},
		{"year", "k YEAR", []string{"k"}, func(r *rand.Rand) []string { return []string{fmt.Sprint(1901 + r.Intn(255))} }},
		{"varbinary", "k VARBINARY(12)", []string{"k"}, func(r *rand.Rand) []string {
			b := make([]byte, r.Intn(13))
			for i := range b {
				b[i] = []byte{0, 1, 0x20, 0x7f, 0x80, 0xff, 'a'}[r.Intn(7)]
			}
			return []string{fmt.Sprintf("X'%x'", b)}
		}},
		{"varchar_0900_bin", "k VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin", []string{"k"}, func(r *rand.Rand) []string {
			return []string{sqlQuote(mysqlString(r, uni, 8))}
		}},
		{"varchar_bin_pad_space", "k VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", []string{"k"}, func(r *rand.Rand) []string {
			s := mysqlString(r, uni, 6)
			if r.Intn(3) == 0 {
				s += "  "
			}
			return []string{sqlQuote(s)}
		}},
		{"char_latin1_bin", "k CHAR(6) CHARACTER SET latin1 COLLATE latin1_bin", []string{"k"}, func(r *rand.Rand) []string {
			return []string{sqlQuote(mysqlString(r, ascii, 6))}
		}},
		{"composite_int_varbinary_datetime", "a INT, b VARBINARY(4), c DATETIME", []string{"a", "b", "c"}, func(r *rand.Rand) []string {
			b := make([]byte, r.Intn(3))
			for i := range b {
				b[i] = byte(r.Intn(3))
			}
			return []string{fmt.Sprint(r.Intn(5) - 2), fmt.Sprintf("X'%x'", b),
				sqlQuote(time.Unix(r.Int63n(5)*86400, 0).UTC().Format("2006-01-02 15:04:05"))}
		}},
	}
}

// mysqlCodec dựng codec từ information_schema (cùng đường như pipeline sẽ dùng).
func mysqlCodec(t *testing.T, db *sql.DB, table string, pk []string) *snapshot.PKCodec {
	t.Helper()
	c := &snapshot.PKCodec{}
	for _, col := range pk {
		var dt, ct string
		var cs, coll, pad sql.NullString
		err := db.QueryRow(`SELECT c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, co.PAD_ATTRIBUTE
			FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = c.COLLATION_NAME
			WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`, table, col).Scan(&dt, &ct, &cs, &coll, &pad)
		if err != nil {
			t.Fatal(err)
		}
		pc, err := snapshot.MySQLPKColumn(col, dt, ct, cs.String, coll.String, pad.String)
		if err != nil {
			t.Fatal(err)
		}
		c.Cols = append(c.Cols, pc)
	}
	return c
}

// invariant: RS-1/RS-2 (việc C mức 1). Thứ tự mã hóa khóa trong Go == ORDER BY của MySQL 8.4 trên mọi
// kiểu PK được nhận, và mã của khóa đọc qua binlog == mã của cùng khóa đọc bằng SELECT.
func TestGPK_MySQLKeyEncodingMatchesDBOrderAndBinlog(t *testing.T) {
	_, dstDSN := getMySQLDSNs(t)
	db, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var gtid string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		t.Fatal(err)
	}
	applied, err := checkpoint.ParseMySQLGTIDSet(strings.ReplaceAll(gtid, "\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	_ = db.QueryRow("SELECT DATABASE()").Scan(&schema)

	cases := mysqlPKCases()
	r := rand.New(rand.NewSource(42))
	for _, c := range cases {
		tbl := "gpk_" + c.name
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))", tbl, c.cols, strings.Join(c.pk, ","))); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 400; i++ {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT IGNORE INTO %s VALUES (%s)", tbl, strings.Join(c.gen(r), ","))); err != nil {
				t.Fatalf("%s insert: %v", c.name, err)
			}
		}
	}

	// Mã từ SELECT … ORDER BY pk (text protocol, như snapshot đọc).
	selectKeys := map[string]map[snapshot.Key]bool{}
	for _, c := range cases {
		tbl := "gpk_" + c.name
		codec := mysqlCodec(t, db, tbl, c.pk)
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", strings.Join(c.pk, ","), tbl, strings.Join(c.pk, ",")))
		if err != nil {
			t.Fatal(err)
		}
		var prev snapshot.Key
		n := 0
		selectKeys[tbl] = map[snapshot.Key]bool{}
		for rows.Next() {
			raw := make([]sql.RawBytes, len(c.pk))
			ptrs := make([]any, len(raw))
			for i := range raw {
				ptrs[i] = &raw[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			vals := make([]model.Value, len(raw))
			for i, b := range raw {
				vals[i] = append([]byte(nil), b...)
			}
			k, err := codec.Encode(vals)
			if err != nil {
				t.Fatalf("%s: encode %q: %v", c.name, vals, err)
			}
			if n > 0 && !(prev < k) {
				t.Fatalf("%s: row %d %q encodes <= previous row in ORDER BY order", c.name, n, vals)
			}
			prev = k
			selectKeys[tbl][k] = true
			n++
		}
		rows.Close()
		if n < 20 {
			t.Fatalf("%s: only %d distinct keys", c.name, n)
		}
	}

	// Mã từ binlog: mọi INSERT ở trên, đọc bằng reader thật.
	rd, err := mysqlreader.New(mysqlreader.Config{DSN: dstDSN, ServerID: 9911})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	ch, err := rd.Start(ctx, applied)
	if err != nil {
		t.Fatal(err)
	}
	codecs := map[string]*snapshot.PKCodec{}
	pkIdx := map[string][]int{}
	for _, c := range cases {
		tbl := "gpk_" + c.name
		codecs[tbl] = mysqlCodec(t, db, tbl, c.pk)
		for _, col := range c.pk {
			var pos int
			_ = db.QueryRow("SELECT ORDINAL_POSITION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?", tbl, col).Scan(&pos)
			pkIdx[tbl] = append(pkIdx[tbl], pos-1)
		}
	}
	want := 0
	for _, m := range selectKeys {
		want += len(m)
	}
	seen := map[string]map[snapshot.Key]bool{}
	got := 0
	deadline := time.After(2 * time.Minute)
	for got < want {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("binlog stream closed")
			}
			if ev.Type == reader.EventError {
				t.Fatal(ev.Err)
			}
			if ev.Type != reader.EventRows || ev.Schema != schema || codecs[ev.Table] == nil {
				continue
			}
			for _, row := range ev.Rows {
				vals := make([]model.Value, 0, len(pkIdx[ev.Table]))
				for _, i := range pkIdx[ev.Table] {
					vals = append(vals, row[i])
				}
				k, err := codecs[ev.Table].Encode(vals)
				if err != nil {
					t.Fatalf("%s: encode binlog key %#v: %v", ev.Table, vals, err)
				}
				if !selectKeys[ev.Table][k] {
					t.Fatalf("%s: binlog key %#v encodes differently from every SELECT key", ev.Table, vals)
				}
				if seen[ev.Table] == nil {
					seen[ev.Table] = map[snapshot.Key]bool{}
				}
				if !seen[ev.Table][k] {
					seen[ev.Table][k] = true
					got++
				}
			}
		case <-deadline:
			t.Fatalf("binlog: matched %d of %d keys", got, want)
		}
	}
	t.Logf("%d tables, %d keys: SELECT order and binlog encodings consistent", len(cases), want)
}

package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/model"
	pgreader "strand/internal/reader/pg"
	"strand/internal/snapshot"
)

func startPGForKeys(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "postgres:18", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "app"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(180 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, nat.Port("5432/tcp"))
	cc, err := pgx.ParseConfig(fmt.Sprintf("postgres://strand:dev@%s:%s/app?sslmode=disable", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range pgreader.SessionParams { // cùng GUC phiên với snapshot/replication
		cc.RuntimeParams[k] = v
	}
	db := stdlib.OpenDB(*cc)
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 60; i++ {
		if err = db.Ping(); err == nil {
			return db
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

// invariant: RS-1/RS-2 (việc C mức 1). Thứ tự mã hóa khóa == ORDER BY của PostgreSQL trên mọi kiểu PK
// được nhận, với giá trị đọc như snapshot PG (`col::text` → ParseTextValue).
func TestGPK_PGKeyEncodingMatchesDBOrder(t *testing.T) {
	db := startPGForKeys(t)
	ctx := context.Background()
	r := rand.New(rand.NewSource(7))
	uni := []rune("aAbZ09 -_~éßÆ中😀 ")
	str := func(n int) string {
		var b strings.Builder
		for i := r.Intn(n + 1); i > 0; i-- {
			b.WriteRune(uni[r.Intn(len(uni))])
		}
		return b.String()
	}
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	cases := []struct {
		name, cols string
		pk         []string
		gen        func() []string
	}{
		{"int8", "k bigint", []string{"k"}, func() []string { return []string{fmt.Sprint(r.Int63() - r.Int63())} }},
		{"numeric", "k numeric", []string{"k"}, func() []string {
			return []string{fmt.Sprintf("%s%d.%0*d", []string{"", "-"}[r.Intn(2)], r.Int63n(1e9)*int64(r.Intn(2)), 1+r.Intn(12), r.Int63n(1e12))}
		}},
		{"timestamptz", "k timestamptz", []string{"k"}, func() []string {
			return []string{q(time.Unix(r.Int63n(253402300799)-62135596800/2, r.Int63n(1e6)*1000).UTC().Format("2006-01-02 15:04:05.000000Z07:00"))}
		}},
		{"timestamp", "k timestamp(3)", []string{"k"}, func() []string {
			return []string{q(time.Unix(r.Int63n(4e9), r.Int63n(1000)*1e6).UTC().Format("2006-01-02 15:04:05.000"))}
		}},
		{"date", "k date", []string{"k"}, func() []string { return []string{q(time.Unix(r.Int63n(253402300799), 0).UTC().Format("2006-01-02"))} }},
		{"text_c", `k text COLLATE "C"`, []string{"k"}, func() []string { return []string{q(str(8))} }},
		{"bpchar_c", `k char(6) COLLATE "C"`, []string{"k"}, func() []string { return []string{q(str(5))} }},
		{"uuid", "k uuid", []string{"k"}, func() []string {
			b := make([]byte, 16)
			r.Read(b)
			if r.Intn(4) == 0 {
				b[0] = 0
			}
			return []string{fmt.Sprintf("'%x'", b)}
		}},
		{"bytea", "k bytea", []string{"k"}, func() []string {
			b := make([]byte, r.Intn(6))
			for i := range b {
				b[i] = []byte{0, 1, 0x7f, 0x80, 0xff}[r.Intn(5)]
			}
			return []string{fmt.Sprintf(`'\x%x'`, b)}
		}},
		{"composite", `a int4, b text COLLATE "C", c numeric`, []string{"a", "b", "c"}, func() []string {
			return []string{fmt.Sprint(r.Intn(4) - 2), q(str(2)), fmt.Sprintf("%d.%d", r.Intn(3), r.Intn(20))}
		}},
	}
	total := 0
	for _, c := range cases {
		tbl := "gpk_" + c.name
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))", tbl, c.cols, strings.Join(c.pk, ","))); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 400; i++ {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (%s) ON CONFLICT DO NOTHING", tbl, strings.Join(c.gen(), ","))); err != nil {
				t.Fatalf("%s insert: %v", c.name, err)
			}
		}
		codec := &snapshot.PKCodec{}
		oids := make([]uint32, len(c.pk))
		for i, col := range c.pk {
			var typ, coll string
			var oid int64
			if err := db.QueryRowContext(ctx, `SELECT ty.typname, a.atttypid::bigint, COALESCE(co.collname, '')
				FROM pg_attribute a JOIN pg_type ty ON ty.oid = a.atttypid LEFT JOIN pg_collation co ON co.oid = a.attcollation
				WHERE a.attrelid = $1::regclass AND a.attname = $2`, tbl, col).Scan(&typ, &oid, &coll); err != nil {
				t.Fatal(err)
			}
			pc, err := snapshot.PGPKColumn(col, typ, coll)
			if err != nil {
				t.Fatal(err)
			}
			codec.Cols, oids[i] = append(codec.Cols, pc), uint32(oid)
		}
		sel := make([]string, len(c.pk))
		for i, col := range c.pk {
			sel[i] = "t." + col + "::text"
		}
		ord := make([]string, len(c.pk))
		for i, col := range c.pk {
			ord[i] = "t." + col // cột của BẢNG, không phải cột kết quả col::text
		}
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s AS t ORDER BY %s", strings.Join(sel, ","), tbl, strings.Join(ord, ",")))
		if err != nil {
			t.Fatal(err)
		}
		var prev snapshot.Key
		n := 0
		for rows.Next() {
			txt := make([]string, len(c.pk))
			ptrs := make([]any, len(txt))
			for i := range txt {
				ptrs[i] = &txt[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			vals := make([]model.Value, len(txt))
			for i, s := range txt {
				if vals[i], err = pgreader.ParseTextValue(oids[i], s); err != nil {
					t.Fatal(err)
				}
			}
			k, err := codec.Encode(vals)
			if err != nil {
				t.Fatalf("%s: encode %q: %v", c.name, txt, err)
			}
			if n > 0 && !(prev < k) {
				t.Fatalf("%s: row %d %q (%#v → %x) encodes <= previous row %x in ORDER BY order", c.name, n, txt, vals, []byte(k), []byte(prev))
			}
			prev = k
			n++
		}
		rows.Close()
		if n < 20 {
			t.Fatalf("%s: only %d distinct keys", c.name, n)
		}
		total += n
	}
	// Collation không phải "C" bị từ chối rõ ràng (mức 3).
	if _, err := snapshot.PGPKColumn("k", "text", "en_US.utf8"); err == nil {
		t.Fatal("non-C text collation must be rejected")
	}
	t.Logf("%d tables, %d keys in ORDER BY order", len(cases), total)
}

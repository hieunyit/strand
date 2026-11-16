package rowkey

// Kiểm thử trên server thật (container): câu SQL catalog của LoadMySQL / LoadPG, cột INVISIBLE, index prefix /
// biểu thức / nullable, và chính câu ALTER gợi ý (SuggestMySQLStmt) chạy được ở từng flavor.

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/testutil"
)

func TestContainer_ResolveMySQL(t *testing.T) {
	for _, image := range []string{"mysql:8.4", "mysql:5.7", "mariadb:10.6"} {
		image := image
		t.Run(image, func(t *testing.T) {
			db := testutil.LegacyMySQL(t, image)
			ctx := context.Background()
			ver, err := mysqlver.Detect(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			mustExec := func(q string) {
				t.Helper()
				if _, err := db.ExecContext(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			mustExec("CREATE DATABASE k")
			mustExec("CREATE TABLE k.t_pk (id INT NOT NULL, a INT NOT NULL, PRIMARY KEY (id), UNIQUE KEY uk_a (a))")
			mustExec("CREATE TABLE k.t_prefix (code VARCHAR(40) NOT NULL, UNIQUE KEY uk_code (code(10)))")
			mustExec("CREATE TABLE k.t_null (a INT NULL, UNIQUE KEY uk_a (a))")
			mustExec("CREATE TABLE k.t_multi (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, UNIQUE KEY uk_abc (a, b, c), UNIQUE KEY uk_ab (a, b), UNIQUE KEY zz_c (c))")
			mustExec("CREATE TABLE k.t_none (a INT, b VARCHAR(20))")
			if !ver.IsMariaDB() && ver.AtLeast(8, 0, 13) {
				mustExec("CREATE TABLE k.t_func (a INT NOT NULL, UNIQUE KEY uk_f ((a + 1)))")
			}
			cases := map[string]struct{ key, why string }{
				"t_pk":     {"PRIMARY (id)", ""},
				"t_prefix": {"", "prefix"},
				"t_null":   {"", "nullable"},
				"t_multi":  {"UNIQUE zz_c (c)", ""},
				"t_none":   {"", ""},
			}
			if !ver.IsMariaDB() && ver.AtLeast(8, 0, 13) {
				cases["t_func"] = struct{ key, why string }{"", "expression"}
			}
			for tb, want := range cases {
				res, err := ResolveMySQL(ctx, db, "k", tb)
				if err != nil {
					t.Fatalf("%s: %v", tb, err)
				}
				got := ""
				if res.Key != nil {
					got = res.Key.Label()
				}
				if got != want.key {
					t.Errorf("%s: key %q, want %q (rejected: %s)", tb, got, want.key, Explain(res.Rejected))
				}
				if want.why != "" && !strings.Contains(Explain(res.Rejected), want.why) {
					t.Errorf("%s: rejected %q lacks %q", tb, Explain(res.Rejected), want.why)
				}
			}

			// Câu gợi ý của Suggest chạy được trên chính server này và biến bảng không khóa thành có khóa.
			flavor := "mysql"
			if ver.IsMariaDB() {
				flavor = "mariadb"
			}
			stmt, invisible := SuggestMySQLStmt(SuggestInput{Flavor: flavor, Major: ver.Major, Minor: ver.Minor, Patch: ver.Patch, Table: model.TableID{Schema: "k", Table: "t_none"}})
			t.Logf("%s: invisible=%v: %s", image, invisible, stmt)
			mustExec("INSERT INTO k.t_none VALUES (1,'x'),(1,'x')")
			mustExec(strings.TrimSuffix(stmt, ";"))
			res, err := ResolveMySQL(ctx, db, "k", "t_none")
			if err != nil || res.Key == nil || !res.Key.IsPrimary() {
				t.Fatalf("after the suggested ALTER: key %+v err %v", res.Key, err)
			}
			wantInv := ver.IsMariaDB() && ver.AtLeast(10, 3, 3) || !ver.IsMariaDB() && ver.AtLeast(8, 0, 23)
			if invisible != wantInv {
				t.Fatalf("invisible=%v, want %v for %s", invisible, wantInv, ver.Raw)
			}
			if invisible {
				var n int
				// SELECT * must not show the key column, an explicit SELECT does.
				rs, err := db.QueryContext(ctx, "SELECT * FROM k.t_none")
				if err != nil {
					t.Fatal(err)
				}
				cols, _ := rs.Columns()
				rs.Close()
				if len(cols) != 2 {
					t.Fatalf("SELECT * returned %v: the key column must be invisible", cols)
				}
				if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM k.t_none", res.Key.Columns[0])).Scan(&n); err != nil || n != 2 {
					t.Fatalf("explicit select of the invisible key: n=%d err=%v", n, err)
				}
				// The column is listed by information_schema (the binlog reader maps columns by position).
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='k' AND TABLE_NAME='t_none'").Scan(&n); err != nil || n != 3 {
					t.Fatalf("information_schema lists %d columns, want 3 (incl. the invisible key)", n)
				}
			}
		})
	}
}

func TestContainer_ResolvePG(t *testing.T) {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16",
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_PASSWORD": "dev"},
			WaitingFor: wait.ForAll(wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp")).WithDeadline(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://postgres:dev@%s/postgres?sslmode=disable", net.JoinHostPort(host, port.Port())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for i := 0; i < 60 && db.PingContext(ctx) != nil; i++ {
		time.Sleep(time.Second)
	}
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE SCHEMA k")
	mustExec("CREATE TABLE k.t_pk (id int PRIMARY KEY, a int NOT NULL)")
	mustExec("CREATE TABLE k.t_ri (a int NOT NULL, b int NOT NULL, n int)")
	mustExec("CREATE UNIQUE INDEX t_ri_ab ON k.t_ri (a, b)")
	mustExec("CREATE UNIQUE INDEX t_ri_a ON k.t_ri (a)")
	mustExec("CREATE UNIQUE INDEX t_ri_n ON k.t_ri (n)")
	mustExec("CREATE UNIQUE INDEX t_ri_part ON k.t_ri (b) WHERE b > 0")
	mustExec("CREATE UNIQUE INDEX t_ri_expr ON k.t_ri ((a + 1))")
	mustExec("CREATE TABLE k.t_none (a int, b text)")

	resolve := func(tb string) *PGResult {
		t.Helper()
		var oid int64
		if err := db.QueryRowContext(ctx, "SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='k' AND c.relname=$1", tb).Scan(&oid); err != nil {
			t.Fatal(err)
		}
		res, err := ResolvePG(ctx, db, model.TableID{Schema: "k", Table: tb}, oid)
		if err != nil {
			t.Fatalf("%s: %v", tb, err)
		}
		return res
	}
	if r := resolve("t_pk"); r.Key == nil || !r.Key.IsPrimary() {
		t.Fatalf("t_pk: %+v", r.Key)
	}
	// DEFAULT replica identity and no PK: no key, the best eligible index is suggested (fewest columns, then name).
	r := resolve("t_ri")
	if r.Key != nil || r.Suggested != "t_ri_a" {
		t.Fatalf("t_ri default identity: key %+v suggested %q (rejected: %s)", r.Key, r.Suggested, Explain(r.Rejected))
	}
	for _, want := range []string{"nullable", "partial", "expression"} {
		if !strings.Contains(Explain(r.Rejected), want) {
			t.Errorf("rejected lacks %q: %s", want, Explain(r.Rejected))
		}
	}
	mustExec("ALTER TABLE k.t_ri REPLICA IDENTITY USING INDEX t_ri_ab")
	if r := resolve("t_ri"); r.Key == nil || r.Key.Label() != "UNIQUE t_ri_ab (a, b)" {
		t.Fatalf("after REPLICA IDENTITY USING INDEX: %+v", r.Key)
	}
	mustExec("ALTER TABLE k.t_ri REPLICA IDENTITY FULL")
	if r := resolve("t_ri"); r.Key != nil || r.Info.RelReplIdent != "f" {
		t.Fatalf("REPLICA IDENTITY FULL is not a key: %+v ident %s", r.Key, r.Info.RelReplIdent)
	}
	// A table with no unique index at all still reports its replica identity.
	if r := resolve("t_none"); r.Key != nil || r.Info.RelReplIdent != "d" {
		t.Fatalf("t_none: %+v ident %q", r.Key, r.Info.RelReplIdent)
	}
}

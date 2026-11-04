package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"strand/internal/model"
	"strand/internal/typemap"
)

func schemaDB(t *testing.T, pg bool) *sql.DB {
	t.Helper()
	ctx := context.Background()
	port := "3306/tcp"
	req := testcontainers.ContainerRequest{Image: "mysql:8.4", ExposedPorts: []string{port}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "test-only", "MYSQL_DATABASE": "strand"}, Cmd: []string{"--innodb-use-native-aio=OFF"}, WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(3 * time.Minute)}
	if pg {
		port = "5432/tcp"
		req = testcontainers.ContainerRequest{Image: "postgres:17", ExposedPorts: []string{port}, Env: map[string]string{"POSTGRES_PASSWORD": "test-only", "POSTGRES_DB": "strand"}, WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute)}
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(context.Background()); err != nil {
			t.Errorf("terminate own test container: %v", err)
		}
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal(err)
	}
	driver := "mysql"
	dsn := fmt.Sprintf("root:test-only@tcp(%s)/strand?parseTime=true", net.JoinHostPort(host, p.Port()))
	if pg {
		driver = "pgx"
		dsn = fmt.Sprintf("postgres://postgres:test-only@%s/strand?sslmode=disable", net.JoinHostPort(host, p.Port()))
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
func execSchema(t *testing.T, db *sql.DB, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}
func applyTwice(t *testing.T, r Request) *Plan {
	t.Helper()
	p, err := Build(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Tables) != len(r.Tables) {
		t.Fatalf("planned tables=%d want=%d", len(p.Tables), len(r.Tables))
	}
	if err := p.Apply(context.Background(), r.Target); err != nil {
		t.Fatal(err)
	}
	again, err := Build(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Statements) != 0 {
		t.Fatalf("second plan is not empty: %v", again.Statements)
	}
	return p
}
func TestSchemaInitThreeDirections(t *testing.T) {
	my := schemaDB(t, false)
	pg := schemaDB(t, true)
	ctx := context.Background()

	t.Run("OwnedSequenceKeepsSequenceType", func(t *testing.T) {
		execSchema(t, pg, "CREATE SCHEMA sequence_source", "CREATE SEQUENCE sequence_source.counter AS bigint START 9 CACHE 7", `CREATE TABLE sequence_source.t(id integer DEFAULT (nextval('sequence_source.counter')*2) PRIMARY KEY)`, "ALTER SEQUENCE sequence_source.counter OWNED BY sequence_source.t.id")
		r := Request{Source: pg, Target: pg, SourceKind: "postgres", TargetKind: "postgres", Tables: []model.TableID{{Schema: "sequence_source", Table: "t"}}, Rename: map[string]string{"sequence_source.t": "sequence_target.t"}}
		applyTwice(t, r)
		var typ string
		if err := pg.QueryRow(`SELECT format_type(s.seqtypid,NULL) FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sequence_target'`).Scan(&typ); err != nil || typ != "bigint" {
			t.Fatalf("sequence type=%s err=%v", typ, err)
		}
		execSchema(t, pg, "INSERT INTO sequence_target.t DEFAULT VALUES")
		var id int
		if err := pg.QueryRow("SELECT id FROM sequence_target.t").Scan(&id); err != nil || id != 18 {
			t.Fatalf("id=%d err=%v", id, err)
		}
	})
	t.Run("MySQLToMySQL", func(t *testing.T) {
		execSchema(t, my, "CREATE DATABASE src CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "CREATE TABLE src.parent (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, code varchar(32) UNIQUE) AUTO_INCREMENT=543", "CREATE TABLE src.child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES src.parent(id))", "CREATE TABLE src.part (id bigint PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2", "CREATE TRIGGER src.trig BEFORE INSERT ON src.parent FOR EACH ROW SET NEW.code=UPPER(NEW.code)")
		tables := []model.TableID{{Schema: "src", Table: "child"}, {Schema: "src", Table: "parent"}, {Schema: "src", Table: "part"}}
		r := Request{Source: my, Target: my, SourceKind: "mysql", TargetKind: "mysql", Tables: tables, Rename: map[string]string{"src.child": "dst.Kid", "src.parent": "dst.Parent", "src.part": "dst.Part"}}
		p := applyTwice(t, r)
		// Mô phỏng crash sau DDL của bảng đầu tiên (MySQL auto-commit).
		resume := r
		resume.Rename = map[string]string{"src.child": "resumed.Kid", "src.parent": "resumed.Parent", "src.part": "resumed.Part"}
		first, err := Build(ctx, resume)
		if err != nil {
			t.Fatal(err)
		}
		half := &Plan{Statements: first.Statements[:2]}
		if err := half.Apply(ctx, my); err != nil {
			t.Fatal(err)
		}
		rest, err := Build(ctx, resume)
		if err != nil {
			t.Fatal(err)
		}
		if len(rest.Tables) != 2 {
			t.Fatalf("resume planned %d tables, want 2", len(rest.Tables))
		}
		if err := rest.Apply(ctx, my); err != nil {
			t.Fatal(err)
		}
		empty, err := Build(ctx, resume)
		if err != nil || len(empty.Statements) != 0 {
			t.Fatalf("resume not idempotent: %+v %v", empty, err)
		}
		// Bảng đã có được giữ nguyên cả cấu trúc lẫn row riêng của target.
		execSchema(t, my, "CREATE TABLE dst.untouched (different_id int PRIMARY KEY, target_only text)", "INSERT INTO dst.untouched VALUES (9,'keep')")
		existing := r
		existing.Tables = []model.TableID{{Schema: "src", Table: "parent"}}
		existing.Rename = map[string]string{"src.parent": "dst.untouched"}
		untouched, err := Build(ctx, existing)
		if err != nil || len(untouched.Statements) != 0 {
			t.Fatalf("existing table planned for mutation: %+v %v", untouched, err)
		}
		var preserved string
		if err := my.QueryRow("SELECT target_only FROM dst.untouched WHERE different_id=9").Scan(&preserved); err != nil || preserved != "keep" {
			t.Fatalf("target changed: %q %v", preserved, err)
		}

		if !strings.Contains(strings.Join(p.Warnings, "\n"), "src.trig") {
			t.Fatal("missing trigger warning")
		}
		var n int
		if err := my.QueryRow("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='dst'").Scan(&n); err != nil || n != 0 {
			t.Fatalf("triggers=%d err=%v", n, err)
		}
		var schema, ddl string
		if err := my.QueryRow("SHOW CREATE TABLE dst.Kid").Scan(&schema, &ddl); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ddl, "REFERENCES `Parent`") {
			t.Fatal(ddl)
		}
		var coll string
		if err := my.QueryRow("SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='dst'").Scan(&coll); err != nil || coll != "utf8mb4_bin" {
			t.Fatalf("collation=%s err=%v", coll, err)
		}
	})
	t.Run("MySQLToPG", func(t *testing.T) {
		execSchema(t, my, "CREATE DATABASE crossdb", "CREATE TABLE crossdb.items (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, code varchar(40) COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, amount decimal(20,4) NOT NULL DEFAULT 1.25, status enum('new','done') NOT NULL DEFAULT 'new', data json, ts timestamp(6), KEY amount_idx(amount))")
		r := Request{Source: my, Target: pg, SourceKind: "mysql", TargetKind: "postgres", Tables: []model.TableID{{Schema: "crossdb", Table: "items"}}}
		execSchema(t, my,
			"CREATE TABLE crossdb.tuple_pk (a int, b varchar(20) COLLATE utf8mb4_bin, val binary(8), PRIMARY KEY(a,b))",
			"CREATE TABLE crossdb.child (id bigint PRIMARY KEY, item_id int, FOREIGN KEY(item_id) REFERENCES crossdb.items(id))",
			"CREATE TABLE crossdb.values_table (id int PRIMARY KEY, u bigint unsigned, amount decimal(30,10), blobval varbinary(20), dt datetime(6), yr year)",
			"CREATE TABLE crossdb.enums (id int PRIMARY KEY, status enum('a','b'), flags set('x','y'))",
			"CREATE TABLE crossdb.quoted (id int PRIMARY KEY, `Order` varchar(100), `a``b` text)")
		for _, table := range []string{"tuple_pk", "child", "values_table", "enums", "quoted"} {
			r.Tables = append(r.Tables, model.TableID{Schema: "crossdb", Table: table})
		}
		applyTwice(t, r)
		warns, err := typemap.CheckMySQLToPGTables(ctx, my, pg, []string{"crossdb.items", "crossdb.tuple_pk", "crossdb.child", "crossdb.values_table", "crossdb.enums", "crossdb.quoted"}, typemap.DefaultOptions())
		if err != nil {
			t.Fatalf("validate: %v warnings=%v", err, warns)
		}
		execSchema(t, pg, `INSERT INTO crossdb.items(code) VALUES ('a')`)
		var n int
		if err := pg.QueryRow(`SELECT count(*) FROM pg_depend d JOIN pg_class c ON c.oid=d.refobjid JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname='crossdb' AND c.relname='items' AND d.deptype='i' AND d.refobjsubid>0`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("identity dependencies=%d err=%v", n, err)
		}
	})

	t.Run("UnsupportedTypesFailBeforeMutation", func(t *testing.T) {
		execSchema(t, pg, "CREATE SCHEMA custom_source", "CREATE TYPE custom_source.status AS ENUM ('a','b')", "CREATE TABLE custom_source.t(id int PRIMARY KEY,v custom_source.status)")
		r := Request{Source: pg, Target: pg, SourceKind: "postgres", TargetKind: "postgres", Tables: []model.TableID{{Schema: "custom_source", Table: "t"}}, Rename: map[string]string{"custom_source.t": "custom_target.t"}}
		if _, err := Build(ctx, r); err == nil || !strings.Contains(err.Error(), "user-defined type") {
			t.Fatalf("expected actionable enum error: %v", err)
		}
		var n int
		if err := pg.QueryRow("SELECT count(*) FROM pg_namespace WHERE nspname='custom_target'").Scan(&n); err != nil || n != 0 {
			t.Fatalf("Build mutated target: count=%d err=%v", n, err)
		}
	})

	t.Run("SessionRestoreAndPGAtomicity", func(t *testing.T) {
		my.SetMaxOpenConns(1)
		defer my.SetMaxOpenConns(0)
		execSchema(t, my, "SET SESSION FOREIGN_KEY_CHECKS=1")
		p := &Plan{Statements: []string{"CREATE DATABASE IF NOT EXISTS session_check", "INVALID SQL"}}
		if err := p.Apply(ctx, my); err == nil {
			t.Fatal("invalid DDL succeeded")
		}
		var checks int
		if err := my.QueryRow("SELECT @@SESSION.FOREIGN_KEY_CHECKS").Scan(&checks); err != nil || checks != 1 {
			t.Fatalf("leaked FOREIGN_KEY_CHECKS=%d err=%v", checks, err)
		}
		p = &Plan{postgres: true, Statements: []string{"CREATE SCHEMA atomic_check", "CREATE TABLE atomic_check.t(id int PRIMARY KEY)", "INVALID SQL"}}
		if err := p.Apply(ctx, pg); err == nil {
			t.Fatal("invalid DDL succeeded")
		}
		var exists bool
		if err := pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='atomic_check')").Scan(&exists); err != nil || exists {
			t.Fatalf("partial PG schema survived: %v err=%v", exists, err)
		}
	})
	t.Run("PGToPG", func(t *testing.T) {
		execSchema(t, pg, `CREATE SCHEMA sourcepg`, `CREATE TABLE sourcepg.parent (id bigint GENERATED ALWAYS AS IDENTITY (START 7 INCREMENT 3 CACHE 10) PRIMARY KEY, code text COLLATE "C" UNIQUE, amount numeric(20,3) CHECK(amount>=0), doubled numeric GENERATED ALWAYS AS (amount*2) STORED)`, `CREATE TABLE sourcepg.child (id bigserial PRIMARY KEY, pid bigint REFERENCES sourcepg.parent(id))`, `CREATE INDEX expr_idx ON sourcepg.parent ((lower(code))) WHERE amount>0`, `CREATE TABLE sourcepg.part (id bigint PRIMARY KEY) PARTITION BY RANGE(id)`, `CREATE TABLE sourcepg.part_a PARTITION OF sourcepg.part FOR VALUES FROM (0) TO (100)`)
		r := Request{Source: pg, Target: pg, SourceKind: "postgres", TargetKind: "postgres", Tables: []model.TableID{{Schema: "sourcepg", Table: "parent"}, {Schema: "sourcepg", Table: "child"}, {Schema: "sourcepg", Table: "part"}}, Rename: map[string]string{"sourcepg.parent": "targetpg.Parent", "sourcepg.child": "targetpg.Child", "sourcepg.part": "targetpg.Part"}}

		execSchema(t, pg,
			`CREATE TABLE sourcepg.standalone (id bigint PRIMARY KEY, code text COLLATE "C")`,
			`CREATE UNIQUE INDEX standalone_code_idx ON sourcepg.standalone(code)`,
			`CREATE TABLE sourcepg.reference (id bigint PRIMARY KEY, code text REFERENCES sourcepg.standalone(code))`,
			`CREATE TABLE sourcepg."Order" (id bigint PRIMARY KEY, "A""B" text, span int4range, EXCLUDE USING gist(span WITH &&))`)
		for _, pair := range [][2]string{{"standalone", "Standalone"}, {"reference", "Reference"}, {"Order", "NewOrder"}} {
			r.Tables = append(r.Tables, model.TableID{Schema: "sourcepg", Table: pair[0]})
			r.Rename["sourcepg."+pair[0]] = "targetpg." + pair[1]
		}
		applyTwice(t, r)
		execSchema(t, pg, `INSERT INTO targetpg."Parent"(code,amount) VALUES ('A',5)`, `INSERT INTO targetpg."Child"(pid) VALUES (7)`, `INSERT INTO targetpg."Part" VALUES (1)`)

		execSchema(t, pg, `INSERT INTO targetpg."Standalone" VALUES (1,'a')`, `INSERT INTO targetpg."Reference" VALUES (1,'a')`, `INSERT INTO targetpg."NewOrder" VALUES(1,'quoted','[1,4)')`)
		var doubled int
		if err := pg.QueryRow(`SELECT doubled::integer FROM targetpg."Parent"`).Scan(&doubled); err != nil || doubled != 10 {
			t.Fatalf("generated=%d err=%v", doubled, err)
		}
		var inc, cache int
		if err := pg.QueryRow(`SELECT s.seqincrement,s.seqcache FROM pg_sequence s JOIN pg_depend d ON d.objid=s.seqrelid JOIN pg_class c ON c.oid=d.refobjid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='targetpg' AND c.relname='Parent'`).Scan(&inc, &cache); err != nil || inc != 3 || cache != 10 {
			t.Fatalf("sequence inc=%d cache=%d err=%v", inc, cache, err)
		}
	})
}

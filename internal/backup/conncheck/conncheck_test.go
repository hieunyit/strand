package conncheck

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/backup"
	"strand/internal/testutil"
)

func TestParseGrantsAndMissing(t *testing.T) {
	cases := []struct {
		name    string
		grants  []string
		missing []string
	}{
		{"all", []string{"GRANT ALL PRIVILEGES ON *.* TO `u`@`%` WITH GRANT OPTION"}, nil},
		{"full set", []string{"GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT, SHOW VIEW, EVENT, TRIGGER ON *.* TO `u`@`%`"}, nil},
		{"db scoped select only", []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON `app`.* TO `u`@`%`"},
			[]string{"RELOAD", "REPLICATION CLIENT"}},
		{"global-only privileges at db scope do not count", []string{"GRANT RELOAD, REPLICATION CLIENT ON `app`.* TO `u`@`%`"},
			[]string{"SELECT", "RELOAD", "LOCK TABLES", "REPLICATION CLIENT", "SHOW VIEW", "EVENT", "TRIGGER"}},
		{"mariadb binlog monitor", []string{"GRANT SELECT, RELOAD, LOCK TABLES, BINLOG MONITOR, SHOW VIEW, EVENT, TRIGGER ON *.* TO `u`@`%`"}, nil},
		{"column grant ignored commas", []string{"GRANT SELECT (a, b), RELOAD ON *.* TO `u`@`%`"},
			[]string{"LOCK TABLES", "REPLICATION CLIENT", "SHOW VIEW", "EVENT", "TRIGGER"}},
		{"role grant line", []string{"GRANT `r1`@`%` TO `u`@`%`"}, []string{"SELECT", "RELOAD", "LOCK TABLES", "REPLICATION CLIENT", "SHOW VIEW", "EVENT", "TRIGGER"}},
	}
	for _, c := range cases {
		g, a := parseGrants(c.grants)
		_, missing := missingMySQL(g, a)
		if !reflect.DeepEqual(missing, c.missing) {
			t.Fatalf("%s: missing = %v, want %v", c.name, missing, c.missing)
		}
	}
}

func TestRedactsSecrets(t *testing.T) {
	// A refused connection must never echo the password back.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().(*net.TCPAddr)
	l.Close() // nothing listens: connection refused
	for _, eng := range []string{backup.ConnMySQL, backup.ConnPostgres, backup.ConnMongoDB} {
		res := Live{Timeout: 5 * time.Second}.Check(context.Background(), Target{Engine: eng, Host: "127.0.0.1", Port: addr.Port, User: "u", Password: "hunter2-secret"})
		if res.OK || res.Error == "" {
			t.Fatalf("%s: %+v", eng, res)
		}
		if b := fmt.Sprintf("%+v", res); strings.Contains(b, "hunter2-secret") {
			t.Fatalf("%s: result leaks the password: %s", eng, b)
		}
		if res.PITRAllowed || res.Databases == nil || res.MissingPrivileges == nil {
			t.Fatalf("%s: unreachable result shape: %+v", eng, res)
		}
	}
	res := Live{}.Check(context.Background(), Target{Engine: "oracle"})
	if res.OK || res.Error == "" {
		t.Fatalf("unknown engine: %+v", res)
	}
}

var ctSeq atomic.Int64

func startContainer(t *testing.T, kind string, req testcontainers.ContainerRequest) (host, port string) {
	t.Helper()
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	prefix := os.Getenv("STRAND_TEST_CONTAINER_PREFIX")
	if prefix == "" {
		prefix = "strand-test"
	}
	req.Name = fmt.Sprintf("%s-conncheck-%s-%d-%d", prefix, kind, os.Getpid(), ctSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		t.Cleanup(func() {
			cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
			defer ccancel()
			_ = c.Terminate(cctx)
		})
	}
	if err != nil {
		t.Fatalf("start %s: %v", kind, err)
	}
	h, _ := c.Host(ctx)
	p, err := c.MappedPort(ctx, "3306/tcp")
	if kind == "pg" {
		p, err = c.MappedPort(ctx, "5432/tcp")
	}
	if err != nil {
		t.Fatal(err)
	}
	return h, p.Port()
}

func waitDB(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		db, err := sql.Open(driver, dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				t.Cleanup(func() { db.Close() })
				return db
			}
			db.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("database not ready: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func execAll(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func checkNames(r Result) map[string]string {
	m := map[string]string{}
	for _, c := range r.Checks {
		m[c.Name] = c.Status
	}
	return m
}

func startMySQLContainer(t *testing.T, image string, args ...string) (*sql.DB, Target) {
	h, p := startContainer(t, "mysql", testcontainers.ContainerRequest{
		Image: image, ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "rootpw"},
		Cmd:        append([]string{"--server-id=1", "--skip-name-resolve", "--mysqlx=OFF"}, args...),
		WaitingFor: wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	})
	db := waitDB(t, "mysql", "root:rootpw@tcp("+net.JoinHostPort(h, p)+")/?multiStatements=true")
	port, _ := strconv.Atoi(p)
	return db, Target{Engine: backup.ConnMySQL, Host: h, Port: port}
}

func TestMySQLContainer(t *testing.T) {
	db, tgt := startMySQLContainer(t, "mysql:8.4", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-bin=binlog", "--binlog-format=ROW")
	execAll(t, db,
		"CREATE DATABASE appdb", "CREATE DATABASE otherdb",
		"CREATE USER 'full'@'%' IDENTIFIED BY 'fullpw'",
		"GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'full'@'%'",
		"CREATE USER 'weak'@'%' IDENTIFIED BY 'weakpw'",
		"GRANT SELECT ON appdb.* TO 'weak'@'%'")

	full := tgt
	full.User, full.Password = "full", "fullpw"
	r := Live{}.Check(context.Background(), full)
	if !r.OK || !strings.HasPrefix(r.Version, "8.4") || len(r.MissingPrivileges) != 0 || r.GTIDMode != "ON" ||
		r.BinlogEnabled == nil || !*r.BinlogEnabled || !reflect.DeepEqual(r.Databases, []string{"appdb", "otherdb"}) {
		t.Fatalf("full user: %+v", r)
	}
	if !r.PITRAllowed || len(r.PITRMethods) != 1 || r.PITRMethods[0] != backup.MethodMySQLDump {
		t.Fatalf("pitr = %v %q %v", r.PITRAllowed, r.PITRReason, r.PITRMethods)
	}
	if st := checkNames(r); st["connect"] != "ok" || st["privileges"] != "ok" || st["gtid_mode"] != "ok" || st["databases"] != "ok" {
		t.Fatalf("checks = %+v", r.Checks)
	}

	weak := tgt
	weak.User, weak.Password = "weak", "weakpw"
	r = Live{}.Check(context.Background(), weak)
	want := []string{"RELOAD", "LOCK TABLES", "REPLICATION CLIENT", "SHOW VIEW", "EVENT", "TRIGGER"}
	if !r.OK || !sameSet(r.MissingPrivileges, want) {
		t.Fatalf("weak user missing = %v, want %v", r.MissingPrivileges, want)
	}
	if !reflect.DeepEqual(r.Databases, []string{"appdb"}) {
		t.Fatalf("weak user databases = %v", r.Databases)
	}

	bad := full
	bad.Password = "wrong-password"
	r = Live{}.Check(context.Background(), bad)
	if r.OK || r.Error == "" || strings.Contains(r.Error, "wrong-password") || r.PITRAllowed {
		t.Fatalf("bad password: %+v", r)
	}

	// MariaDB-typed connection to a MySQL server warns; PITR for mariadb is never allowed.
	m := full
	m.Engine = backup.ConnMariaDB
	r = Live{}.Check(context.Background(), m)
	if r.PITRAllowed || !strings.Contains(r.PITRReason, "MariaDB") || checkNames(r)["version"] != "warn" {
		t.Fatalf("mariadb on mysql: %+v", r)
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		if !m[x] {
			return false
		}
	}
	return true
}

func TestMySQLContainerNoGTID(t *testing.T) {
	db, tgt := startMySQLContainer(t, "mysql:8.4", "--log-bin=binlog")
	execAll(t, db, "CREATE USER 'u'@'%' IDENTIFIED BY 'pw'", "GRANT ALL ON *.* TO 'u'@'%'")
	tgt.User, tgt.Password = "u", "pw"
	r := Live{}.Check(context.Background(), tgt)
	if !r.OK || r.GTIDMode != "OFF" || r.PITRAllowed || !strings.Contains(r.PITRReason, "gtid_mode") || len(r.MissingPrivileges) != 0 {
		t.Fatalf("no gtid: %+v", r)
	}
}

func TestMariaDBContainer(t *testing.T) {
	h, p := startContainer(t, "mysql", testcontainers.ContainerRequest{
		Image: "mariadb:10.11", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MARIADB_ROOT_PASSWORD": "rootpw"},
		Cmd:        []string{"--log-bin=binlog", "--server-id=1"},
		WaitingFor: wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	})
	waitDB(t, "mysql", "root:rootpw@tcp("+net.JoinHostPort(h, p)+")/")
	port, _ := strconv.Atoi(p)
	r := Live{}.Check(context.Background(), Target{Engine: backup.ConnMariaDB, Host: h, Port: port, User: "root", Password: "rootpw"})
	if !r.OK || !strings.Contains(strings.ToLower(r.Version), "mariadb") || len(r.MissingPrivileges) != 0 {
		t.Fatalf("mariadb root: %+v", r)
	}
	if r.PITRAllowed || !strings.Contains(r.PITRReason, "MariaDB does not support") {
		t.Fatalf("mariadb pitr: %v %q", r.PITRAllowed, r.PITRReason)
	}
	if checkNames(r)["version"] != "ok" {
		t.Fatalf("checks = %+v", r.Checks)
	}
}

func startPGContainer(t *testing.T, args ...string) (*sql.DB, Target) {
	h, p := startContainer(t, "pg", testcontainers.ContainerRequest{
		Image: "postgres:17", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_PASSWORD": "pgpw"},
		Cmd: append([]string{"postgres", "-c", "fsync=off"}, args...),
		WaitingFor: wait.ForAll(wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp")).WithDeadline(3 * time.Minute),
	})
	db := waitDB(t, "pgx", "postgres://postgres:pgpw@"+net.JoinHostPort(h, p)+"/postgres?sslmode=disable")
	port, _ := strconv.Atoi(p)
	return db, Target{Engine: backup.ConnPostgres, Host: h, Port: port, TLSMode: "disable"}
}

func TestPostgresContainer(t *testing.T) {
	db, tgt := startPGContainer(t, "-c", "wal_level=replica", "-c", "max_wal_senders=5")
	execAll(t, db, "CREATE DATABASE appdb", "CREATE ROLE repl LOGIN REPLICATION PASSWORD 'replpw'", "CREATE ROLE plain LOGIN PASSWORD 'plainpw'")

	repl := tgt
	repl.User, repl.Password = "repl", "replpw"
	r := Live{}.Check(context.Background(), repl)
	if !r.OK || !strings.HasPrefix(r.Version, "17") || r.WALLevel != "replica" || r.ReplicationRole == nil || !*r.ReplicationRole ||
		len(r.MissingPrivileges) != 0 || !reflect.DeepEqual(r.Databases, []string{"appdb", "postgres"}) {
		t.Fatalf("replication role: %+v", r)
	}
	if !r.PITRAllowed || len(r.PITRMethods) != 1 || r.PITRMethods[0] != backup.MethodPGBaseBackup {
		t.Fatalf("pitr = %v %q %v", r.PITRAllowed, r.PITRReason, r.PITRMethods)
	}

	plain := tgt
	plain.User, plain.Password = "plain", "plainpw"
	r = Live{}.Check(context.Background(), plain)
	if !r.OK || r.ReplicationRole == nil || *r.ReplicationRole || !reflect.DeepEqual(r.MissingPrivileges, []string{"REPLICATION"}) ||
		r.PITRAllowed || !strings.Contains(r.PITRReason, "REPLICATION") {
		t.Fatalf("plain role: %+v", r)
	}

	bad := repl
	bad.Password = "wrong-password"
	r = Live{}.Check(context.Background(), bad)
	if r.OK || r.Error == "" || strings.Contains(r.Error, "wrong-password") {
		t.Fatalf("bad password: %+v", r)
	}
}

func TestPostgresContainerMinimalWAL(t *testing.T) {
	db, tgt := startPGContainer(t, "-c", "wal_level=minimal", "-c", "max_wal_senders=0", "-c", "archive_mode=off")
	_ = db
	tgt.User, tgt.Password = "postgres", "pgpw"
	r := Live{}.Check(context.Background(), tgt)
	if !r.OK || r.WALLevel != "minimal" || r.PITRAllowed || !strings.Contains(r.PITRReason, "wal_level") || checkNames(r)["max_wal_senders"] != "warn" {
		t.Fatalf("minimal wal: %+v", r)
	}
}

func TestMongoContainer(t *testing.T) {
	rs := testutil.MongoReplicaSet(t)
	r := Live{}.Check(context.Background(), Target{Engine: backup.ConnMongoDB, URI: rs.URI})
	if !r.OK || r.Version == "" || r.IsReplicaSet == nil || !*r.IsReplicaSet || r.ReplicaSet != "rs0" || !r.PITRAllowed ||
		len(r.PITRMethods) != 1 || r.PITRMethods[0] != backup.MethodMongodump {
		t.Fatalf("mongo rs: %+v", r)
	}
}

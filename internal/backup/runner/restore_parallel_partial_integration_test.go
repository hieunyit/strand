package runner

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
	"strand/internal/objectstore"
	"strand/internal/testutil"
	"strand/internal/web/store"
)

func startRPPContainer(t *testing.T, name, dir string, args ...string) string {
	t.Helper()
	full := fmt.Sprintf("%s-%s-%d-%d", containerPrefix("rpp"), name, os.Getpid(), time.Now().UnixNano()%100000)
	_ = exec.Command("docker", "rm", "-f", full).Run()
	run := append([]string{"run", "-d", "--name", full, "--network", "host", "-v", dir + ":" + dir}, args...)
	docker(t, run...)
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "--tail", "40", full).CombinedOutput()
			t.Logf("%s logs:\n%s", full, out)
		}
		_ = exec.Command("docker", "rm", "-f", "-v", full).Run()
	})
	return full
}

func startRPPMinIO(t *testing.T, dir string, buckets ...string) map[string]string {
	port := freePort(t)
	cPort := freePort(t)
	startRPPContainer(t, "minio", dir, "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin", "--entrypoint", testutil.MinIOEntrypoint, testutil.PinnedMinIOImage,
		"server", "/tmp/minio-data", "--address", fmt.Sprintf("127.0.0.1:%d", port), "--console-address", fmt.Sprintf("127.0.0.1:%d", cPort))
	env := map[string]string{
		"STRAND_BACKUP_S3_ENDPOINT":             fmt.Sprintf("http://127.0.0.1:%d", port),
		"STRAND_BACKUP_S3_ACCESS_KEY":           "minioadmin",
		"STRAND_BACKUP_S3_SECRET_KEY":           "minioadmin",
		"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true",
	}
	for _, b := range buckets {
		deadline := time.Now().Add(time.Minute)
		for {
			obj, err := objectstore.New(objectstore.Config{
				Endpoint:           env["STRAND_BACKUP_S3_ENDPOINT"],
				Bucket:             b,
				AccessKey:          "minioadmin",
				SecretKey:          "minioadmin",
				AllowInsecureLocal: true,
				MaxRetries:         -1,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = obj.CreateBucket(context.Background())
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("create bucket %s: %v", b, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return env
}

func startRPPPG(t *testing.T, dir string) pgFixture {
	port := freePort(t)
	args := []string{
		"-e", "POSTGRES_PASSWORD=pgpw", "postgres:17", "-p", fmt.Sprint(port),
		"-c", "wal_level=replica", "-c", "max_wal_senders=5", "-c", "listen_addresses=127.0.0.1",
	}
	c := startRPPContainer(t, "pg", dir, args...)
	db := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
	time.Sleep(2 * time.Second)
	db = waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
	return pgFixture{Container: c, Port: port, DB: db}
}

func startRPPMySQL(t *testing.T, dir, image string) mysqlFixture {
	port := freePort(t)
	c := startRPPContainer(t, "mysql", dir, "-e", "MYSQL_ROOT_PASSWORD=rootpw", image, fmt.Sprintf("--port=%d", port),
		"--mysqlx=OFF", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--server-id=1", "--log-bin=binlog",
		"--binlog-format=ROW", "--skip-name-resolve")
	db := waitSQL(t, "mysql", fmt.Sprintf("root:rootpw@tcp(127.0.0.1:%d)/?multiStatements=true&parseTime=true", port))
	return mysqlFixture{Container: c, Port: port, DB: db}
}

type mongoFixture struct {
	Container string
	Port      int
	Client    *mongo.Client
}

func startRPPMongo(t *testing.T, dir string) mongoFixture {
	port := freePort(t)
	c := startRPPContainer(t, "mongo", dir, "mongo:7.0", "--port", fmt.Sprint(port), "--bind_ip", "127.0.0.1")
	uri := fmt.Sprintf("mongodb://127.0.0.1:%d", port)
	deadline := time.Now().Add(time.Minute)
	var client *mongo.Client
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var err error
		client, err = mongo.Connect(options.Client().ApplyURI(uri))
		if err == nil {
			err = client.Ping(ctx, nil)
		}
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mongo not ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return mongoFixture{Container: c, Port: port, Client: client}
}

func TestPGDumpParallelAndPartialRestore(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	tools := filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars := startRPPMinIO(t, dir, "pg-backups")
	pg := startRPPPG(t, dir)

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	vars["STRAND_BACKUP_ENCRYPTION_KEYS"] = "k2026:" + key
	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["PG_BACKUP_PW"] = "pgpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}

	wrapper(t, tools, "pg_dump", pg.Container, "pg_dump", "PGPASSWORD")
	wrapper(t, tools, "pg_restore", pg.Container, "pg_restore", "PGPASSWORD")

	// Seed source database with 3 tables: t1 (with sequence + index), t2 (with FK to t1), t3
	mustExec(t, pg.DB, "CREATE DATABASE app")
	pgApp := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app?sslmode=disable", pg.Port))

	mustExec(t, pgApp, "CREATE TABLE t1 (id serial primary key, note text, created_at timestamp default now())")
	mustExec(t, pgApp, "CREATE INDEX idx_t1_created ON t1(created_at)")
	mustExec(t, pgApp, "INSERT INTO t1 (note) SELECT 'note ' || g FROM generate_series(1, 50) g")

	mustExec(t, pgApp, "CREATE TABLE t2 (id serial primary key, t1_id int references t1(id), payload text)")
	mustExec(t, pgApp, "INSERT INTO t2 (t1_id, payload) SELECT (g % 50) + 1, 'payload ' || g FROM generate_series(1, 100) g")

	mustExec(t, pgApp, "CREATE TABLE t3 (id serial primary key, val text)")
	mustExec(t, pgApp, "INSERT INTO t3 (val) SELECT 'val ' || g FROM generate_series(1, 200) g")

	mustExec(t, pgApp, "CREATE TABLE orders (id serial primary key, info text, created_at timestamp default now())")
	mustExec(t, pgApp, "CREATE INDEX idx_orders_created ON orders(created_at)")
	mustExec(t, pgApp, "INSERT INTO orders (info) SELECT 'order ' || g FROM generate_series(1, 30) g")

	mustExec(t, pgApp, "CREATE TABLE orders_archive (id serial primary key, info text, created_at timestamp default now())")
	mustExec(t, pgApp, "CREATE INDEX idx_orders_archive_created ON orders_archive(created_at)")
	mustExec(t, pgApp, "INSERT INTO orders_archive (info) SELECT 'archive ' || g FROM generate_series(1, 40) g")

	st := store.NewMem()
	e := testEnv(t, vars, st)
	ctx := context.Background()

	pgJob := backup.Job{
		ID:           "bjob-pg",
		Name:         "pgdump",
		EverySeconds: 3600,
		Bucket:       "pg-backups",
		JobOptions: backup.JobOptions{
			Method: backup.MethodPGDump,
			Source: "pg-prod",
			Conn: backup.ConnSpec{
				Host:        "127.0.0.1",
				Port:        pg.Port,
				User:        "postgres",
				PasswordEnv: "PG_BACKUP_PW",
				Databases:   []string{"app"},
			},
		},
	}
	rec, err := e.RunJob(ctx, pgJob)
	if err != nil {
		t.Fatalf("RunJob pg_dump: %v", err)
	}
	if rec.Status != backup.StatusComplete {
		t.Fatalf("pg_dump backup status: %v", rec.Status)
	}

	plan := RestorePlan{Chain: []backup.Record{rec}}

	// 1. Partial restore with threads=4, objects=["t1"] into NEW database "app_partial"
	partialTarget := RestoreTarget{
		Conn: backup.ConnSpec{
			Host:        "127.0.0.1",
			Port:        pg.Port,
			User:        "postgres",
			PasswordEnv: "PG_BACKUP_PW",
		},
		Database: "app_partial",
		Threads:  4,
		Objects:  []string{"t1"},
	}
	if err := e.Restore(ctx, plan, partialTarget); err != nil {
		t.Fatalf("Restore partial with threads=4: %v", err)
	}

	partialDB := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app_partial?sslmode=disable", pg.Port))
	// Verify only t1 exists
	var tableCount int
	if err := partialDB.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 1 {
		t.Fatalf("expected exactly 1 table in partial restore, got %d", tableCount)
	}

	var t1Count int
	if err := partialDB.QueryRow("SELECT count(*) FROM t1").Scan(&t1Count); err != nil || t1Count != 50 {
		t.Fatalf("t1 row count want 50, got %d (err: %v)", t1Count, err)
	}

	// Verify sequence exists and works
	var nextSeq int64
	if err := partialDB.QueryRow("SELECT nextval('t1_id_seq')").Scan(&nextSeq); err != nil || nextSeq <= 50 {
		t.Fatalf("t1 sequence check failed: nextval=%d, err=%v", nextSeq, err)
	}

	// Verify index exists
	var indexCount int
	if err := partialDB.QueryRow("SELECT count(*) FROM pg_indexes WHERE tablename = 't1' AND indexname = 'idx_t1_created'").Scan(&indexCount); err != nil || indexCount != 1 {
		t.Fatalf("expected index idx_t1_created, got %d (err: %v)", indexCount, err)
	}

	// 2. Partial restore of "orders" table only (regression test for P2: orders vs orders_archive)
	// Must succeed without trying to restore idx_orders_archive_created or orders_archive_id_seq
	ordersTarget := RestoreTarget{
		Conn: backup.ConnSpec{
			Host:        "127.0.0.1",
			Port:        pg.Port,
			User:        "postgres",
			PasswordEnv: "PG_BACKUP_PW",
		},
		Database: "app_orders_only",
		Threads:  4,
		Objects:  []string{"orders"},
	}
	if err := e.Restore(ctx, plan, ordersTarget); err != nil {
		t.Fatalf("Restore orders only failed (P2 regression): %v", err)
	}

	ordersDB := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app_orders_only?sslmode=disable", pg.Port))
	var ordersCount int
	if err := ordersDB.QueryRow("SELECT count(*) FROM orders").Scan(&ordersCount); err != nil || ordersCount != 30 {
		t.Fatalf("orders row count want 30, got %d (err: %v)", ordersCount, err)
	}
	// Verify orders_archive does NOT exist
	var archiveCount int
	if err := ordersDB.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders_archive'").Scan(&archiveCount); err != nil || archiveCount != 0 {
		t.Fatalf("expected orders_archive to not exist, got %d (err: %v)", archiveCount, err)
	}
	// Verify idx_orders_archive_created does NOT exist
	var archiveIdxCount int
	if err := ordersDB.QueryRow("SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_orders_archive_created'").Scan(&archiveIdxCount); err != nil || archiveIdxCount != 0 {
		t.Fatalf("expected idx_orders_archive_created to not exist, got %d (err: %v)", archiveIdxCount, err)
	}

	// 3. Full restore with threads=4 into NEW database "app_full"
	fullTarget := RestoreTarget{
		Conn: backup.ConnSpec{
			Host:        "127.0.0.1",
			Port:        pg.Port,
			User:        "postgres",
			PasswordEnv: "PG_BACKUP_PW",
		},
		Database: "app_full",
		Threads:  4,
	}
	if err := e.Restore(ctx, plan, fullTarget); err != nil {
		t.Fatalf("Restore full with threads=4: %v", err)
	}

	fullDB := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app_full?sslmode=disable", pg.Port))
	var c1, c2, c3 int
	if err := fullDB.QueryRow("SELECT count(*) FROM t1").Scan(&c1); err != nil || c1 != 50 {
		t.Fatalf("full restore t1 want 50, got %d (%v)", c1, err)
	}
	if err := fullDB.QueryRow("SELECT count(*) FROM t2").Scan(&c2); err != nil || c2 != 100 {
		t.Fatalf("full restore t2 want 100, got %d (%v)", c2, err)
	}
	if err := fullDB.QueryRow("SELECT count(*) FROM t3").Scan(&c3); err != nil || c3 != 200 {
		t.Fatalf("full restore t3 want 200, got %d (%v)", c3, err)
	}
	var co, ca int
	if err := fullDB.QueryRow("SELECT count(*) FROM orders").Scan(&co); err != nil || co != 30 {
		t.Fatalf("full restore orders want 30, got %d (%v)", co, err)
	}
	if err := fullDB.QueryRow("SELECT count(*) FROM orders_archive").Scan(&ca); err != nil || ca != 40 {
		t.Fatalf("full restore orders_archive want 40, got %d (%v)", ca, err)
	}
}

func TestMySQLDumpPartialRestore(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	tools := filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars := startRPPMinIO(t, dir, "mysql-backups")
	my := startRPPMySQL(t, dir, "mysql:8.4")

	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["MYSQL_BACKUP_PW"] = "rootpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}

	wrapper(t, tools, "mysqldump", my.Container, "mysqldump", "MYSQL_PWD")
	wrapper(t, tools, "mysql", my.Container, "mysql", "MYSQL_PWD")

	// Seed source database with 3 tables
	mustExec(t, my.DB, "CREATE DATABASE shop")
	mustExec(t, my.DB, "CREATE TABLE shop.t_one (id INT PRIMARY KEY, name VARCHAR(50))")
	mustExec(t, my.DB, "CREATE TABLE shop.t_two (id INT PRIMARY KEY, val VARCHAR(50))")
	mustExec(t, my.DB, "CREATE TABLE shop.t_three (id INT PRIMARY KEY, descr VARCHAR(50))")

	for i := 1; i <= 20; i++ {
		mustExec(t, my.DB, "INSERT INTO shop.t_one VALUES (?, ?)", i, fmt.Sprintf("one-%d", i))
	}
	for i := 1; i <= 30; i++ {
		mustExec(t, my.DB, "INSERT INTO shop.t_two VALUES (?, ?)", i, fmt.Sprintf("two-%d", i))
	}
	for i := 1; i <= 40; i++ {
		mustExec(t, my.DB, "INSERT INTO shop.t_three VALUES (?, ?)", i, fmt.Sprintf("three-%d", i))
	}

	st := store.NewMem()
	e := testEnv(t, vars, st)
	ctx := context.Background()

	myJob := backup.Job{
		ID:           "bjob-my",
		Name:         "mysqldump",
		EverySeconds: 3600,
		Bucket:       "mysql-backups",
		JobOptions: backup.JobOptions{
			Method: backup.MethodMySQLDump,
			Source: "my-prod",
			Conn: backup.ConnSpec{
				Host:        "127.0.0.1",
				Port:        my.Port,
				User:        "root",
				PasswordEnv: "MYSQL_BACKUP_PW",
				Databases:   []string{"shop"},
			},
		},
	}
	rec, err := e.RunJob(ctx, myJob)
	if err != nil {
		t.Fatalf("RunJob mysqldump: %v", err)
	}
	if rec.Status != backup.StatusComplete {
		t.Fatalf("mysqldump status: %v", rec.Status)
	}

	plan := RestorePlan{Chain: []backup.Record{rec}}

	// Partial restore of table t_one into NEW database "shop_partial"
	partialTarget := RestoreTarget{
		Conn: backup.ConnSpec{
			Host:        "127.0.0.1",
			Port:        my.Port,
			User:        "root",
			PasswordEnv: "MYSQL_BACKUP_PW",
		},
		Database: "shop_partial",
		Objects:  []string{"t_one"},
	}
	if err := e.Restore(ctx, plan, partialTarget); err != nil {
		t.Fatalf("Restore partial mysql: %v", err)
	}

	// Verify only t_one exists
	var tableCount int
	if err := my.DB.QueryRow("SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'shop_partial' AND TABLE_TYPE = 'BASE TABLE'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 1 {
		t.Fatalf("expected 1 table in shop_partial, got %d", tableCount)
	}

	var rowCount int
	if err := my.DB.QueryRow("SELECT count(*) FROM shop_partial.t_one").Scan(&rowCount); err != nil || rowCount != 20 {
		t.Fatalf("t_one row count want 20, got %d (%v)", rowCount, err)
	}
}

func TestMongodumpPartialParallelRestore(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	tools := filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars := startRPPMinIO(t, dir, "mongo-backups")
	mo := startRPPMongo(t, dir)

	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["STRAND_TEST_MONGO_URI"] = fmt.Sprintf("mongodb://127.0.0.1:%d", mo.Port)
	for k, v := range vars {
		t.Setenv(k, v)
	}

	wrapper(t, tools, "mongodump", mo.Container, "mongodump")
	wrapper(t, tools, "mongorestore", mo.Container, "mongorestore")

	ctx := context.Background()

	// Seed source database "testdb" with collections coll_a and coll_b
	db := mo.Client.Database("testdb")
	for i := 1; i <= 25; i++ {
		_, err := db.Collection("coll_a").InsertOne(ctx, bson.M{"num": i, "name": fmt.Sprintf("item-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 35; i++ {
		_, err := db.Collection("coll_b").InsertOne(ctx, bson.M{"num": i, "name": fmt.Sprintf("other-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	st := store.NewMem()
	e := testEnv(t, vars, st)

	moJob := backup.Job{
		ID:           "bjob-mongo",
		Name:         "mongodump",
		EverySeconds: 3600,
		Bucket:       "mongo-backups",
		JobOptions: backup.JobOptions{
			Method: backup.MethodMongodump,
			Source: "mongo-prod",
			Conn: backup.ConnSpec{
				URIEnv:    "STRAND_TEST_MONGO_URI",
				Databases: []string{"testdb"},
			},
		},
	}
	rec, err := e.RunJob(ctx, moJob)
	if err != nil {
		t.Fatalf("RunJob mongodump: %v", err)
	}
	if rec.Status != backup.StatusComplete {
		t.Fatalf("mongodump status: %v", rec.Status)
	}

	plan := RestorePlan{Chain: []backup.Record{rec}}

	// Partial restore of coll_a with threads=4 into "testdb_restored"
	partialTarget := RestoreTarget{
		Conn: backup.ConnSpec{
			URIEnv: "STRAND_TEST_MONGO_URI",
		},
		Database: "testdb_restored",
		Threads:  4,
		Objects:  []string{"coll_a"},
	}
	if err := e.Restore(ctx, plan, partialTarget); err != nil {
		t.Fatalf("Restore mongo partial with threads=4: %v", err)
	}

	restoredDB := mo.Client.Database("testdb_restored")
	colls, err := restoredDB.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	var nonSystem []string
	for _, c := range colls {
		if !strings.HasPrefix(c, "system.") {
			nonSystem = append(nonSystem, c)
		}
	}
	if len(nonSystem) != 1 || nonSystem[0] != "coll_a" {
		t.Fatalf("expected only coll_a in testdb_restored, got: %v", nonSystem)
	}

	cnt, err := restoredDB.Collection("coll_a").CountDocuments(ctx, bson.M{})
	if err != nil || cnt != 25 {
		t.Fatalf("coll_a document count want 25, got %d (%v)", cnt, err)
	}
}

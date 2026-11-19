package runner

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func readPayload(t *testing.T, e *Env, rec backup.Record) []byte {
	t.Helper()
	repo, err := e.OpenRecordRepository(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := repo.OpenPayload(context.Background(), rec, e.Keys)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read payload %s: %v", rec.ID, err)
	}
	return b
}

// Built-in methods against real servers, streamed compressed+encrypted to MinIO, then restored with the
// native tools into NEW databases and compared; retention removes old objects; a failing tool fails
// the backup without leaving a complete record.
func TestBuiltinMethodsMySQLAndPostgres(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	tools := filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars := startMinIO(t, dir, "central-backups")
	my := startMySQL(t, dir, "mysql:8.4")
	pg := startPG(t, dir)
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	vars["STRAND_BACKUP_ENCRYPTION_KEYS"] = "k2026:" + key
	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["MYSQL_BACKUP_PW"] = "rootpw"
	vars["PG_BACKUP_PW"] = "pgpw"
	for k, v := range vars {
		t.Setenv(k, v) // wrappers read the forwarded variables from the real environment
	}
	wrapper(t, tools, "mysqldump", my.Container, "mysqldump", "MYSQL_PWD")
	wrapper(t, tools, "pg_dump", pg.Container, "pg_dump", "PGPASSWORD")
	wrapper(t, tools, "mysql", my.Container, "mysql", "MYSQL_PWD")
	wrapper(t, tools, "pg_basebackup", pg.Container, "pg_basebackup", "PGPASSWORD")

	mustExec(t, my.DB, "CREATE DATABASE shop")
	mustExec(t, my.DB, "CREATE TABLE shop.orders (id INT PRIMARY KEY, note VARCHAR(100))")
	for i := 1; i <= 200; i++ {
		mustExec(t, my.DB, "INSERT INTO shop.orders VALUES (?, ?)", i, fmt.Sprintf("order %d ünïcode", i))
	}
	mustExec(t, pg.DB, "CREATE DATABASE app")
	pgApp := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app?sslmode=disable", pg.Port))
	mustExec(t, pgApp, "CREATE TABLE items (id int primary key, payload text)")
	mustExec(t, pgApp, "INSERT INTO items SELECT g, repeat('x', 100) || g FROM generate_series(1, 5000) g")

	st := store.NewMem()
	e := testEnv(t, vars, st)
	ctx := context.Background()
	myJob := backup.Job{ID: "bjob-my", Name: "mysql", EverySeconds: 3600, Bucket: "central-backups", Prefix: "prod",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod", Compress: true, Encrypt: true,
			Conn:      backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root", PasswordEnv: "MYSQL_BACKUP_PW", Databases: []string{"shop"}},
			Retention: backup.RetentionPolicy{KeepLast: 1}}}
	if err := backup.ValidateJob(myJob); err != nil {
		t.Fatal(err)
	}
	rec, err := e.RunJob(ctx, myJob)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != backup.StatusComplete || rec.Format != backup.FormatSBK1 || rec.KeyID != "k2026" || rec.Compression != "zstd" ||
		rec.Position.GTIDSet == "" || rec.Position.BinlogFile == "" || rec.PlainSHA256 == "" || rec.PlainBytes <= rec.Bytes {
		t.Fatalf("mysqldump record = %+v", rec)
	}
	dump := readPayload(t, e, rec)
	if !bytes.Contains(dump, []byte("INSERT INTO `orders`")) || bytes.Contains(dump, []byte("CREATE DATABASE")) {
		t.Fatal("dump content unexpected")
	}
	// Restore into a NEW database with the native client and compare.
	mustExec(t, my.DB, "CREATE DATABASE shop_restored")
	cmd := exec.Command("docker", "exec", "-i", "-e", "MYSQL_PWD=rootpw", my.Container, "mysql", "-h127.0.0.1", fmt.Sprintf("-P%d", my.Port), "-uroot", "shop_restored")
	cmd.Stdin = bytes.NewReader(StripGTIDPurged(dump))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restore: %v %s", err, out)
	}
	var n int
	if err := my.DB.QueryRow("SELECT count(*) FROM shop_restored.orders WHERE note LIKE '%ünïcode'").Scan(&n); err != nil || n != 200 {
		t.Fatalf("restored rows %d %v", n, err)
	}
	// Ciphertext in S3 does not contain the plaintext.
	repo, _ := e.OpenRecordRepository(ctx, rec)
	raw, _, _ := repo.Store.Download(ctx, rec.ObjectKey)
	rawBytes, _ := io.ReadAll(raw)
	raw.Close()
	if bytes.Contains(rawBytes, []byte("orders")) {
		t.Fatal("stored object is not encrypted")
	}
	// Wrong key: clear error, nothing decoded.
	e2 := testEnv(t, map[string]string{"STRAND_BACKUP_ENCRYPTION_KEYS": "k2026:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32)),
		"STRAND_BACKUP_S3_ENDPOINT": vars["STRAND_BACKUP_S3_ENDPOINT"], "STRAND_BACKUP_S3_ACCESS_KEY": "minioadmin",
		"STRAND_BACKUP_S3_SECRET_KEY": "minioadmin", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}, st)
	if _, err := repo.OpenPayload(ctx, rec, e2.Keys); err == nil || !strings.Contains(err.Error(), "wrong encryption key") {
		t.Fatalf("wrong key: %v", err)
	}

	// Retention: two more runs, keep-last 1 expires the older two and deletes their objects.
	time.Sleep(10 * time.Millisecond)
	r2, err := e.RunJob(ctx, myJob)
	if err != nil {
		t.Fatal(err)
	}
	r3, err := e.RunJob(ctx, myJob)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := e.PruneJob(ctx, myJob, true)
	if err != nil || len(plan.Expired) != 0 || len(plan.Decisions) != 3 {
		t.Fatalf("dry run = %+v %v", plan, err)
	}
	if _, err := repo.Store.Head(ctx, rec.ObjectKey); err != nil {
		t.Fatal("dry run deleted an object")
	}
	res, err := e.PruneJob(ctx, myJob, false)
	if err != nil || len(res.Expired) != 2 || len(res.Errors) != 0 {
		t.Fatalf("prune = %+v %v", res, err)
	}
	for _, old := range []backup.Record{rec, r2} {
		if _, err := repo.Store.Head(ctx, old.ObjectKey); err == nil {
			t.Fatalf("expired object %s still exists", old.ID)
		}
		got, _, _ := e.Catalog.Get(ctx, old.ID)
		if got.Status != backup.StatusExpired || got.ExpiredAt == nil {
			t.Fatalf("catalog %s = %s", old.ID, got.Status)
		}
	}
	if _, err := repo.Store.Head(ctx, r3.ObjectKey); err != nil {
		t.Fatal("kept backup deleted")
	}

	// Automated test restore into a scratch database: rows counted and recorded, scratch dropped.
	myJob.TestRestore, myJob.Scratch = true, backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root", PasswordEnv: "MYSQL_BACKUP_PW"}
	tr, err := e.TestRestore(ctx, myJob)
	if err != nil || tr.Status != backup.CheckOK || tr.Metrics["rows"] != "200" || tr.Metrics["rows.orders"] != "200" {
		t.Fatalf("test restore %+v %v", tr, err)
	}
	var scratchLeft int
	_ = my.DB.QueryRow("SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'strand_verify_%'").Scan(&scratchLeft)
	if scratchLeft != 0 {
		t.Fatal("scratch database not dropped")
	}
	// Verification detects a payload replaced behind the catalog's back.
	if err := e.VerifyBackup(ctx, r3, true); err != nil {
		t.Fatalf("verify intact: %v", err)
	}
	if _, err := repo.Store.Upload(ctx, r3.ObjectKey, bytes.NewReader(make([]byte, r3.Bytes)), r3.Bytes, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.VerifyBackup(ctx, r3, true); err == nil {
		t.Fatal("tampered payload passed verification")
	}
	if cs, _ := e.Catalog.Checks(ctx, r3.ID, 5); len(cs) < 2 || cs[0].Status != backup.CheckFailed {
		t.Fatalf("checks %+v", cs)
	}

	// PostgreSQL logical + physical.
	pgJob := backup.Job{ID: "bjob-pgd", Name: "pgdump", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodPGDump, Source: "pg-prod", Compress: true,
			Conn: backup.ConnSpec{Host: "127.0.0.1", Port: pg.Port, User: "postgres", PasswordEnv: "PG_BACKUP_PW", Databases: []string{"app"}}}}
	prec, err := e.RunJob(ctx, pgJob)
	if err != nil {
		t.Fatal(err)
	}
	pdump := readPayload(t, e, prec)
	if !bytes.HasPrefix(pdump, []byte("PGDMP")) || prec.KeyID != "" || prec.Compression != "zstd" {
		t.Fatalf("pg_dump record %+v", prec)
	}
	mustExec(t, pg.DB, "CREATE DATABASE app_restored")
	wrapper(t, tools, "pg_restore", pg.Container, "pg_restore", "PGPASSWORD")
	cmd = exec.Command(filepath.Join(tools, "pg_restore"), "-h", "127.0.0.1", "-p", fmt.Sprint(pg.Port), "-U", "postgres", "-d", "app_restored", "--no-owner")
	cmd.Env = append(os.Environ(), "PGPASSWORD=pgpw")
	cmd.Stdin = bytes.NewReader(pdump)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pg_restore: %v %s", err, out)
	}
	restored := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app_restored?sslmode=disable", pg.Port))
	if err := restored.QueryRow("SELECT count(*) FROM items").Scan(&n); err != nil || n != 5000 {
		t.Fatalf("pg restored %d %v", n, err)
	}
	baseJob := pgJob
	baseJob.ID, baseJob.Method, baseJob.Conn.Databases, baseJob.Encrypt = "bjob-pgb", backup.MethodPGBaseBackup, nil, true
	brec, err := e.RunJob(ctx, baseJob)
	if err != nil {
		t.Fatal(err)
	}
	if brec.Position.LSN == "" || brec.EndPosition.LSN == "" || brec.Details["timeline"] != "1" || brec.KeyID != "k2026" {
		t.Fatalf("pg_basebackup record %+v", brec)
	}
	tarball := readPayload(t, e, brec)
	if !bytes.Contains(tarball, []byte("backup_label")) || !bytes.Contains(tarball, []byte("PG_VERSION")) {
		t.Fatal("base backup tar lacks backup_label/PG_VERSION")
	}

	// A failing tool (missing database; local connections use trust auth) fails the backup and leaves no complete record.
	bad := pgJob
	bad.ID, bad.Conn.PasswordEnv, bad.Conn.Databases = "bjob-bad", "PG_WRONG_PW", []string{"no_such_db"}
	vars["PG_WRONG_PW"] = "nope"
	t.Setenv("PG_WRONG_PW", "nope")
	if _, err := e.RunJob(ctx, bad); err == nil || strings.Contains(err.Error(), "nope") {
		t.Fatalf("wrong password run: %v", err)
	}
	failed, _ := e.Catalog.List(ctx, backup.Filter{JobID: "bjob-bad"})
	if len(failed) != 1 || failed[0].Status != backup.StatusFailed {
		t.Fatalf("failed run catalog = %+v", failed)
	}
}

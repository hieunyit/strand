package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func pitrSetup(t *testing.T) (dir, tools string, vars map[string]string) {
	requireDocker(t)
	dir = shared(t)
	tools = filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars = startMinIO(t, dir, "central-backups")
	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["STRAND_BACKUP_ENCRYPTION_KEY"] = "q7Yf1oGZ0L9bmmJ2y4mF6p4b4m2Cq9cJqZ0p6rS1f2s="
	return
}

func waitLogs(t *testing.T, e *Env, jobID string, until time.Time) []backup.Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		rs, _ := e.Catalog.List(context.Background(), backup.Filter{JobID: jobID, Kind: backup.KindLog, Status: backup.StatusComplete})
		for _, r := range rs {
			if r.EndTime != nil && !r.EndTime.Before(until) {
				return rs
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no archived log reaching %s (have %d)", until, len(rs))
		}
		time.Sleep(time.Second)
	}
}

// Point-in-time recovery, MySQL: base dump, binlogs archived continuously, writes before and after the
// target time; the restore into a NEW database contains exactly the rows committed before the target.
func TestPITRMySQL(t *testing.T) {
	dir, tools, vars := pitrSetup(t)
	my := startMySQL(t, dir, "mysql:8.4")
	vars["MYSQL_BACKUP_PW"] = "rootpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}
	wrapper(t, tools, "mysqldump", my.Container, "mysqldump", "MYSQL_PWD")
	wrapper(t, tools, "mysql", my.Container, "mysql", "MYSQL_PWD")
	binlogTool := "#!/bin/sh\nexec docker run --rm -i --network host -e TZ -v " + dir + ":" + dir + " percona/percona-xtrabackup:8.4 mysqlbinlog \"$@\"\n"
	if err := os.WriteFile(filepath.Join(tools, "mysqlbinlog"), []byte(binlogTool), 0o755); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my.DB, "CREATE DATABASE shop")
	mustExec(t, my.DB, "CREATE TABLE shop.t (id INT PRIMARY KEY, v VARCHAR(20))")
	insert := func(from, to int) {
		for i := from; i <= to; i++ {
			mustExec(t, my.DB, "INSERT INTO shop.t VALUES (?, ?)", i, fmt.Sprint("v", i))
		}
	}
	insert(1, 10)
	st := store.NewMem()
	e := testEnv(t, vars, st)
	job := backup.Job{ID: "bjob-mypitr", Name: "mysql pitr", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod", Compress: true, Encrypt: true, LogArchive: true,
			Conn: backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root", PasswordEnv: "MYSQL_BACKUP_PW", Databases: []string{"shop"}}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	archErr := make(chan error, 1)
	go func() { archErr <- e.ArchiveBinlog(ctx, job, filepath.Join(dir, "binlog-staging")) }()
	base, err := e.RunJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	insert(11, 20)
	target := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(target.Add(1100 * time.Millisecond)))
	insert(21, 30)
	mustExec(t, my.DB, "FLUSH BINARY LOGS")
	waitLogs(t, e, job.ID, target)
	all, _ := e.Catalog.ListAll(context.Background(), backup.Filter{Source: "mysql-prod", Status: backup.StatusComplete}, 0)
	w := RecoveryWindow(all)
	if !w.Continuous || w.End.Before(target) || w.Start.After(ConsistentTime(base)) {
		t.Fatalf("window %+v", w)
	}
	plan, err := PlanRestore(all, nil, &target)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Base().ID != base.ID || len(plan.Logs) == 0 {
		t.Fatalf("plan %+v", plan)
	}
	err = e.Restore(context.Background(), plan, RestoreTarget{Conn: backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root", PasswordEnv: "MYSQL_BACKUP_PW"}, Database: "shop_pitr"})
	if err != nil {
		t.Fatal(err)
	}
	var n, maxID int
	if err := my.DB.QueryRow("SELECT count(*), max(id) FROM shop_pitr.t").Scan(&n, &maxID); err != nil || n != 20 || maxID != 20 {
		t.Fatalf("restored to %s: %d rows (max id %d), want exactly 20: %v", target, n, maxID, err)
	}
	// The source is untouched and a second restore into the same name is refused.
	if err := my.DB.QueryRow("SELECT count(*) FROM shop.t").Scan(&n); err != nil || n != 30 {
		t.Fatalf("source rows %d", n)
	}
	if err := e.Restore(context.Background(), plan, RestoreTarget{Conn: backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root",
		PasswordEnv: "MYSQL_BACKUP_PW"}, Database: "shop_pitr"}); err == nil {
		t.Fatal("restore over an existing database accepted")
	}
	// Verification: payloads re-read, logs contiguous.
	rep, err := e.VerifyJob(context.Background(), job, VerifyOptions{})
	if err != nil || !rep.OK() || rep.FullReads < 2 {
		t.Fatalf("verify %+v %v", rep, err)
	}
	cancel()
	<-archErr
}

// Point-in-time recovery, PostgreSQL: pg_basebackup + WAL streamed by Strand's replication client;
// the restored data directory recovers to the target time and contains exactly the earlier rows.
func TestPITRPostgres(t *testing.T) {
	dir, tools, vars := pitrSetup(t)
	pg := startPG(t, dir)
	vars["PG_BACKUP_PW"] = "pgpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}
	wrapper(t, tools, "pg_basebackup", pg.Container, "pg_basebackup", "PGPASSWORD")
	mustExec(t, pg.DB, "CREATE TABLE items (id int primary key)")
	mustExec(t, pg.DB, "INSERT INTO items SELECT generate_series(1, 100)")
	st := store.NewMem()
	e := testEnv(t, vars, st)
	job := backup.Job{ID: "bjob-pgpitr", Name: "pg pitr", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodPGBaseBackup, Source: "pg-prod", Compress: true, Encrypt: true, LogArchive: true,
			Conn: backup.ConnSpec{Host: "127.0.0.1", Port: pg.Port, User: "postgres", PasswordEnv: "PG_BACKUP_PW"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	archErr := make(chan error, 1)
	go func() { archErr <- e.ArchiveWAL(ctx, job) }()
	time.Sleep(2 * time.Second)
	base, err := e.RunJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, pg.DB, "INSERT INTO items SELECT generate_series(101, 200)")
	time.Sleep(1100 * time.Millisecond)
	target := time.Now().UTC()
	time.Sleep(1100 * time.Millisecond)
	mustExec(t, pg.DB, "INSERT INTO items SELECT generate_series(201, 300)")
	mustExec(t, pg.DB, "SELECT pg_switch_wal()")
	waitLogs(t, e, job.ID, target.Add(time.Second))
	all, _ := e.Catalog.ListAll(context.Background(), backup.Filter{Source: "pg-prod", Status: backup.StatusComplete}, 0)
	if gaps := LogGaps(all); len(gaps) != 0 {
		t.Fatalf("gaps %v", gaps)
	}
	plan, err := PlanRestore(all, nil, &target)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Base().ID != base.ID {
		t.Fatalf("base %s", plan.Base().ID)
	}
	data := filepath.Join(dir, "restored-pg")
	if err := e.Restore(context.Background(), plan, RestoreTarget{Dir: data}); err != nil {
		t.Fatal(err)
	}
	if err := e.Restore(context.Background(), plan, RestoreTarget{Dir: data}); err == nil {
		t.Fatal("restore into a non-empty directory accepted")
	}
	port := freePort(t)
	name := startContainer(t, "pgrestore", dir, "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", data+":/pgdata",
		"-e", "PGDATA=/pgdata", "postgres:17", "postgres", "-D", "/pgdata", "-p", fmt.Sprint(port), "-c", "listen_addresses=127.0.0.1")
	_ = name
	db := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
	deadline := time.Now().Add(time.Minute)
	for {
		var inRecovery bool
		if err := db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery); err == nil && !inRecovery {
			break
		}
		if time.Now().After(deadline) {
			out, _ := exec.Command("docker", "logs", "--tail", "30", name).CombinedOutput()
			t.Fatalf("restored server did not promote:\n%s", out)
		}
		time.Sleep(time.Second)
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM items").Scan(&n); err != nil || n != 200 {
		t.Fatalf("restored rows %d (want 200): %v", n, err)
	}
	cancel()
	if err := <-archErr; err != nil {
		t.Fatalf("archiver: %v", err)
	}
}

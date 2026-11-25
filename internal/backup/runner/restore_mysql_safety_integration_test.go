package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// sleepPastSecond returns the next whole second after now and sleeps until just after it, so statements
// run before the call are strictly before the returned time and statements run after it are not.
func sleepPastSecond() time.Time {
	at := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(at.Add(1100 * time.Millisecond)))
	return at
}

// A MySQL restore into a new database next to the source (same server, another name):
//   - the dump load is binlogged like the rest of the restore, so replicas of the target see the data;
//   - point-in-time replay applies unqualified DDL to the new database, but stops (and creates nothing)
//     when a binlog statement names the source database explicitly, which --rewrite-db does not rename,
//     instead of running it against the source; and when a statement changed the source with another
//     default database, which mysqlbinlog --database would silently leave out;
//   - a test restore drops only the scratch database it created.
func TestMySQLRestoreStaysInsideTheTarget(t *testing.T) {
	dir, tools, vars := pitrSetup(t)
	my := startMySQL(t, dir, "mysql:8.4")
	vars["MYSQL_BACKUP_PW"] = "rootpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}
	wrapper(t, tools, "mysqldump", my.Container, "mysqldump", "MYSQL_PWD")
	wrapper(t, tools, "mysql", my.Container, "mysql", "MYSQL_PWD")
	binlogTool := fmt.Sprintf("#!/bin/sh\nexec docker run --rm -i --name %s-mysqlbinlog-$$-$(date +%%s%%N) --network host --user %d:%d -e TZ -v %s:%s --entrypoint mysqlbinlog percona/percona-xtrabackup:8.4 \"$@\"\n",
		containerPrefix("bkp"), os.Getuid(), os.Getgid(), dir, dir)
	if err := os.WriteFile(filepath.Join(tools, "mysqlbinlog"), []byte(binlogTool), 0o755); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my.DB, "CREATE DATABASE shop")
	mustExec(t, my.DB, "CREATE DATABASE other")
	mustExec(t, my.DB, "CREATE TABLE shop.orders (id INT PRIMARY KEY, v VARCHAR(20))")
	for i := 1; i <= 10; i++ {
		mustExec(t, my.DB, "INSERT INTO shop.orders VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	dsn := func(db string) string {
		return fmt.Sprintf("root:rootpw@tcp(127.0.0.1:%d)/%s?parseTime=true", my.Port, db)
	}
	shopDB := waitSQL(t, "mysql", dsn("shop"))
	shopDB.SetMaxOpenConns(1)
	otherDB := waitSQL(t, "mysql", dsn("other"))
	otherDB.SetMaxOpenConns(1)

	e := testEnv(t, vars, store.NewMem())
	conn := backup.ConnSpec{Host: "127.0.0.1", Port: my.Port, User: "root", PasswordEnv: "MYSQL_BACKUP_PW"}
	job := backup.Job{ID: "bjob-myguard", Name: "mysql guard", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-guard", Compress: true, Encrypt: true, LogArchive: true,
			Conn: conn, Scratch: conn}}
	job.Conn.Databases = []string{"shop"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	archErr := make(chan error, 1)
	go func() { archErr <- e.ArchiveBinlog(ctx, job, filepath.Join(dir, "binlog-staging")) }()
	base, err := e.RunJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(base.EndTime.Truncate(time.Second).Add(1100 * time.Millisecond)))

	// 1. Unqualified DDL and rows, run with the source as default database: replayed into the new name.
	mustExec(t, shopDB, "ALTER TABLE orders ADD COLUMN note VARCHAR(10)")
	for i := 11; i <= 15; i++ {
		mustExec(t, shopDB, "INSERT INTO orders VALUES (?, ?, 'n')", i, fmt.Sprint("v", i))
	}
	t1 := sleepPastSecond()
	// 2. The source named explicitly, then undone in the source after the target time.
	mustExec(t, shopDB, "ALTER TABLE shop.orders ADD COLUMN c2 INT")
	t2 := sleepPastSecond()
	mustExec(t, shopDB, "ALTER TABLE orders DROP COLUMN c2")
	// 3. After a newer base backup (so that the replay range does not include 2), the source changed
	// while another database was the default one.
	base2, err := e.RunJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(base2.EndTime.Truncate(time.Second).Add(1100 * time.Millisecond)))
	mustExec(t, otherDB, "CREATE TABLE shop.drift (id INT)")
	t3 := sleepPastSecond()
	mustExec(t, my.DB, "FLUSH BINARY LOGS")
	waitLogs(t, e, job.ID, t3.Add(time.Second))
	all, err := e.Catalog.ListAll(ctx, backup.Filter{Source: job.Source, Status: backup.StatusComplete}, 0)
	if err != nil {
		t.Fatal(err)
	}
	restoreTo := func(at time.Time, name string, want backup.Record) error {
		t.Helper()
		plan, err := PlanRestore(all, nil, &at)
		if err != nil {
			t.Fatalf("plan %s: %v", at, err)
		}
		if plan.Base().ID != want.ID || len(plan.Logs) == 0 {
			t.Fatalf("plan %+v", plan)
		}
		return e.Restore(context.Background(), plan, RestoreTarget{Conn: conn, Database: name})
	}
	schemaExists := func(name string) bool {
		var n int
		if err := my.DB.QueryRow("SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	columnExists := func(db, table, column string) bool {
		var n int
		if err := my.DB.QueryRow("SELECT count(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
			db, table, column).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}

	err = restoreTo(t2, "shop_r2", base)
	if columnExists("shop", "orders", "c2") {
		t.Fatalf("the replay changed the source: shop.orders gained column c2 again (restore error: %v)", err)
	}
	if err == nil || !strings.Contains(err.Error(), "names database `shop` explicitly") || !strings.Contains(err.Error(), "separate server under the same database name") {
		t.Fatalf("restore past a statement naming the source: want a clear refusal, got %v", err)
	}
	if schemaExists("shop_r2") {
		t.Fatal("refused restore left its database")
	}

	err = restoreTo(t3, "shop_r3", base2)
	if err == nil || !strings.Contains(err.Error(), "while its default database was `other`") {
		t.Fatalf("restore past a source change made from another default database: want a drift refusal, got %v", err)
	}
	if schemaExists("shop_r3") {
		t.Fatal("refused restore left its database")
	}

	if err := restoreTo(t1, "shop_r1", base); err != nil {
		t.Fatal(err)
	}
	var n, notes int
	if err := my.DB.QueryRow("SELECT count(*), count(note) FROM shop_r1.orders").Scan(&n, &notes); err != nil || n != 15 || notes != 5 {
		t.Fatalf("restored rows %d (notes %d), want 15 (5): %v", n, notes, err)
	}
	if err := my.DB.QueryRow("SELECT count(*) FROM shop.orders").Scan(&n); err != nil || n != 15 || columnExists("shop", "orders", "c2") {
		t.Fatalf("source changed: %d rows, %v", n, err)
	}

	// The dump load of a plain restore is binlogged (replicas of the target receive the rows).
	if err := e.Restore(context.Background(), RestorePlan{Chain: []backup.Record{base2}}, RestoreTarget{Conn: conn, Database: "shop_plain"}); err != nil {
		t.Fatal(err)
	}
	if !binlogHasTableMap(t, my, "shop_plain", "orders") {
		t.Fatal("the dump load of the restore is not in the binary log: replicas of the target would miss the restored rows")
	}

	// A test restore never drops a database it did not create.
	mustExec(t, my.DB, "CREATE DATABASE strand_verify_taken")
	mustExec(t, my.DB, "CREATE TABLE strand_verify_taken.keep (id INT)")
	if _, err := e.testRestore(context.Background(), job, base, "strand_verify_taken"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("test restore into an existing database: %v", err)
	}
	if !schemaExists("strand_verify_taken") {
		t.Fatal("a test restore dropped a scratch database another run created")
	}
	res, err := e.TestRestore(context.Background(), job)
	if err != nil || res.Status != backup.CheckOK || res.Metrics["rows"] != "15" {
		t.Fatalf("test restore: %+v %v", res, err)
	}
	if !regexp.MustCompile(`^strand_verify_\d+_[0-9a-f]{12}$`).MatchString(res.Metrics["scratch_database"]) {
		t.Fatalf("scratch database name %q has no random suffix", res.Metrics["scratch_database"])
	}
	if schemaExists(res.Metrics["scratch_database"]) {
		t.Fatal("test restore left its scratch database")
	}
	cancel()
	<-archErr
}

// binlogHasTableMap reports whether a row event for db.table is in any binary log of the server.
func binlogHasTableMap(t *testing.T, my mysqlFixture, db, table string) bool {
	t.Helper()
	rows, err := my.DB.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		files = append(files, fmt.Sprintf("%s", vals[0]))
	}
	rows.Close()
	want := fmt.Sprintf("(%s.%s)", db, table)
	for _, f := range files {
		ev, err := my.DB.Query("SHOW BINLOG EVENTS IN '" + f + "'")
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := ev.Columns()
		for ev.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := ev.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%s", vals[2]) == "Table_map" && strings.Contains(fmt.Sprintf("%s", vals[5]), want) {
				ev.Close()
				return true
			}
		}
		ev.Close()
	}
	return false
}

package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"strand/internal/testutil"

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
	wrapper(t, tools, "mysqldump-inner", my.Container, "mysqldump", "MYSQL_PWD")
	dumpReady := filepath.Join(tools, "dump-ready")
	dumpRelease := filepath.Join(tools, "dump-release")
	dumpTool := fmt.Sprintf(`#!/bin/sh
if [ "$1" != --version ]; then
  touch "%s"
  until [ -f "%s" ]; do sleep 0.1; done
fi
exec "%s" "$@"
`, dumpReady, dumpRelease, filepath.Join(tools, "mysqldump-inner"))
	if err := os.WriteFile(filepath.Join(tools, "mysqldump"), []byte(dumpTool), 0o755); err != nil {
		t.Fatal(err)
	}
	wrapper(t, tools, "mysql", my.Container, "mysql", "MYSQL_PWD")
	binlogTool := "#!/bin/sh\nexec docker run --rm -i --network host --user " + fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()) + " -e TZ -v " + dir + ":" + dir + " --entrypoint mysqlbinlog percona/percona-xtrabackup:8.4 \"$@\"\n"
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
	keys := e.Keys
	e.Keys = nil
	missingCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	_, backupErr := e.RunJob(missingCtx, job)
	archiveErr := e.ArchiveBinlog(missingCtx, job, filepath.Join(dir, "binlog-staging"))
	stop()
	e.Keys = keys
	for _, err := range []error{backupErr, archiveErr} {
		if err == nil || !strings.Contains(err.Error(), "requires encryption but no key") {
			t.Fatalf("missing key must refuse to create either kind of payload: %v", err)
		}
	}
	if records, err := e.Catalog.ListAll(ctx, backup.Filter{Source: job.Source}, 0); err != nil || len(records) != 0 {
		t.Fatalf("missing key left a catalog record: records=%v err=%v", records, err)
	}
	archErr := make(chan error, 1)
	go func() { archErr <- e.ArchiveBinlog(ctx, job, filepath.Join(dir, "binlog-staging")) }()

	// Commit rows after the first requested time but before mysqldump takes its snapshot.
	dumpCtx, cancelDump := context.WithTimeout(ctx, time.Minute)
	var base backup.Record
	var dumpErr error
	dumpDone := make(chan struct{})
	go func() {
		defer close(dumpDone)
		base, dumpErr = e.RunJob(dumpCtx, job)
	}()
	defer func() {
		cancelDump()
		<-dumpDone
	}()
	for {
		if _, err := os.Stat(dumpReady); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-dumpDone:
			t.Fatalf("backup finished before reaching the snapshot gate: %v", dumpErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
	beforeSnapshot := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(beforeSnapshot.Add(time.Millisecond)))
	insert(11, 20)
	if err := os.WriteFile(dumpRelease, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	<-dumpDone
	if dumpErr != nil {
		t.Fatal(dumpErr)
	}
	if base.StartTime == nil || base.EndTime == nil || !base.StartTime.Before(beforeSnapshot) || !base.EndTime.After(beforeSnapshot) {
		t.Fatalf("test target %s must fall between dump start %v and end %v", beforeSnapshot, base.StartTime, base.EndTime)
	}

	target := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(target.Add(1100 * time.Millisecond)))
	insert(21, 30)
	mustExec(t, my.DB, "FLUSH BINARY LOGS")
	waitLogs(t, e, job.ID, target)
	all, err := e.Catalog.ListAll(ctx, backup.Filter{Source: "mysql-prod", Status: backup.StatusComplete}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range all {
		if record.KeyID != base.KeyID || record.Format != backup.FormatSBK1 {
			t.Fatalf("full backup and logs should use the configured key: %+v", record)
		}
	}
	if plan, err := PlanRestore(all, nil, &beforeSnapshot); err == nil {
		t.Fatalf("planned restore to %s with a dump that already includes later writes: %+v", beforeSnapshot, plan)
	}
	w := RecoveryWindow(all)
	if !w.Continuous || w.End.Before(target) || !w.Start.Equal(base.EndTime.Truncate(time.Second).Add(time.Second)) {
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
		for _, r := range all {
			t.Logf("archive position %s: start=%+v end=%+v details=%v", r.ID, r.Position, r.EndPosition, r.Details)
		}
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
	missingWAL := plan
	missingWAL.Logs = append([]backup.Record(nil), plan.Logs...)
	missingWAL.Logs[0].ObjectKey += "-not-present"
	missingWAL.Logs[0].VersionID = ""
	missingDir := filepath.Join(dir, "missing-wal")
	if err := e.Restore(t.Context(), missingWAL, RestoreTarget{Dir: missingDir}); err == nil {
		t.Fatal("restore with an unavailable WAL object succeeded")
	}
	if _, err := os.Stat(missingDir); !os.IsNotExist(err) {
		t.Fatalf("missing WAL left a published recovery directory: %v", err)
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
	// A restored primary holds the same system ID on a new timeline. An
	// archiver using the old job must not attach an old chain or reserve WAL.
	mustExec(t, db, "CHECKPOINT")
	var timeline string
	if err := db.QueryRow("SELECT timeline_id::text FROM pg_control_checkpoint()").Scan(&timeline); err != nil || timeline == base.Details["timeline"] {
		t.Fatalf("fixture did not change PostgreSQL timeline: %s, %v", timeline, err)
	}
	forkJob := job
	forkJob.Conn.Port = port
	forkCtx, stopFork := context.WithTimeout(t.Context(), 10*time.Second)
	forkErr := e.ArchiveWAL(forkCtx, forkJob)
	stopFork()
	if forkErr == nil || !strings.Contains(forkErr.Error(), "prior log does not prove the current PostgreSQL system and timeline") {
		t.Fatalf("archiving continued across divergent WAL timelines: %v", forkErr)
	}
	if err := db.QueryRow("SELECT count(*) FROM pg_replication_slots").Scan(&n); err != nil || n != 0 {
		t.Fatalf("rejecting the timeline left %d replication slots: %v", n, err)
	}
	// WAL delivery time cannot certify recovery time. Prepare the same
	// available WAL for a later point, then require PostgreSQL itself to
	// reject the target since there is no committed WAL covering it.
	unreachable := target.Add(time.Hour)
	provisional, err := PlanRestore(all, &base, &unreachable)
	if err != nil || !provisional.VerifyOnReplay {
		t.Fatalf("expected preparation to require PostgreSQL replay verification: %v", err)
	}
	unreachedDir := filepath.Join(dir, "pg-unreached")
	if err := e.Restore(t.Context(), provisional, RestoreTarget{Dir: unreachedDir}); err != nil {
		t.Fatal(err)
	}
	verifyCtx, stop := context.WithTimeout(t.Context(), 25*time.Second)
	defer stop()
	name = fmt.Sprintf("bkp-pg-unreached-%d", os.Getpid())
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	out, err := exec.CommandContext(verifyCtx, "docker", "run", "--rm", "--name", name,
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", unreachedDir+":/pgdata", "-e", "PGDATA=/pgdata", "postgres:17",
		"postgres", "-D", "/pgdata", "-c", "listen_addresses=").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "recovery ended before configured recovery target was reached") {
		t.Fatalf("PostgreSQL did not reject an unreached PITR target: %v\n%s", err, out)
	}

}

func TestPITRMongo(t *testing.T) {
	dir, tools, vars := pitrSetup(t)
	source := testutil.MongoReplicaSet(t)
	target := testutil.MongoReplicaSet(t)
	for _, tool := range []string{"mongodump", "mongorestore"} {
		script := fmt.Sprintf("#!/bin/sh\nexec docker run --rm -i --network host -v %q --entrypoint %s mongo:8.0 \"$@\"\n", dir+":"+dir, tool)
		if err := os.WriteFile(filepath.Join(tools, tool), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	vars["MONGO_SRC_URI"], vars["MONGO_DST_URI"] = source.URI, target.URI
	env := testEnv(t, vars, store.NewMem())
	src, err := connectBackupMongo(source.URI, backup.ConnSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Disconnect(context.Background())
	dst, err := connectBackupMongo(target.URI, backup.ConnSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Disconnect(context.Background())
	ctx := t.Context()
	items := src.Database("app").Collection("items")
	if _, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	job := backup.Job{ID: "bjob-mongopitr", Name: "Mongo PITR", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMongodump, Source: "mongo-prod", Compress: true, Encrypt: true,
			LogArchive: true, LogFlushSeconds: 1, Conn: backup.ConnSpec{URIEnv: "MONGO_SRC_URI"}}}
	archiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.ArchiveOplog(archiveCtx, job) }()
	base, err := env.RunJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if !pitrBase(base) {
		t.Fatalf("missing embedded oplog boundary: %+v", base.EndPosition)
	}
	if _, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: 2}}); err != nil {
		t.Fatal(err)
	}
	at, err := latestOplogTS(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	waitLogs(t, env, job.ID, time.Unix(int64(at.T), 0))
	// Restart the archiver at the committed end of the first slice.
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	archiveCtx, cancel = context.WithCancel(ctx)
	defer cancel()
	go func() { done <- env.ArchiveOplog(archiveCtx, job) }()
	if err := src.Database("app").CreateCollection(ctx, "added"); err != nil {
		t.Fatal(err)
	}
	session, err := src.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(ctx)
	if _, err := session.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if _, err := items.InsertOne(tx, bson.D{{Key: "_id", Value: 3}}); err != nil {
			return nil, err
		}
		return src.Database("app").Collection("added").InsertOne(tx, bson.D{{Key: "_id", Value: 10}})
	}); err != nil {
		t.Fatal(err)
	}
	at, err = latestOplogTS(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Unix(int64(at.T)+1, 0).UTC()
	time.Sleep(time.Until(cutoff.Add(100 * time.Millisecond)))
	if _, err := items.InsertOne(ctx, bson.D{{Key: "_id", Value: 4}}); err != nil {
		t.Fatal(err)
	}
	waitLogs(t, env, job.ID, cutoff)
	records, err := env.Catalog.ListAll(ctx, backup.Filter{JobID: job.ID, Status: backup.StatusComplete}, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanRestore(records, &base, &cutoff)
	if err != nil || len(plan.Logs) < 2 {
		t.Fatalf("expected multiple covered oplog slices, got %d: %v", len(plan.Logs), err)
	}
	// A later archive segment under the same human source label cannot fill
	// a missing committed segment or replace the source replica set.
	onlyLater := append([]backup.Record{base}, plan.Logs[1:]...)
	if _, err := PlanRestore(onlyLater, &base, &cutoff); err == nil {
		t.Fatal("accepted MongoDB oplog chain missing the base boundary")
	}
	if err := env.Restore(ctx, plan, RestoreTarget{Conn: backup.ConnSpec{URIEnv: "MONGO_DST_URI"}, Deployment: true}); err != nil {
		t.Fatal(err)
	}
	for collection, want := range map[string]int64{"items": 3, "added": 1} {
		got, err := dst.Database("app").Collection(collection).CountDocuments(ctx, bson.D{})
		if err != nil || got != want {
			t.Fatalf("%s count = %d, want %d: %v", collection, got, want, err)
		}
	}
	if err := dst.Database("app").Collection("items").FindOne(ctx, bson.D{{Key: "_id", Value: 4}}).Err(); err != mongo.ErrNoDocuments {
		t.Fatalf("restore included a later write: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

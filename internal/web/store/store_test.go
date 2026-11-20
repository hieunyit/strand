package store_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/testutil"
	"strand/internal/web/store"
	"strand/internal/web/store/storetest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.TerminatePostgres()
	os.Exit(code)
}

func TestMemContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return store.NewMem() })
}

func TestPGContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		p, err := store.Open(context.Background(), testutil.NewPostgresDSN(t), store.OpenOptions{MaxWait: 10 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.Close() })
		return p
	})
}

func TestMigrateIdempotent(t *testing.T) {
	ctx := context.Background()
	p, err := store.Open(ctx, testutil.NewPostgresDSN(t), store.OpenOptions{NoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ms, err := store.Migrations()
	if err != nil || len(ms) < 2 {
		t.Fatalf("migrations = %v, %v", ms, err)
	}
	first, err := store.Migrate(ctx, p.DB())
	if err != nil || len(first) != len(ms) {
		t.Fatalf("first migrate applied %v err %v, want %d", first, err, len(ms))
	}
	again, err := store.Migrate(ctx, p.DB())
	if err != nil || len(again) != 0 {
		t.Fatalf("second migrate applied %v err %v, want none", again, err)
	}
	var n int
	if err := p.DB().QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil || n != len(ms) {
		t.Fatalf("schema_migrations rows = %d err %v", n, err)
	}
}

// Hai (hoặc nhiều) web khởi động cùng lúc trên database trống: advisory lock tuần tự hóa, mỗi migration
// được áp dụng đúng một lần và không instance nào lỗi (không có lock: "relation already exists").
func TestMigrateConcurrentLock(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewPostgresDSN(t)
	ms, _ := store.Migrations()
	const N = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	errs := make([]error, N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p, err := store.Open(ctx, dsn, store.OpenOptions{NoMigrate: true})
			if err != nil {
				errs[i] = err
				return
			}
			defer p.Close()
			applied, err := store.Migrate(ctx, p.DB())
			errs[i] = err
			mu.Lock()
			total += len(applied)
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d: %v", i, err)
		}
	}
	if total != len(ms) {
		t.Fatalf("migrations applied %d times in total across %d instances, want exactly %d", total, N, len(ms))
	}
}

func TestAuditAppendOnly(t *testing.T) {
	ctx := context.Background()
	p, err := store.Open(ctx, testutil.NewPostgresDSN(t), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.AppendAudit(ctx, store.AuditRow{Time: time.Now(), Action: "login", Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`UPDATE audit_log SET action = 'x'`, `DELETE FROM audit_log`, `TRUNCATE audit_log`} {
		if _, err := p.DB().Exec(q); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: err = %v, want append-only rejection", q, err)
		}
	}
}

// The archive path does not open a hole in append-only: a DELETE outside strand_audit_archive is rejected, also
// with the session flag the trigger checks set by hand (no manifest row in the same transaction), the
// audit.archive entries and the audit_archives manifest are immutable, and the function deletes all or nothing.
func TestAuditArchiveKeepsAppendOnly(t *testing.T) {
	ctx := context.Background()
	p, err := store.Open(ctx, testutil.NewPostgresDSN(t), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	old := time.Now().Add(-48 * time.Hour)
	for i := 0; i < 3; i++ {
		if err := p.AppendAudit(ctx, store.AuditRow{Time: old, Action: "start", Data: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	db := p.DB()
	reject := func(what string, f func(*sql.Tx) error) {
		t.Helper()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback() //nolint:errcheck
		if err := f(tx); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: err = %v, want append-only rejection", what, err)
		}
	}
	reject("plain delete", func(tx *sql.Tx) error { _, err := tx.Exec(`DELETE FROM audit_log`); return err })
	reject("delete with a forged flag", func(tx *sql.Tx) error {
		if _, err := tx.Exec(`SELECT set_config('strand.audit_archive_nonce', 'forged', true)`); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM audit_log`)
		return err
	})
	pl, _ := p.PlanAuditArchive(ctx, time.Now().Add(-24*time.Hour))
	if n, err := p.ArchiveAudit(ctx, store.AuditArchive{Before: pl.Before, MaxID: pl.MaxID, Count: pl.Count, File: "f.jsonl.gz", SHA256: strings.Repeat("c", 64)}); err != nil || n != 3 {
		t.Fatalf("archive: %d %v", n, err)
	}
	reject("delete of the audit.archive entry", func(tx *sql.Tx) error { _, err := tx.Exec(`DELETE FROM audit_log`); return err })
	reject("update of the manifest", func(tx *sql.Tx) error { _, err := tx.Exec(`UPDATE audit_archives SET row_count = 0`); return err })
	reject("delete of the manifest", func(tx *sql.Tx) error { _, err := tx.Exec(`DELETE FROM audit_archives`); return err })
	reject("truncate of the manifest", func(tx *sql.Tx) error { _, err := tx.Exec(`TRUNCATE audit_archives`); return err })
	reject("truncate audit_log", func(tx *sql.Tx) error { _, err := tx.Exec(`TRUNCATE audit_log`); return err })
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM audit_log WHERE action = 'audit.archive'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("audit.archive entries = %d %v", n, err)
	}
}

func TestOpenRetriesAndRedacts(t *testing.T) {
	// Cổng không có ai nghe: Open phải thử lại tới MaxWait rồi lỗi, và lỗi không chứa mật khẩu.
	dsn := "postgres://strand:Sup3rSecretPw@127.0.0.1:1/strand?sslmode=disable"
	t0 := time.Now()
	_, err := store.Open(context.Background(), dsn, store.OpenOptions{MaxWait: 2 * time.Second})
	if err == nil {
		t.Fatal("expected connection error")
	}
	if strings.Contains(err.Error(), "Sup3rSecretPw") {
		t.Fatalf("error leaks the password: %v", err)
	}
	if time.Since(t0) < time.Second {
		t.Fatalf("gave up after %v: no retry/backoff", time.Since(t0))
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"postgres://u:p%40ss@h:5432/db?sslmode=disable": "postgres://u:xxxxx@h:5432/db?sslmode=disable",
		"host=h user=u password=sekret dbname=d":        "host=h user=u password=xxxxx dbname=d",
		"host=h password='se kret' dbname=d":            "host=h password=xxxxx dbname=d",
		"no secret here":                                "no secret here",
	} {
		if got := store.Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSecretBox(t *testing.T) {
	if _, err := store.NewBox("short"); err == nil {
		t.Fatal("short key accepted")
	}
	b, err := store.NewBox("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	ct, err := b.Seal([]byte("SRC_PW=hunter2\n"), "orders")
	if err != nil || strings.Contains(string(ct), "hunter2") {
		t.Fatalf("seal: %v", err)
	}
	if pt, err := b.Open(ct, "orders"); err != nil || string(pt) != "SRC_PW=hunter2\n" {
		t.Fatalf("open: %q %v", pt, err)
	}
	if _, err := b.Open(ct, "other"); err == nil {
		t.Fatal("ciphertext opened under another pipeline name")
	}
	b2, _ := store.NewBox("ffffffffffffffffffffffffffffffff")
	if _, err := b2.Open(ct, "orders"); err == nil {
		t.Fatal("ciphertext opened under another key")
	}
}

// A database migrated by the feat/web-db-first WIP build has version 010 = pipeline_config_json. Version-only
// bookkeeping skipped main's 010_backups forever (then 011 failed on the missing table); Migrate must replace
// the stray migration instead, and must refuse unknown collisions rather than skip them.
func TestMigrateReplacesSupersededWIPMigration(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewPostgresDSN(t)
	p, err := store.Open(ctx, dsn, store.OpenOptions{NoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ms, err := store.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	db := p.DB()
	if _, err := db.Exec(`CREATE TABLE schema_migrations (version int PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, m := range ms[:9] {
		if _, err := db.Exec(m.SQL); err != nil {
			t.Fatalf("%03d: %v", m.Version, err)
		}
		if _, err := db.Exec(`INSERT INTO schema_migrations (version, name) VALUES ($1,$2)`, m.Version, m.Name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`ALTER TABLE pipelines ADD COLUMN config_json jsonb;
		CREATE INDEX pipelines_config_json_present_idx ON pipelines (name) WHERE config_json IS NOT NULL;
		INSERT INTO schema_migrations (version, name) VALUES (10, 'pipeline_config_json')`); err != nil {
		t.Fatal(err)
	}
	got, err := store.Migrate(ctx, db)
	if err != nil {
		t.Fatalf("migrate WIP database: %v", err)
	}
	if len(got) != len(ms)-9 {
		t.Fatalf("applied %v, want versions 010..%03d", got, len(ms))
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM schema_migrations WHERE version = 10`).Scan(&name); err != nil || name != "backups" {
		t.Fatalf("version 10 = %q, %v", name, err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass('backups') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatalf("backups table missing after heal: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) > 0 FROM information_schema.columns WHERE table_name='pipelines' AND column_name='config_json'`).Scan(&exists); err != nil || exists {
		t.Fatalf("stray config_json column kept: %v", err)
	}
	// Unknown collision: refuse with a clear error instead of skipping.
	if _, err := db.Exec(`UPDATE schema_migrations SET name = 'something_else' WHERE version = 11`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Migrate(ctx, db); err == nil || !strings.Contains(err.Error(), "different build") {
		t.Fatalf("unknown collision: err = %v", err)
	}
}

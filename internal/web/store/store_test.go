package store_test

import (
	"context"
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

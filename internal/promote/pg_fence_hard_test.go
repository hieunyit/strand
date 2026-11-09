package promote_test

import (
	"context"
	"database/sql"
	"testing"

	"strand/internal/promote"
)

// invariant: PR-1 — review R06. Fence source PG với role ứng dụng đã khai báo phải là hàng rào cứng:
// session đang mở của role bị ngắt, và role không đăng nhập lại được để tự tắt read-only rồi ghi.
func TestPR1_PGSourceFenceBlocksDeclaredAppRoleFromWriting(t *testing.T) {
	s := sourcePG(t)
	db, _ := newTestDB(t, s)
	ctx := context.Background()
	var dbName string
	if err := db.QueryRow("SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"DROP ROLE IF EXISTS r06_app",
		"CREATE ROLE r06_app LOGIN PASSWORD 'pw'",
	} {
		if err := s.adminExec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = s.adminExec(context.Background(), "ALTER ROLE r06_app LOGIN") })
	for _, q := range []string{
		"CREATE TABLE public.r06 (id int PRIMARY KEY)",
		"GRANT INSERT, SELECT ON public.r06 TO r06_app",
		"GRANT CONNECT ON DATABASE " + dbName + " TO r06_app",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	app, err := sql.Open("pgx", s.dsn("r06_app", "pw", dbName))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	app.SetMaxOpenConns(1)
	if _, err := app.ExecContext(ctx, "INSERT INTO public.r06 VALUES (1)"); err != nil {
		t.Fatalf("app write before fence: %v", err)
	}

	unfenceOnCleanup(t, s)
	if err := promote.NewPGSourceDriver(db, "r06_app").Fence(ctx); err != nil {
		t.Fatal(err)
	}
	// Session quản trị dùng SET off phải bị loại khỏi pool; session mới vẫn thấy fence cluster.
	var readOnly string
	if err := db.QueryRowContext(ctx, "SHOW default_transaction_read_only").Scan(&readOnly); err != nil || readOnly != "on" {
		t.Fatalf("fence did not reach a fresh pooled session: %q (%v)", readOnly, err)
	}
	// Ứng dụng thử lại như thật: kết nối (mới) rồi tắt read-only trong phiên và ghi.
	for i := 0; i < 3; i++ {
		_, _ = app.ExecContext(ctx, "SET default_transaction_read_only = off")
		if _, err := app.ExecContext(ctx, "INSERT INTO public.r06 VALUES ($1)", 10+i); err == nil {
			t.Fatalf("declared app role wrote to the fenced source (attempt %d)", i)
		}
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM public.r06").Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows on fenced source = %d (%v), want 1", n, err)
	}
}

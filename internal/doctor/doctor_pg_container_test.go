package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/findings"
	"strand/internal/testutil"
	"strand/internal/validate"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.TerminatePostgres()
	os.Exit(code)
}

// PG → PG: wal_level không phải logical, chưa có publication, bảng không có khóa. validate dừng ở lỗi đầu tiên
// (wal_level); doctor báo cả ba cùng lúc.
func TestDoctor_Container_PostgresSource(t *testing.T) {
	ctx := context.Background()
	srcDSN, dstDSN := testutil.NewPostgresDSN(t), testutil.NewPostgresDSN(t)
	src, err := sql.Open("pgx", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Exec(`CREATE TABLE public.nopk (v text); CREATE TABLE public.ok (id int PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	yaml := fmt.Sprintf("version: 1\nname: doctor-pg\nsource: { kind: postgres, dsn: %q }\ntarget: { kind: postgres, dsn: %q }\ntables: { include: [\"public.*\"] }\nresources: { spool: { path: %s } }\n",
		srcDSN, dstDSN, filepath.Join(dir, "spool"))
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c := &findings.Collector{}
	Run(ctx, cfg, c)
	var out strings.Builder
	findings.WriteHuman(&out, findings.NewReport(cfg.Name, c))
	t.Log(out.String())
	for _, want := range []string{"source_pg_wal_level", "source_pg_publication", "source_table_no_key"} {
		if !c.Codes()[want] {
			t.Errorf("missing finding %s; got %v", want, c.Codes())
		}
	}
	if !c.HasErrors() {
		t.Fatal("want error findings")
	}
	if strings.Contains(out.String(), ":dev@") {
		t.Fatal("output leaks the DSN password")
	}
	// A/B: validate's PG source check stops at the first problem.
	_, err = validate.CheckPGSource(ctx, src, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "wal_level") || strings.Contains(err.Error(), "publication") {
		t.Fatalf("validate PG source check: %v", err)
	}
}

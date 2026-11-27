package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/config"
	"strand/internal/findings"
	"strand/internal/testutil"
	"strand/internal/validate"
)

// Source PG giữ WAL qua replication slot: max_slot_wal_keep_size = -1 (mặc định) là cảnh báo
// source_pg_wal_keep_unlimited; có giới hạn mà slot của pipeline giữ từ 50% giới hạn là source_pg_slot_wal.
// validate (không có bộ gom) trả cùng cảnh báo dạng chuỗi.
func TestDoctor_Container_PGSlotWAL(t *testing.T) {
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image:        "postgres:18",
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "app"},
		Cmd:          []string{"postgres", "-c", "wal_level=logical", "-c", "fsync=off"},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(2 * time.Minute),
	}, Started: true})
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	srcDSN := fmt.Sprintf("postgres://strand:dev@%s:%s/app?sslmode=disable", host, port.Port())
	src, err := sql.Open("pgx", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, q := range []string{
		`CREATE TABLE public.ok (id int PRIMARY KEY)`,
		`CREATE PUBLICATION strand_doctor_wal FOR TABLE public.ok`,
		`SELECT pg_create_logical_replication_slot('strand_doctor_wal', 'pgoutput')`,
	} {
		if _, err := src.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	yaml := fmt.Sprintf("version: 1\nname: doctor-wal\nsource: { kind: postgres, dsn: %q, slot: strand_doctor_wal, publication: strand_doctor_wal }\ntarget: { kind: postgres, dsn: %q }\ntables: { include: [\"public.*\"] }\nresources: { spool: { path: %s } }\n",
		srcDSN, testutil.NewPostgresDSN(t), filepath.Join(dir, "spool"))
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	run := func() (map[string]bool, string) {
		t.Helper()
		col := &findings.Collector{}
		Run(ctx, cfg, col)
		var out strings.Builder
		findings.WriteHuman(&out, findings.NewReport(cfg.Name, col))
		return col.Codes(), out.String()
	}

	codes, out := run()
	if !codes["source_pg_wal_keep_unlimited"] || codes["source_pg_slot_wal"] {
		t.Fatalf("keep = -1, fresh slot: want source_pg_wal_keep_unlimited only\n%s", out)
	}
	if !strings.Contains(out, "max_slot_wal_keep_size") {
		t.Fatalf("finding must name the setting and the fix\n%s", out)
	}
	ws, err := validate.CheckPGSource(ctx, src, cfg, nil)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !strings.Contains(strings.Join(ws, "\n"), "max_slot_wal_keep_size = -1") {
		t.Fatalf("validate warnings lack the unlimited WAL warning: %v", ws)
	}

	// Giới hạn 64 MB; slot đứng yên trong khi WAL đi qua 4 segment (16 MB) → giữ >= 48 MB, >= 50%.
	for _, q := range []string{`ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'`, `SELECT pg_reload_conf()`} {
		if _, err := src.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := src.Exec(`INSERT INTO public.ok VALUES ($1)`, i); err != nil {
			t.Fatal(err)
		}
		if _, err := src.Exec(`SELECT pg_switch_wal()`); err != nil {
			t.Fatal(err)
		}
	}
	codes, out = run()
	if codes["source_pg_wal_keep_unlimited"] || !codes["source_pg_slot_wal"] || !strings.Contains(out, "of max_slot_wal_keep_size (64 MB)") {
		t.Fatalf("keep = 64MB, slot holding >= 48 MB: want source_pg_slot_wal naming the limit\n%s", out)
	}
}

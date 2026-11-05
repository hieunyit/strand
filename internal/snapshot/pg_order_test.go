package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/model"
	pgreader "strand/internal/reader/pg"
	"strand/internal/snapshot"
)

func startPGSource(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "postgres:18", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "app"},
		Cmd:        []string{"-c", "wal_level=logical"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(180 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, nat.Port("5432/tcp"))
	cc, err := pgx.ParseConfig(fmt.Sprintf("postgres://strand:dev@%s:%s/app?sslmode=disable", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range pgreader.SessionParams {
		cc.RuntimeParams[k] = v
	}
	db := stdlib.OpenDB(*cc)
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 60; i++ {
		if err = db.Ping(); err == nil {
			return db
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

// invariant: RS-1 — chunk PG bị cắt theo max_chunk_bytes (review R10) phải phủ TRỌN [Start, Cut]. SELECT
// đọc `pk::text` nên `ORDER BY pk` trỏ vào cột kết quả (text): "10" < "9", row cuối không phải khóa lớn
// nhất đã đọc, các khóa nhỏ hơn Cut chưa đọc bị flush xóa ở target và không bao giờ được đọc lại.
func TestR10_PGTruncatedChunkCoversItsWholeRange(t *testing.T) {
	db := startPGSource(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE public.ord (id bigint PRIMARY KEY, pad text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.ord SELECT g, repeat('x', 100) FROM generate_series(1, 30) g`); err != nil {
		t.Fatal(err)
	}
	var ver int
	_ = db.QueryRow("SELECT current_setting('server_version_num')::int").Scan(&ver)
	table := model.TableID{Schema: "public", Table: "ord"}
	opts := snapshot.DefaultChunkOptions()
	opts.MaxChunkBytes = 500 // ~5 row
	s := snapshot.NewMySQLSnapshotter(opts)
	src := &snapshot.PGChunkSource{DB: db, ServerVersion: ver}
	s.Manager.RegisterTable(table, "id", s.Chunker.PlanChunks(table, "id", 1, 30, 30), 30)
	chunk, err := s.Manager.CarveChunk(table, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	read, _, err := src.ReadChunk(ctx, s, chunk, []string{"id", "pad"}, 0, table)
	if err != nil {
		t.Fatal(err)
	}
	if !read.Truncated {
		t.Fatalf("expected a truncated read at %d bytes", opts.MaxChunkBytes)
	}
	got := map[int64]bool{}
	for _, r := range read.Rows {
		got[r.Key[0].(int64)] = true
	}
	for k := int64(1); k <= read.Cut; k++ {
		if !got[k] {
			t.Fatalf("truncated read cut at %d but key %d of [1,%d] was not read (rows %v)", read.Cut, k, read.Cut, got)
		}
	}
}

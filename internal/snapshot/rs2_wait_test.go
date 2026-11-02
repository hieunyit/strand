package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/model"
)

// invariant: RS-2
// Seam giả lập source/replica: ghi lại thứ tự low → wait → SELECT → high và kết nối dùng cho từng bước.

type fakeSource struct {
	mu      sync.Mutex
	log     []string
	gtids   []string // lần lượt trả cho mỗi lần đọc gtid_executed
	waitRes []sql.NullInt64
	nextID  int
}

func (f *fakeSource) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, s)
}

func (f *fakeSource) GetGlobalGTIDExecuted(ctx context.Context) (string, error) {
	f.mu.Lock()
	g := f.gtids[0]
	if len(f.gtids) > 1 {
		f.gtids = f.gtids[1:]
	}
	f.mu.Unlock()
	f.record("gtid:" + g)
	return g, nil
}

func (f *fakeSource) OpenChunkConn(ctx context.Context) (ChunkConn, error) {
	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.mu.Unlock()
	return &fakeConn{f: f, id: id}, nil
}

type fakeConn struct {
	f  *fakeSource
	id int
}

func (c *fakeConn) WaitForExecutedGTIDSet(ctx context.Context, gtidSet string, timeoutSeconds int) (sql.NullInt64, error) {
	c.f.mu.Lock()
	res := sql.NullInt64{Valid: true}
	if len(c.f.waitRes) > 0 {
		res = c.f.waitRes[0]
		c.f.waitRes = c.f.waitRes[1:]
	}
	c.f.mu.Unlock()
	c.f.record("wait:" + gtidSet + "@conn" + itoa(c.id) + "/" + itoa(timeoutSeconds))
	return res, nil
}

func (c *fakeConn) QueryChunk(ctx context.Context, query string, startPK, endPK int64, columnNames []string, pkIdx int, table model.TableID) ([]*model.RowChange, int64, error) {
	c.f.record("select@conn" + itoa(c.id))
	return []*model.RowChange{{Table: table, Op: model.OpInsert, Key: []model.Value{startPK}}}, 1, nil
}

func (c *fakeConn) Close() error { return nil }

func itoa(i int) string { return strconv.Itoa(i) }

func rs2Snapshotter() (*MySQLSnapshotter, *Chunk) {
	s := NewMySQLSnapshotter(DefaultChunkOptions())
	table := model.TableID{Schema: "app", Table: "t"}
	chunks := s.Chunker.PlanChunks(table, "id", 1, 10, 10)
	s.Manager.RegisterTable(table, "id", chunks, 10)
	return s, chunks[0]
}

func TestRS2_WaitRunsOnSameConnBetweenLowAndSelect(t *testing.T) {
	s, chunk := rs2Snapshotter()
	low := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	high := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-7"
	src := &fakeSource{gtids: []string{low, high}}
	rows, h, err := s.readChunkWatermarked(context.Background(), src, chunk, "SELECT", []string{"id"}, 0, chunk.Table)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || h == nil {
		t.Fatalf("rows=%d high=%v", len(rows), h)
	}
	want := []string{
		"gtid:" + low,
		"wait:" + low + "@conn1/30",
		"select@conn1",
		"gtid:" + high,
	}
	if strings.Join(src.log, "|") != strings.Join(want, "|") {
		t.Fatalf("order:\n got %v\nwant %v", src.log, want)
	}
}

func TestRS2_WaitTimeoutIsRetriedThenSelect(t *testing.T) {
	s, chunk := rs2Snapshotter()
	low := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	src := &fakeSource{gtids: []string{low}, waitRes: []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 0, Valid: true}}}
	if _, _, err := s.readChunkWatermarked(context.Background(), src, chunk, "SELECT", []string{"id"}, 0, chunk.Table); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"gtid:" + low,
		"wait:" + low + "@conn1/30",
		"wait:" + low + "@conn1/30",
		"select@conn1",
		"gtid:" + low,
	}
	if strings.Join(src.log, "|") != strings.Join(want, "|") {
		t.Fatalf("order:\n got %v\nwant %v", src.log, want)
	}
}

func TestRS2_WaitResults(t *testing.T) {
	ctx := context.Background()
	f := &fakeSource{}
	c := &fakeConn{f: f, id: 1}
	low := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

	f.waitRes = []sql.NullInt64{{Int64: 1, Valid: true}}
	err := WaitForExecutedGTIDSet(ctx, c, low, WaitExecutedGTIDTimeoutSeconds)
	var me *drivermysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1205 || !isRetryableMySQLError(err) {
		t.Fatalf("timeout must map to retryable 1205, got %v", err)
	}

	f.waitRes = []sql.NullInt64{{}}
	err = WaitForExecutedGTIDSet(ctx, c, low, WaitExecutedGTIDTimeoutSeconds)
	if err == nil || isRetryableMySQLError(err) {
		t.Fatalf("NULL must be a non-retryable error, got %v", err)
	}

	f.waitRes = []sql.NullInt64{{Int64: 0, Valid: true}}
	if err := WaitForExecutedGTIDSet(ctx, c, low, WaitExecutedGTIDTimeoutSeconds); err != nil {
		t.Fatal(err)
	}
}

func TestRS2_PersistentTimeoutFails(t *testing.T) {
	s, chunk := rs2Snapshotter()
	low := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	res := make([]sql.NullInt64, waitGTIDMaxAttempts)
	for i := range res {
		res[i] = sql.NullInt64{Int64: 1, Valid: true}
	}
	src := &fakeSource{gtids: []string{low}, waitRes: res}
	_, _, err := s.readChunkWatermarked(context.Background(), src, chunk, "SELECT", []string{"id"}, 0, chunk.Table)
	if !isRetryableMySQLError(err) {
		t.Fatalf("want 1205 after %d attempts, got %v", waitGTIDMaxAttempts, err)
	}
	for _, l := range src.log {
		if strings.HasPrefix(l, "select") {
			t.Fatalf("SELECT must not run before low is visible: %v", src.log)
		}
	}
}

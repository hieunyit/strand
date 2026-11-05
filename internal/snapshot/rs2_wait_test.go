package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func (c *fakeConn) QueryChunk(ctx context.Context, query string, args []any, columnNames []string, pkIdxs []int, table model.TableID, _ int64) ([]*model.RowChange, int64, error) {
	c.f.record("select@conn" + itoa(c.id))
	return []*model.RowChange{{Table: table, Op: model.OpInsert, Key: []model.Value{args[0]}}}, 1, nil
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
	rows, h, err := s.readChunkWatermarked(context.Background(), src, chunk, newKeySQL(chunk.Table, nil), "`id`", []string{"id"}, []int{0}, chunk.Table)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 1 || h == nil {
		t.Fatalf("rows=%d high=%v", len(rows.Rows), h)
	}
	want := []string{
		"gtid:" + low,
		"wait:" + low + "@conn1/30",
		"select@conn1",
		"gtid:" + high,
	}
	if !strings.EqualFold(strings.Join(src.log, "|"), strings.Join(want, "|")) { // UUID được chuẩn hóa chữ thường
		t.Fatalf("order:\n got %v\nwant %v", src.log, want)
	}
}

func TestRS2_WaitTimeoutIsRetriedThenSelect(t *testing.T) {
	s, chunk := rs2Snapshotter()
	low := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	src := &fakeSource{gtids: []string{low}, waitRes: []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 0, Valid: true}}}
	if _, _, err := s.readChunkWatermarked(context.Background(), src, chunk, newKeySQL(chunk.Table, nil), "`id`", []string{"id"}, []int{0}, chunk.Table); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"gtid:" + low,
		"wait:" + low + "@conn1/30",
		"wait:" + low + "@conn1/30",
		"select@conn1",
		"gtid:" + low,
	}
	if !strings.EqualFold(strings.Join(src.log, "|"), strings.Join(want, "|")) { // UUID được chuẩn hóa chữ thường
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
	_, _, err := s.readChunkWatermarked(context.Background(), src, chunk, newKeySQL(chunk.Table, nil), "`id`", []string{"id"}, []int{0}, chunk.Table)
	if !isRetryableMySQLError(err) {
		t.Fatalf("want 1205 after %d attempts, got %v", waitGTIDMaxAttempts, err)
	}
	for _, l := range src.log {
		if strings.HasPrefix(l, "select") {
			t.Fatalf("SELECT must not run before low is visible: %v", src.log)
		}
	}
}

// invariant: RS-1
// Event của range "Chưa đọc" bị bỏ, nhưng ở MySQL stream thấy event (binlog đã flush/sync) TRƯỚC khi
// InnoDB commit xong: gtid_executed lúc đọc low có thể chưa chứa nó và SELECT có thể chưa thấy nó.
// Trước SELECT phải chờ cả các GTID đã bỏ, nếu không thay đổi đó mất vĩnh viễn (vd. row đổi PK đi
// nơi khác vẫn nằm lại ở target).
func TestRS1_PendingDiscardMustBeVisibleBeforeChunkSelect(t *testing.T) {
	s, chunk := rs2Snapshotter()
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	ev := &model.RowChange{Table: chunk.Table, Op: model.OpDelete, Key: []model.Value{int64(3)}}
	if act, _ := s.Manager.HandleStreamEvent(ev, model.Pos{Engine: model.EngineMySQL, GTID: u + ":7"}); act != ActionDiscard {
		t.Fatalf("pending range: action %v, want discard", act)
	}
	// low đọc ngay sau đó CHƯA chứa :7 (engine chưa commit xong).
	src := &fakeSource{gtids: []string{u + ":1-5", u + ":1-8"}}
	if _, _, err := s.readChunkWatermarked(context.Background(), src, chunk, newKeySQL(chunk.Table, nil), "`id`", []string{"id"}, []int{0}, chunk.Table); err != nil {
		t.Fatal(err)
	}
	var wait string
	for _, l := range src.log {
		if strings.HasPrefix(l, "wait:") {
			wait = l
			break
		}
		if strings.HasPrefix(l, "select") {
			t.Fatalf("SELECT ran before any wait: %v", src.log)
		}
	}
	set := ParseMySQLGTIDSet(strings.TrimSuffix(strings.TrimPrefix(wait, "wait:"), "@conn1/30"))
	if !set.Contains(model.Pos{Engine: model.EngineMySQL, GTID: u + ":7"}) {
		t.Fatalf("wait set %q does not include the discarded GTID :7 (log %v)", wait, src.log)
	}
	// Sau khi đã chờ, GTID :7 visible với mọi read view: chunk sau không phải chờ lại.
	s.Manager.mu.RLock()
	left := s.Manager.tables[chunk.Table.String()].PendingDiscards.String()
	s.Manager.mu.RUnlock()
	if left != "" {
		t.Fatalf("PendingDiscards not trimmed after confirmed wait: %q", left)
	}
}

// Event đến trong lúc chunk đã chuyển "Đang đọc" nhưng chưa có low phải được buffer (không bỏ).
func TestRS1_EventBetweenBeginReadingAndLowIsBuffered(t *testing.T) {
	s, chunk := rs2Snapshotter()
	s.Manager.BeginReading(chunk)
	ev := &model.RowChange{Table: chunk.Table, Op: model.OpDelete, Key: []model.Value{int64(4)}}
	if act, _ := s.Manager.HandleStreamEvent(ev, model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:9"}); act != ActionBuffer {
		t.Fatalf("action %v, want buffer", act)
	}
}

// invariant: RS-1
// Tái hiện từ trace e2e (snapshot add dưới tải, khóa 49200): INSERT được buffer khi chunk "Đang đọc"
// chưa có low; low đọc sau chứa DELETE kế tiếp nên DELETE bị bỏ — ảnh INSERT cũ trong buffer phải bị
// bỏ theo, nếu không flush ghi lại row đã xóa.
func TestRS1_DiscardInLowDropsOlderBufferedImage(t *testing.T) {
	s, chunk := rs2Snapshotter()
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	pos := func(n int) model.Pos { return model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", u, n)} }
	s.Manager.BeginReading(chunk)
	ins := &model.RowChange{Table: chunk.Table, Op: model.OpInsert, Key: []model.Value{int64(7)},
		Cols: []model.ColValue{{Name: "id", Value: int64(7)}}}
	if act, _ := s.Manager.HandleStreamEvent(ins, pos(1964)); act != ActionBuffer {
		t.Fatalf("insert: %v", act)
	}
	s.Manager.StartReading(chunk, ParseMySQLGTIDSet(u+":1-2128"))
	del := &model.RowChange{Table: chunk.Table, Op: model.OpDelete, Key: []model.Value{int64(7)}}
	if act, _ := s.Manager.HandleStreamEvent(del, pos(1968)); act != ActionDiscard {
		t.Fatalf("delete in low: %v", act)
	}
	// Snapshot (read view sau low) không còn row 7.
	s.Manager.FinishReading(chunk, ParseMySQLGTIDSet(u+":1-2140"), nil)
	s.Manager.mu.Lock()
	merged := s.Manager.MergeLocked(chunk)
	s.Manager.mu.Unlock()
	for _, r := range merged {
		if r.Key[0] == int64(7) {
			t.Fatalf("deleted row 7 resurrected by a stale buffered INSERT")
		}
	}
}

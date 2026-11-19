package snapshot

import (
	"context"
	"sync"
	"testing"
	"time"

	"strand/internal/model"
)

// Snapshot song song nhiều bảng (bootstrap.logical.max_parallel): FlushChunk không được giữ
// RangeStateManager.mu suốt lần ghi target, nếu không reader của BẢNG KHÁC (BeginReading, CarveChunk,
// StartReading...) đứng chờ mọi flush và các bảng bị tuần tự hóa.

func parallelTestChunk(t *testing.T, s *MySQLSnapshotter, tbl model.TableID, lo, hi int64) *Chunk {
	t.Helper()
	s.Manager.RegisterTable(tbl, "id", []*Chunk{{ID: 1, Table: tbl, PKCol: "id", StartPK: lo, EndPK: hi,
		State: StatePending, BufferedChanges: map[Key]*model.RowChange{}}}, hi)
	c, err := s.Manager.CarveChunk(tbl, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func intRow(tbl model.TableID, id int64) *model.RowChange {
	return &model.RowChange{Table: tbl, Op: model.OpInsert, Key: []model.Value{id},
		Cols: []model.ColValue{{Name: "id", Value: id}}}
}

// Trong lúc bảng A đang ghi target (FlushRows chặn), reader của bảng B phải làm được mọi bước range-state.
func TestParallelFlushDoesNotBlockOtherTablesRangeState(t *testing.T) {
	a := model.TableID{Schema: "db", Table: "a"}
	b := model.TableID{Schema: "db", Table: "b"}
	s := NewMySQLSnapshotter(DefaultChunkOptions())
	s.Barrier = func(_ context.Context, fn func() error) error { return fn() } // router "dừng" (không có router)
	inFlush := make(chan struct{})
	release := make(chan struct{})
	s.FlushRows = func(_ context.Context, tbl model.TableID, _ string, _ []string, _ []*model.RowChange, _, _ int64) error {
		if tbl == a {
			close(inFlush)
			<-release
		}
		return nil
	}
	ca := parallelTestChunk(t, s, a, 1, 10)
	s.Manager.StartReading(ca, nil)
	s.Manager.FinishReading(ca, nil, []*model.RowChange{intRow(a, 1), intRow(a, 2)})
	flushErr := make(chan error, 1)
	go func() { flushErr <- s.FlushChunk(context.Background(), nil, ca, a, "id", []string{"id"}) }()
	<-inFlush

	// Bảng B: đăng ký, cắt chunk, đọc, merge, flush — tất cả khi A còn đang ghi target.
	done := make(chan error, 1)
	go func() {
		cb := parallelTestChunk(t, s, b, 1, 5)
		s.Manager.BeginReading(cb)
		s.Manager.StartReading(cb, nil)
		s.Manager.FinishReading(cb, nil, []*model.RowChange{intRow(b, 3)})
		_ = s.Manager.StateCounts()
		done <- s.FlushChunk(context.Background(), nil, cb, b, "id", []string{"id"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush of table b: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("range-state of table b blocked while table a was writing its chunk to the target (FlushChunk holds RangeStateManager.mu)")
	}
	close(release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}
	if ca.State != StateDone {
		t.Fatalf("chunk of a state = %s, want Done", ca.State)
	}
	if got := s.RowsFlushed.Load(); got != 3 {
		t.Fatalf("RowsFlushed = %d, want 3", got)
	}
}

// Chunk chỉ thành "Xong" SAU khi transaction target commit: event tới sau đó mới apply thẳng. Nếu router
// (vi phạm hợp đồng Barrier) gặp chunk đang ghi ngoài khóa, event bị từ chối — không buffer vào chunk mà
// flush đã merge (mất event, RS-1).
func TestUnlockedFlushRejectsStreamEventsUntilCommitted(t *testing.T) {
	a := model.TableID{Schema: "db", Table: "a"}
	s := NewMySQLSnapshotter(DefaultChunkOptions())
	s.Barrier = func(_ context.Context, fn func() error) error { return fn() }
	inFlush := make(chan struct{})
	release := make(chan struct{})
	s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
		close(inFlush)
		<-release
		return nil
	}
	c := parallelTestChunk(t, s, a, 1, 10)
	s.Manager.StartReading(c, nil)
	s.Manager.FinishReading(c, nil, []*model.RowChange{intRow(a, 1)})
	errc := make(chan error, 1)
	go func() { errc <- s.FlushChunk(context.Background(), nil, c, a, "id", []string{"id"}) }()
	<-inFlush
	ev := &model.RowChange{Table: a, Op: model.OpUpdate, Key: []model.Value{int64(5)}, Cols: []model.ColValue{{Name: "id", Value: int64(5)}}}
	acts := make(chan Action, 1)
	go func() {
		act, _ := s.Manager.HandleStreamEvent(ev, model.Pos{})
		acts <- act
	}()
	select {
	case act := <-acts:
		if act != ActionReject {
			t.Fatalf("event during an unlocked flush: action %s, want Reject", act)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("range-state blocked for the whole target write (FlushChunk holds RangeStateManager.mu)")
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if act, _ := s.Manager.HandleStreamEvent(ev, model.Pos{}); act != ActionApply {
		t.Fatalf("event after the flush committed: action %s, want Apply", act)
	}
}

// Không có Barrier (router có thể chạy song song): giữ khóa suốt flush như cũ — event của router chờ, rồi
// apply sau khi chunk Xong.
func TestFlushWithoutBarrierKeepsLock(t *testing.T) {
	a := model.TableID{Schema: "db", Table: "a"}
	s := NewMySQLSnapshotter(DefaultChunkOptions())
	inFlush := make(chan struct{})
	release := make(chan struct{})
	s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
		close(inFlush)
		<-release
		return nil
	}
	c := parallelTestChunk(t, s, a, 1, 10)
	s.Manager.StartReading(c, nil)
	s.Manager.FinishReading(c, nil, nil)
	errc := make(chan error, 1)
	go func() { errc <- s.FlushChunk(context.Background(), nil, c, a, "id", []string{"id"}) }()
	<-inFlush
	acts := make(chan Action, 1)
	go func() {
		act, _ := s.Manager.HandleStreamEvent(intRow(a, 5), model.Pos{})
		acts <- act
	}()
	select {
	case act := <-acts:
		t.Fatalf("router was not held during a flush without Barrier (action %s)", act)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if act := <-acts; act != ActionApply {
		t.Fatalf("action after flush = %s, want Apply", act)
	}
}

// WEIGHT_STRING của khóa row snapshot (hỏi source) được tính ngoài RangeStateManager.mu.
func TestFlushEncodesSnapshotKeysOutsideManagerLock(t *testing.T) {
	tbl := model.TableID{Schema: "db", Table: "w"}
	s := NewMySQLSnapshotter(DefaultChunkOptions())
	s.Barrier = func(_ context.Context, fn func() error) error { return fn() }
	var probe sync.Once
	blocked := make(chan bool, 1)
	codec := &PKCodec{Cols: []PKColumn{{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}},
		Weigh: func(_ PKColumn, v model.Value) ([]byte, error) { return []byte(v.(string)), nil }}
	codec.WeighMany = func(_ PKColumn, vs []model.Value) (map[string][]byte, error) {
		probe.Do(func() {
			got := make(chan struct{})
			go func() { s.Manager.TableCount(); close(got) }()
			select {
			case <-got:
				blocked <- false
			case <-time.After(2 * time.Second):
				blocked <- true
			}
		})
		out := map[string][]byte{}
		for _, v := range vs {
			out[v.(string)] = []byte(v.(string))
		}
		return out, nil
	}
	var flushed []*model.RowChange
	s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
		return nil
	}
	s.FlushRange = func(_ context.Context, _ model.TableID, _ []string, rows []*model.RowChange, _, _ Bound) error {
		flushed = rows
		return nil
	}
	lo, _ := codec.BoundOf([]model.Value{"a"})
	hi, _ := codec.BoundOf([]model.Value{"z"})
	s.Manager.RegisterTableKeys(tbl, "k", codec, []*Chunk{{Table: tbl, PKCol: "k", Lo: lo, Hi: hi, State: StatePending,
		BufferedChanges: map[Key]*model.RowChange{}}}, &hi)
	c, err := s.Manager.CarveChunkKeys(tbl, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	s.Manager.StartReading(c, nil)
	s.Manager.FinishReading(c, nil, []*model.RowChange{
		{Table: tbl, Op: model.OpInsert, Key: []model.Value{"b"}, Cols: []model.ColValue{{Name: "k", Value: "b"}}},
		{Table: tbl, Op: model.OpInsert, Key: []model.Value{"c"}, Cols: []model.ColValue{{Name: "k", Value: "c"}}},
	})
	done := make(chan error, 1)
	go func() { done <- s.FlushChunk(context.Background(), nil, c, tbl, "k", []string{"k"}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flush hung")
	}
	if <-blocked {
		t.Fatal("snapshot key weights were computed while holding RangeStateManager.mu")
	}
	if len(flushed) != 2 {
		t.Fatalf("flushed %d rows, want 2", len(flushed))
	}
}

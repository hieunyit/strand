package sched

// Regression tests from the Gate 1 code review (2026-11-02).
// Each test reproduces a confirmed defect and must stay in the suite permanently (04 §2).

import (
	"context"
	"errors"
	"testing"
	"time"

	"strand/internal/model"
)

func regTxn(table string, pks ...int64) *model.Txn {
	t := &model.Txn{}
	for _, pk := range pks {
		t.Rows = append(t.Rows, model.RowChange{
			Table: model.TableID{Schema: "app", Table: table},
			Op:    model.OpUpdate,
			Key:   []model.Value{pk},
		})
	}
	return t
}

// invariant: CO-1, CP-1
// A failed Txn must not be treated as applied: its dependents must stay blocked,
// lwd must not move past it, and the scheduler must refuse new work.
func TestCO1_DoneWithErrorDoesNotReleaseDependents(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())

	a, b := regTxn("t", 1), regTxn("t", 1)
	if err := s.Admit(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.Admit(ctx, b); err != nil {
		t.Fatal(err)
	}
	<-s.Ready()

	s.Done(a.Seq(), errors.New("apply failed"))

	select {
	case got := <-s.Ready():
		t.Fatalf("txn %d released although its dependency %d failed", got.Seq(), a.Seq())
	case <-time.After(150 * time.Millisecond):
	}
	if s.Lwd() >= a.Seq() {
		t.Fatalf("lwd advanced to %d past failed txn %d", s.Lwd(), a.Seq())
	}
	if s.Err() == nil {
		t.Fatalf("scheduler must expose the apply error")
	}
	if err := s.Admit(ctx, regTxn("t", 99)); err == nil {
		t.Fatalf("Admit must fail after a Txn failed")
	}
}

// invariant: CO-1 (liveness)
// Done must never block: Admit used to hold s.mu while blocked on a full ready
// channel, so every worker calling Done deadlocked.
func TestCO1_DoneDoesNotDeadlockWhenReadyQueueFull(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())

	first := regTxn("t", 0)
	if err := s.Admit(ctx, first); err != nil {
		t.Fatal(err)
	}
	<-s.Ready() // worker takes txn 1 and works on it

	for i := int64(1); i <= int64(cap(s.readyCh))+10; i++ {
		admitted := make(chan struct{})
		go func(pk int64) {
			_ = s.Admit(ctx, regTxn("t", 100000+pk))
			close(admitted)
		}(i)
		select {
		case <-admitted:
		case <-time.After(2 * time.Second):
			// Admit is allowed to apply back-pressure; stop filling.
			i = 1 << 40
		}
	}

	done := make(chan struct{})
	go func() {
		s.Done(first.Seq(), nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Done blocked: scheduler deadlocks when the ready queue is full")
	}
}

// invariant: CO-1
// A spilled Txn has no Rows in RAM. It must still order against every Txn that
// touches the same tables (table-level dependency), never run unordered.
func TestCO1_SpilledTxnOrdersAgainstConflictingTxns(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())

	before := regTxn("big", 7)
	if err := s.Admit(ctx, before); err != nil {
		t.Fatal(err)
	}
	spilled := &model.Txn{
		SpillPath:     "/nonexistent/spill",
		RowCount:      50_000_000,
		SpilledTables: []model.TableID{{Schema: "app", Table: "big"}},
	}
	if err := s.Admit(ctx, spilled); err != nil {
		t.Fatal(err)
	}
	after := regTxn("big", 8)
	if err := s.Admit(ctx, after); err != nil {
		t.Fatal(err)
	}

	got := <-s.Ready()
	if got.Seq() != before.Seq() {
		t.Fatalf("expected txn %d first, got %d", before.Seq(), got.Seq())
	}
	select {
	case x := <-s.Ready():
		t.Fatalf("txn %d ran concurrently with a conflicting txn (spilled txn not ordered)", x.Seq())
	case <-time.After(100 * time.Millisecond):
	}
	s.Done(before.Seq(), nil)
	if x := <-s.Ready(); x.Seq() != spilled.Seq() {
		t.Fatalf("expected spilled txn %d, got %d", spilled.Seq(), x.Seq())
	}
	select {
	case x := <-s.Ready():
		t.Fatalf("txn %d ran before spilled table-level txn finished", x.Seq())
	case <-time.After(100 * time.Millisecond):
	}
	s.Done(spilled.Seq(), nil)
	if x := <-s.Ready(); x.Seq() != after.Seq() {
		t.Fatalf("expected txn %d, got %d", after.Seq(), x.Seq())
	}
}

// A spilled Txn whose tables are unknown cannot be ordered safely: reject it.
func TestCO1_SpilledTxnWithoutTablesRejected(t *testing.T) {
	s := NewScheduler(DefaultConfig())
	err := s.Admit(context.Background(), &model.Txn{SpillPath: "/x", RowCount: 20000})
	if err == nil {
		t.Fatalf("spilled txn without table list must be rejected")
	}
}

// Performance: Done must cost O(keys of the Txn), not O(all live writeset entries).
func TestCO2_DoneCostIndependentOfLiveWriteset(t *testing.T) {
	measure := func(live int) time.Duration {
		ctx := context.Background()
		s := NewScheduler(DefaultConfig())
		if live > 0 {
			pks := make([]int64, live)
			for i := range pks {
				pks[i] = int64(i)
			}
			_ = s.Admit(ctx, regTxn("t", pks...))
			<-s.Ready()
		}
		const n = 300
		start := time.Now()
		for i := 0; i < n; i++ {
			x := regTxn("t", int64(10_000_000+i))
			_ = s.Admit(ctx, x)
			<-s.Ready()
			s.Done(x.Seq(), nil)
		}
		return time.Since(start) / n
	}
	small := measure(0)
	large := measure(40_000)
	t.Logf("per Admit+Done: live=0 %v, live=40000 %v", small, large)
	if large > 20*small+50*time.Microsecond {
		t.Fatalf("Done scales with live writeset size: %v vs %v", large, small)
	}
}

// Serial (strict, một writer): mọi Txn được phát ngay theo đúng thứ tự seq, kể cả khi xung đột,
// để writer gom được batch lớn; thứ tự apply chính là thứ tự nguồn.
func TestCO1_SerialModeEmitsInSeqOrderImmediately(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Serial = true
	s := NewScheduler(cfg)
	ctx := context.Background()
	var txns []*model.Txn
	for i := 0; i < 5; i++ {
		tx := regTxn("t", 1) // cùng một khóa: xung đột với nhau
		if err := s.Admit(ctx, tx); err != nil {
			t.Fatal(err)
		}
		txns = append(txns, tx)
	}
	for _, want := range txns {
		select {
		case got := <-s.Ready():
			if got.Seq() != want.Seq() {
				t.Fatalf("got seq %d, want %d", got.Seq(), want.Seq())
			}
		case <-time.After(time.Second):
			t.Fatalf("seq %d not emitted immediately in serial mode", want.Seq())
		}
	}
	for _, tx := range txns {
		s.Done(tx.Seq(), nil)
	}
	if s.Lwd() != txns[len(txns)-1].Seq() {
		t.Fatalf("lwd = %d", s.Lwd())
	}
}

package sched

// Gate 2 (G2-8 / CO-1, CO-2): Txn tràn đĩa (> MaxInMemRows của assembler, 10.000 row) trước đây luôn
// bị coi là cấp bảng, nên ngưỡng cấp bảng thực tế là 10.000 row thay vì table_level_threshold = 50.000.
// Writeset của Txn tràn đĩa giờ được gom khi ghi file tràn (model.WritesetCollector, như router làm),
// và chỉ nâng cấp bảng khi vượt ngưỡng / writeset_budget.

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
)

var bigTbl = model.TableID{Schema: "app", Table: "big"}

// spilledFrom dựng một Txn tràn đĩa từ rows giống router (pipeline.route): gom writeset theo cấu hình
// của scheduler trong khi "ghi" row ra file tràn (file không cần tồn tại: scheduler không đọc nó).
func spilledFrom(cfg Config, rows []model.RowChange) *model.Txn {
	ws := model.NewWritesetCollector(cfg.TableLevelThreshold, cfg.WritesetBudget)
	seen := map[model.TableID]bool{}
	var tables []model.TableID
	note := func(t model.TableID) {
		if !seen[t] {
			seen[t] = true
			tables = append(tables, t)
		}
	}
	for i := range rows {
		ws.Add(&rows[i])
		note(rows[i].Table)
		for _, fk := range rows[i].ForeignKeys {
			note(fk.ParentTable)
		}
	}
	return &model.Txn{
		SpillPath:     "/nonexistent/strand-routed-test.spill",
		RowCount:      int64(len(rows)),
		Bytes:         int64(len(rows)) * 1024, // payload (trên đĩa)
		SpilledTables: tables,
		SpillWriteset: ws.Writeset(),
	}
}

func bigRows(from, n int) []model.RowChange {
	rows := make([]model.RowChange, n)
	for i := range rows {
		rows[i] = model.RowChange{Table: bigTbl, Op: model.OpUpdate, Key: []model.Value{int64(from + i)}}
	}
	return rows
}

func mustAdmit(t *testing.T, s *Scheduler, txn *model.Txn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Admit(ctx, txn); err != nil {
		t.Fatalf("admit: %v", err)
	}
}

// readyWithin trả Txn sẵn sàng tiếp theo, hoặc nil nếu không có trong d.
func readyWithin(s *Scheduler, d time.Duration) *model.Txn {
	select {
	case x := <-s.Ready():
		return x
	case <-time.After(d):
		return nil
	}
}

// invariant: CO-1. Txn tràn đĩa 20.000 row (> 10.000 của assembler, ≤ table_level_threshold) giữ cấp row:
// Txn chạm một khóa của nó phải chờ; Txn chạm khóa khác của cùng bảng chạy song song.
func TestCO1_SpilledTxnBelowThresholdKeepsRowLevel(t *testing.T) {
	cfg := DefaultConfig()
	s := NewScheduler(cfg)
	defer s.Close()

	big := spilledFrom(cfg, bigRows(0, 20_000))
	mustAdmit(t, s, big)
	if x := readyWithin(s, time.Second); x == nil || x.Seq() != big.Seq() {
		t.Fatalf("spilled txn not ready")
	}
	conflict := regTxn("big", 5)
	mustAdmit(t, s, conflict)
	other := regTxn("big", 50_000)
	mustAdmit(t, s, other)

	x := readyWithin(s, time.Second)
	if x == nil || x.Seq() != other.Seq() {
		t.Fatalf("txn on a key the spilled txn does not touch must run in parallel (row-level), got %v", x)
	}
	if y := readyWithin(s, 100*time.Millisecond); y != nil {
		t.Fatalf("txn %d touching a key of the running spilled txn ran concurrently", y.Seq())
	}
	s.mu.Lock()
	retained := s.retainedKeys
	s.mu.Unlock()
	if retained < 20_000 {
		t.Fatalf("retainedKeys=%d: keys of the spilled txn must count toward writeset_budget", retained)
	}
	s.Done(other.Seq(), nil)
	s.Done(big.Seq(), nil)
	if y := readyWithin(s, time.Second); y == nil || y.Seq() != conflict.Seq() {
		t.Fatalf("conflicting txn not released after the spilled txn finished")
	}
	s.Done(conflict.Seq(), nil)
}

// invariant: CO-1. Mọi loại khóa của Txn tràn đĩa được giữ: UNIQUE (kể cả khóa hằng của index prefix),
// OldKey khi đổi PK, khóa FK ở bảng cha.
func TestCO1_SpilledTxnKeepsAllWritesetKeyKinds(t *testing.T) {
	parent := model.TableID{Schema: "app", Table: "parent"}
	prefixKey := []model.Value{"\x00prefix-index"} // = assemble.prefixIndexKey
	rows := bigRows(0, 12_000)
	rows[0].OldKey = []model.Value{int64(900_000)}
	rows[1].AddUniqueKey("uk_code", []model.Value{"c-1"}, []model.Value{"c-0"})
	rows[2].AddUniqueKey("uk_prefix", prefixKey)
	rows[3].AddForeignKey(parent, []model.Value{int64(77)})
	rows[4].AddForeignKeyIndex(parent, "uk_ref", []model.Value{"r-9"})

	probes := map[string]*model.Txn{
		"old_pk":     regTxn("big", 900_000),
		"uk_new":     {Rows: []model.RowChange{{Table: bigTbl, Key: []model.Value{int64(-1)}, UniqueKeys: []model.UniqueKey{{Name: "uk_code", Key: []model.Value{"c-1"}}}}}},
		"uk_old":     {Rows: []model.RowChange{{Table: bigTbl, Key: []model.Value{int64(-2)}, UniqueKeys: []model.UniqueKey{{Name: "uk_code", Key: []model.Value{"c-0"}}}}}},
		"uk_prefix":  {Rows: []model.RowChange{{Table: bigTbl, Key: []model.Value{int64(-3)}, UniqueKeys: []model.UniqueKey{{Name: "uk_prefix", Key: prefixKey}}}}},
		"fk_pk":      {Rows: []model.RowChange{{Table: parent, Key: []model.Value{int64(77)}}}},
		"fk_uk":      {Rows: []model.RowChange{{Table: parent, Key: []model.Value{int64(-4)}, UniqueKeys: []model.UniqueKey{{Name: "uk_ref", Key: []model.Value{"r-9"}}}}}},
		"parent_new": {Rows: []model.RowChange{{Table: parent, Key: []model.Value{int64(78)}}}},
	}
	for name, probe := range probes {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			s := NewScheduler(cfg)
			defer s.Close()
			big := spilledFrom(cfg, append([]model.RowChange(nil), rows...))
			mustAdmit(t, s, big)
			<-s.Ready()
			mustAdmit(t, s, probe)
			x := readyWithin(s, 100*time.Millisecond)
			if name == "parent_new" {
				if x == nil {
					t.Fatalf("parent row not referenced by the spilled txn must run in parallel")
				}
				return
			}
			if x != nil {
				t.Fatalf("probe %s ran concurrently with the spilled txn that writes the same key", name)
			}
		})
	}
}

// Txn tràn đĩa chạm > table_level_threshold khóa trên một bảng: cấp bảng cho bảng đó (map row bị xóa),
// bảng khác của nó vẫn cấp row.
func TestCO1_SpilledTxnAboveThresholdIsTableLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TableLevelThreshold = 1000
	s := NewScheduler(cfg)
	defer s.Close()

	rows := bigRows(0, 1001)
	small := model.TableID{Schema: "app", Table: "small"}
	rows = append(rows, model.RowChange{Table: small, Op: model.OpUpdate, Key: []model.Value{int64(1)}})
	big := spilledFrom(cfg, rows)
	mustAdmit(t, s, big)
	<-s.Ready()
	if n := s.TableWritesetEntries("app.big"); n != 0 {
		t.Fatalf("table-level table must not keep row entries, got %d", n)
	}

	far := regTxn("big", 999_999)
	mustAdmit(t, s, far)
	if x := readyWithin(s, 100*time.Millisecond); x != nil {
		t.Fatalf("txn %d ran concurrently with a table-level spilled txn on the same table", x.Seq())
	}
	smallOther := regTxn("small", 2)
	mustAdmit(t, s, smallOther)
	if x := readyWithin(s, time.Second); x == nil || x.Seq() != smallOther.Seq() {
		t.Fatalf("table under the threshold must stay row-level, got %v", x)
	}
	s.Done(smallOther.Seq(), nil)
	s.Done(big.Seq(), nil)
	if x := readyWithin(s, time.Second); x == nil || x.Seq() != far.Seq() {
		t.Fatalf("txn not released after the table-level spilled txn finished")
	}
}

// invariant: CO-2. writeset_budget: Txn tràn đĩa có nhiều khóa hơn ngân sách không giữ khóa (cấp bảng);
// Txn tràn đĩa dưới ngân sách nhưng đẩy tổng map row vượt ngân sách thì bảng lớn nhất lên cấp bảng.
func TestCO2_SpilledTxnWritesetBudgetFallback(t *testing.T) {
	t.Run("collector_over_budget", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.WritesetBudget = 1000
		s := NewScheduler(cfg)
		defer s.Close()
		big := spilledFrom(cfg, bigRows(0, 1500))
		if big.SpillWriteset != nil {
			t.Fatalf("keys over writeset_budget must be dropped while collecting")
		}
		mustAdmit(t, s, big)
		<-s.Ready()
		mustAdmit(t, s, regTxn("big", 999_999))
		if x := readyWithin(s, 100*time.Millisecond); x != nil {
			t.Fatalf("spilled txn over writeset_budget must be table-level")
		}
	})
	t.Run("scheduler_budget", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.WritesetBudget = 1000
		s := NewScheduler(cfg)
		defer s.Close()
		first := spilledFrom(cfg, bigRows(0, 600))
		mustAdmit(t, s, first)
		<-s.Ready()
		second := spilledFrom(cfg, bigRows(10_000, 600)) // 600 + 600 > 1000
		mustAdmit(t, s, second)
		if !s.IsForcedTableLevel("app.big") {
			t.Fatalf("largest table must switch to table-level when writeset_budget is exceeded")
		}
		if x := readyWithin(s, 100*time.Millisecond); x != nil {
			t.Fatalf("txn %d must wait: table switched to table-level", x.Seq())
		}
	})
}

// invariant: CO-2. Txn tràn đĩa chờ trong DAG chỉ tính phần RAM (khóa + metadata) vào MaxPendingBytes,
// không tính payload trên đĩa (Txn.Bytes).
func TestCO2_SpilledTxnPendingBytesCountsOnlyMemory(t *testing.T) {
	cfg := DefaultConfig() // MaxPendingBytes = 512 MiB
	s := NewScheduler(cfg)
	defer s.Close()

	head := regTxn("big", 1) // kẹt ở target: chưa Done
	mustAdmit(t, s, head)
	<-s.Ready()
	for i := 0; i < 3; i++ {
		rows := bigRows(1, 11_000) // chạm khóa 1 → chờ head
		sp := spilledFrom(cfg, rows)
		sp.Bytes = 400 << 20 // payload 400 MiB trên đĩa
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := s.Admit(ctx, sp)
		cancel()
		if err != nil {
			t.Fatalf("admit spilled txn %d: %v (payload on disk counted against MaxPendingBytes)", i, err)
		}
	}
	s.mu.Lock()
	pb, pending := s.pendingBytes, len(s.pendingTxns)
	s.mu.Unlock()
	if pending != 3 {
		t.Fatalf("pending=%d, want 3", pending)
	}
	if pb <= 0 || pb > 16<<20 {
		t.Fatalf("pendingBytes=%d: want only the in-memory part (keys + metadata) of spilled txns", pb)
	}
	// Txn trong RAM vẫn tính đủ Bytes.
	inMem := regTxn("big", 1)
	inMem.Bytes = 600 << 20
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := s.Admit(ctx, inMem); err == nil {
		t.Fatalf("in-memory txn over MaxPendingBytes must still be held back")
	}
	s.Done(head.Seq(), nil)
}

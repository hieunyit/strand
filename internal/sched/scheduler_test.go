package sched

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/model"
)

// invariant: CO-1
// AC1: TestCO1: hai Txn có writeset giao nhau commit đúng thứ tự source.
func TestCO1_WritesetOrder(t *testing.T) {
	ctx := context.Background()
	sched := NewScheduler(DefaultConfig())

	// 1. Kiểm thử cơ bản 2 Txn xung đột cùng một key
	tbl := model.TableID{Schema: "test", Table: "orders"}
	txn1 := &model.Txn{
		Rows: []model.RowChange{
			{Table: tbl, Op: model.OpUpdate, Key: []model.Value{100}},
		},
	}
	txn2 := &model.Txn{
		Rows: []model.RowChange{
			{Table: tbl, Op: model.OpUpdate, Key: []model.Value{100}},
		},
	}

	_ = sched.Admit(ctx, txn1)
	_ = sched.Admit(ctx, txn2)

	// Txn1 phải được phát vào readyCh trước
	select {
	case ready := <-sched.Ready():
		if ready.Seq() != txn1.Seq() {
			t.Fatalf("kỳ vọng txn1 (seq=%d) ready trước, got seq=%d", txn1.Seq(), ready.Seq())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout chờ txn1 ready")
	}

	// Txn2 KHÔNG ĐƯỢC ready khi txn1 chưa Done
	select {
	case ready := <-sched.Ready():
		t.Fatalf("vi phạm CO-1: txn2 (seq=%d) ready trước khi txn1 Done!", ready.Seq())
	case <-time.After(100 * time.Millisecond):
		// Đúng kỳ vọng: Txn2 bị chặn lại bởi dependency của Txn1
	}

	// Báo Txn1 Done
	sched.Done(txn1.Seq(), nil)

	// Bây giờ Txn2 phải được unblock và emit vào readyCh
	select {
	case ready := <-sched.Ready():
		if ready.Seq() != txn2.Seq() {
			t.Fatalf("kỳ vọng txn2 (seq=%d) ready sau khi txn1 Done, got seq=%d", txn2.Seq(), ready.Seq())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout chờ txn2 ready sau khi txn1 Done")
	}
	sched.Done(txn2.Seq(), nil)

	// 2. Kiểm thử nhiều worker đồng thời xử lý các transaction có hot key
	t.Run("ConcurrentWorkers_StrictCommitOrder", func(t *testing.T) {
		concurSched := NewScheduler(DefaultConfig())
		numTxns := 200
		numWorkers := 8

		// Mỗi key lưu lại seq cuối cùng đã commit
		var committedOrderMu sync.Mutex
		lastCommittedSeqByKey := make(map[int]uint64)

		var wg sync.WaitGroup
		var completedCount int64

		// Khởi động worker pool
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for txn := range concurSched.Ready() {
					// Lấy key đang chạm
					keyVal := txn.Rows[0].Key[0].(int)

					committedOrderMu.Lock()
					lastSeq := lastCommittedSeqByKey[keyVal]
					if lastSeq >= txn.Seq() {
						t.Errorf("vi phạm bất biến CO-1 trên key %d: lastSeq=%d >= curSeq=%d",
							keyVal, lastSeq, txn.Seq())
					}
					// Giả lập thời gian apply
					time.Sleep(100 * time.Microsecond)
					lastCommittedSeqByKey[keyVal] = txn.Seq()
					committedOrderMu.Unlock()

					concurSched.Done(txn.Seq(), nil)
					atomic.AddInt64(&completedCount, 1)

					if atomic.LoadInt64(&completedCount) == int64(numTxns) {
						concurSched.Close()
						close(concurSched.readyCh)
					}
				}
			}()
		}

		// Nạp 200 transaction tuần tự, luân phiên trên 5 hot key (0..4)
		for i := 1; i <= numTxns; i++ {
			hotKey := i % 5
			tObj := &model.Txn{
				Rows: []model.RowChange{
					{Table: tbl, Op: model.OpUpdate, Key: []model.Value{hotKey}},
				},
			}
			tObj.SetSeq(uint64(i))
			if err := concurSched.Admit(ctx, tObj); err != nil {
				t.Fatalf("failed to admit: %v", err)
			}
		}

		wg.Wait()
		if completedCount != int64(numTxns) {
			t.Fatalf("chỉ hoàn thành %d / %d transaction", completedCount, numTxns)
		}
		t.Logf("AC1 TestCO1 PASS: 200 Txns trên %d worker tuân thủ tuyệt đối thứ tự source", numWorkers)
	})
}

// AC2: Txn vượt 50.000 khóa được nâng lên khóa cấp bảng; map row được dọn.
func TestAC2_TableLevelLock(t *testing.T) {
	ctx := context.Background()
	// Ngưỡng cấp bảng: 50.000 khóa (theo spec 02 §6)
	cfg := DefaultConfig()
	cfg.TableLevelThreshold = 50000
	sched := NewScheduler(cfg)

	tbl := model.TableID{Schema: "test", Table: "large_table"}
	tblKey := tbl.String()

	// Bước 1: Thêm một vài transaction nhỏ trước để map rows có dữ liệu
	smallTxn1 := &model.Txn{
		Rows: []model.RowChange{
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{1}},
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{2}},
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{3}},
		},
	}
	_ = sched.Admit(ctx, smallTxn1)
	readySmall1 := <-sched.Ready()
	if readySmall1.Seq() != smallTxn1.Seq() {
		t.Fatalf("kỳ vọng smallTxn1 ready ngay, got %d", readySmall1.Seq())
	}
	st := sched.table(tblKey)
	if len(st.rows) != 3 {
		t.Fatalf("kỳ vọng 3 rows trong map, got %d", len(st.rows))
	}

	// Bước 2: Tạo transaction lớn với 50.001 khóa (> 50.000)
	numKeys := 50001
	bigRows := make([]model.RowChange, numKeys)
	for i := 0; i < numKeys; i++ {
		bigRows[i] = model.RowChange{
			Table: tbl,
			Op:    model.OpInsert,
			Key:   []model.Value{1000 + i},
		}
	}
	bigTxn := &model.Txn{
		Rows: bigRows,
	}

	_ = sched.Admit(ctx, bigTxn)

	// Kiểm tra trạng thái cấp bảng:
	// 1. tableWriter phải được gán bằng seq của bigTxn
	if st.tableWriter != bigTxn.Seq() {
		t.Fatalf("kỳ vọng tableWriter=%d, got %d", bigTxn.Seq(), st.tableWriter)
	}

	// 2. Map rows của bảng PHẢI BỊ DỌN (len == 0) theo spec 02 §6
	if len(st.rows) != 0 {
		t.Fatalf("AC2 vi phạm: kỳ vọng map row được dọn sạch (len=0) sau khi nâng cấp bảng, actual len=%d", len(st.rows))
	}
	t.Logf("AC2: Map rows đã được dọn sạch về 0 sau khi chạm ngưỡng %d khóa", cfg.TableLevelThreshold)

	// Bước 3: Một transaction nhỏ tiếp theo sau bigTxn PHẢI phụ thuộc vào bigTxn
	smallTxn2 := &model.Txn{
		Rows: []model.RowChange{
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{9999999}},
		},
	}
	_ = sched.Admit(ctx, smallTxn2)

	// smallTxn2 không được ready trước khi bigTxn Done
	sched.Done(smallTxn1.Seq(), nil) // smallTxn1 xong
	// Lúc này bigTxn phải ready
	readyBig := <-sched.Ready()
	if readyBig.Seq() != bigTxn.Seq() {
		t.Fatalf("kỳ vọng bigTxn ready, got %d", readyBig.Seq())
	}

	select {
	case r := <-sched.Ready():
		t.Fatalf("smallTxn2 không được ready khi bigTxn chưa Done, got %d", r.Seq())
	case <-time.After(100 * time.Millisecond):
		// Đúng kỳ vọng
	}

	// Báo bigTxn Done -> smallTxn2 được emit
	sched.Done(bigTxn.Seq(), nil)
	readySmall := <-sched.Ready()
	if readySmall.Seq() != smallTxn2.Seq() {
		t.Fatalf("kỳ vọng smallTxn2 ready sau khi bigTxn Done, got %d", readySmall.Seq())
	}
	sched.Done(smallTxn2.Seq(), nil)

	t.Log("AC2 PASS: Txn vượt 50.000 khóa được nâng thành khóa cấp bảng; map row được dọn dẹp thành công")
}

// invariant: CP-4
// AC3: holes > 4.096 thì scheduler ngừng nhận (TestCP4).
func TestCP4_SchedulerStopsOnMaxHoles(t *testing.T) {
	for _, limit := range []int{1, 5, DefaultConfig().MaxHoles} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.MaxHoles = limit
			s := NewScheduler(cfg)
			defer s.Close()
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				for {
					select {
					case <-stop:
						return
					case <-s.Ready():
					}
				}
			}()
			for i := 0; i < limit+1; i++ {
				if err := s.Admit(context.Background(), &model.Txn{}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := s.Admit(ctx, &model.Txn{}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unreserved admission: %v", err)
			}
			s.Done(uint64(limit+1), nil)
			if s.Holes() != limit {
				t.Fatalf("holes=%d want %d", s.Holes(), limit)
			}
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			defer cancel2()
			if err := s.Admit(ctx2, &model.Txn{}); err != nil {
				t.Fatal(err)
			}
			s.Done(uint64(limit+2), nil)
			if s.Holes() != limit {
				t.Fatalf("holes=%d want %d", s.Holes(), limit)
			}
			for i := 1; i <= limit; i++ {
				s.Done(uint64(i), nil)
			}
			if s.Holes() != 0 || s.Lwd() != uint64(limit+2) {
				t.Fatalf("holes=%d lwd=%d", s.Holes(), s.Lwd())
			}
		})
	}
}

// invariant: CO-1
// AC1: writeset gồm PK, unique key, FK (TestCO1_FullWriteset_PK_UK_FK).
func TestCO1_FullWriteset_PK_UK_FK(t *testing.T) {
	ctx := context.Background()

	t.Run("PK_And_OldKey", func(t *testing.T) {
		sched := NewScheduler(DefaultConfig())
		defer sched.Close()

		tbl := model.TableID{Schema: "test", Table: "orders"}

		// Txn 1 và Txn 2: cùng PK [100]
		t1 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpUpdate, Key: []model.Value{100}},
			},
		}
		t2 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpUpdate, Key: []model.Value{100}},
			},
		}

		if err := sched.Admit(ctx, t1); err != nil {
			t.Fatalf("admit t1: %v", err)
		}
		if err := sched.Admit(ctx, t2); err != nil {
			t.Fatalf("admit t2: %v", err)
		}

		// t1 ready ngay, t2 bị chặn
		select {
		case ready := <-sched.Ready():
			if ready.Seq() != t1.Seq() {
				t.Fatalf("kỳ vọng t1 ready trước, got %d", ready.Seq())
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout chờ t1 ready")
		}

		select {
		case r := <-sched.Ready():
			t.Fatalf("t2 không được ready khi t1 chưa Done, got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t1.Seq(), nil)

		select {
		case ready := <-sched.Ready():
			if ready.Seq() != t2.Seq() {
				t.Fatalf("kỳ vọng t2 ready sau khi t1 Done, got %d", ready.Seq())
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout chờ t2 ready")
		}
		sched.Done(t2.Seq(), nil)

		// UPDATE đổi PK: Txn 3 đổi PK từ 200 sang 201
		t3 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpUpdate, Key: []model.Value{201}, OldKey: []model.Value{200}},
			},
		}
		// Txn 4 chạm PK cũ [200]
		t4 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpUpdate, Key: []model.Value{200}},
			},
		}
		// Txn 5 chạm PK mới [201]
		t5 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpUpdate, Key: []model.Value{201}},
			},
		}

		_ = sched.Admit(ctx, t3)
		_ = sched.Admit(ctx, t4)
		_ = sched.Admit(ctx, t5)

		r3 := <-sched.Ready()
		if r3.Seq() != t3.Seq() {
			t.Fatalf("kỳ vọng t3 ready, got %d", r3.Seq())
		}

		// Cả t4 và t5 đều phải bị chặn bởi t3
		select {
		case r := <-sched.Ready():
			t.Fatalf("t4/t5 không được ready khi t3 chưa Done, got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t3.Seq(), nil)

		// Sau khi t3 Done, t4 và t5 không phụ thuộc lẫn nhau nên cả 2 đều ready
		readyMap := make(map[uint64]bool)
		for i := 0; i < 2; i++ {
			select {
			case r := <-sched.Ready():
				readyMap[r.Seq()] = true
				sched.Done(r.Seq(), nil)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("timeout chờ t4/t5 ready")
			}
		}
		if !readyMap[t4.Seq()] || !readyMap[t5.Seq()] {
			t.Fatalf("kỳ vọng cả t4 và t5 đều ready sau khi t3 Done, got %v", readyMap)
		}
	})

	t.Run("UniqueKey_And_OldUniqueKey", func(t *testing.T) {
		sched := NewScheduler(DefaultConfig())
		defer sched.Close()

		tblUsers := model.TableID{Schema: "test", Table: "users"}

		// Txn 6: PK [1], UniqueKey email="alice@test.com"
		t6 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblUsers,
					Op:    model.OpInsert,
					Key:   []model.Value{1},
					UniqueKeys: []model.UniqueKey{
						{Name: "uk_email", Key: []model.Value{"alice@test.com"}},
					},
				},
			},
		}
		// Txn 7: PK [2] (khác PK), cùng UniqueKey email="alice@test.com"
		t7 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblUsers,
					Op:    model.OpInsert,
					Key:   []model.Value{2},
					UniqueKeys: []model.UniqueKey{
						{Name: "uk_email", Key: []model.Value{"alice@test.com"}},
					},
				},
			},
		}

		_ = sched.Admit(ctx, t6)
		_ = sched.Admit(ctx, t7)

		r6 := <-sched.Ready()
		if r6.Seq() != t6.Seq() {
			t.Fatalf("kỳ vọng t6 ready trước, got %d", r6.Seq())
		}

		// t7 phải bị chặn vì trùng unique key
		select {
		case r := <-sched.Ready():
			t.Fatalf("vi phạm CO-1: t7 ready khi t6 chưa Done (trùng unique key), got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t6.Seq(), nil)

		r7 := <-sched.Ready()
		if r7.Seq() != t7.Seq() {
			t.Fatalf("kỳ vọng t7 ready sau khi t6 Done, got %d", r7.Seq())
		}
		sched.Done(t7.Seq(), nil)

		// UPDATE đổi UK: t8 đổi email từ bob_old@test.com sang bob_new@test.com
		t8 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblUsers,
					Op:    model.OpUpdate,
					Key:   []model.Value{3},
					UniqueKeys: []model.UniqueKey{
						{
							Name:   "uk_email",
							Key:    []model.Value{"bob_new@test.com"},
							OldKey: []model.Value{"bob_old@test.com"},
						},
					},
				},
			},
		}
		// t9 chạm email cũ bob_old@test.com
		t9 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblUsers,
					Op:    model.OpInsert,
					Key:   []model.Value{4},
					UniqueKeys: []model.UniqueKey{
						{Name: "uk_email", Key: []model.Value{"bob_old@test.com"}},
					},
				},
			},
		}

		_ = sched.Admit(ctx, t8)
		_ = sched.Admit(ctx, t9)

		r8 := <-sched.Ready()
		if r8.Seq() != t8.Seq() {
			t.Fatalf("kỳ vọng t8 ready trước, got %d", r8.Seq())
		}

		// t9 phải chờ t8 vì chạm OldKey của UniqueKey
		select {
		case r := <-sched.Ready():
			t.Fatalf("t9 không được ready khi t8 chưa Done (chạm old unique key), got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t8.Seq(), nil)

		r9 := <-sched.Ready()
		if r9.Seq() != t9.Seq() {
			t.Fatalf("kỳ vọng t9 ready sau khi t8 Done, got %d", r9.Seq())
		}
		sched.Done(t9.Seq(), nil)
	})

	t.Run("ForeignKey_ParentChildOrdering", func(t *testing.T) {
		sched := NewScheduler(DefaultConfig())
		defer sched.Close()

		tblDept := model.TableID{Schema: "test", Table: "departments"}
		tblEmp := model.TableID{Schema: "test", Table: "employees"}

		// Txn 10: insert department id=10 (bảng cha)
		t10 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblDept,
					Op:    model.OpInsert,
					Key:   []model.Value{10},
				},
			},
		}

		// Txn 11: insert employee id=1 có FK tham chiếu dept_id=10 (bảng con)
		t11 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblEmp,
					Op:    model.OpInsert,
					Key:   []model.Value{1},
					ForeignKeys: []model.ForeignKey{
						{
							ParentTable: tblDept,
							ParentKey:   []model.Value{10},
						},
					},
				},
			},
		}

		_ = sched.Admit(ctx, t10)
		_ = sched.Admit(ctx, t11)

		r10 := <-sched.Ready()
		if r10.Seq() != t10.Seq() {
			t.Fatalf("kỳ vọng t10 (bảng cha) ready trước, got %d", r10.Seq())
		}

		// t11 (bảng con) KHÔNG ĐƯỢC ready trước khi t10 Done
		select {
		case r := <-sched.Ready():
			t.Fatalf("vi phạm CO-1: row con (t11) ready trước khi row cha (t10) Done, got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t10.Seq(), nil)

		r11 := <-sched.Ready()
		if r11.Seq() != t11.Seq() {
			t.Fatalf("kỳ vọng t11 ready sau khi t10 Done, got %d", r11.Seq())
		}

		// Txn 12: update bảng cha dept_id=10
		t12 := &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tblDept,
					Op:    model.OpUpdate,
					Key:   []model.Value{10},
				},
			},
		}
		_ = sched.Admit(ctx, t12)

		// t12 phải chờ t11 vì t11 đang tham chiếu dept_id=10
		select {
		case r := <-sched.Ready():
			t.Fatalf("t12 không được ready khi t11 chưa Done, got %d", r.Seq())
		case <-time.After(50 * time.Millisecond):
		}

		sched.Done(t11.Seq(), nil)

		r12 := <-sched.Ready()
		if r12.Seq() != t12.Seq() {
			t.Fatalf("kỳ vọng t12 ready sau khi t11 Done, got %d", r12.Seq())
		}
		sched.Done(t12.Seq(), nil)
	})
}

// invariant: CO-2
// AC2: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng (TestCO2_WritesetBudgetTableLevel).
func TestCO2_WritesetBudgetTableLevel(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	cfg.TableLevelThreshold = 50000
	cfg.WritesetBudget = 1000

	sched := NewScheduler(cfg)
	defer sched.Close()

	tblA := model.TableID{Schema: "test", Table: "table_a"}
	tblB := model.TableID{Schema: "test", Table: "table_b"}

	// Bước 1: Nạp 400 khóa vào table_b (dưới budget 1000)
	txnB := &model.Txn{}
	for i := 0; i < 400; i++ {
		txnB.Rows = append(txnB.Rows, model.RowChange{
			Table: tblB,
			Op:    model.OpInsert,
			Key:   []model.Value{i},
		})
	}
	if err := sched.Admit(ctx, txnB); err != nil {
		t.Fatalf("admit txnB: %v", err)
	}
	<-sched.Ready()

	if sched.TableWritesetEntries("test.table_b") != 400 {
		t.Fatalf("kỳ vọng table_b có 400 entries, got %d", sched.TableWritesetEntries("test.table_b"))
	}
	if sched.IsForcedTableLevel("test.table_b") {
		t.Fatal("table_b không được ở chế độ forcedTableLevel")
	}

	// Bước 2: Nạp 500 khóa vào table_a (tổng cộng 900 <= 1000)
	txnA1 := &model.Txn{}
	for i := 0; i < 500; i++ {
		txnA1.Rows = append(txnA1.Rows, model.RowChange{
			Table: tblA,
			Op:    model.OpInsert,
			Key:   []model.Value{i},
		})
	}
	if err := sched.Admit(ctx, txnA1); err != nil {
		t.Fatalf("admit txnA1: %v", err)
	}
	<-sched.Ready()

	if sched.TotalWritesetEntries() != 900 {
		t.Fatalf("kỳ vọng totalRows == 900, got %d", sched.TotalWritesetEntries())
	}
	if sched.IsForcedTableLevel("test.table_a") {
		t.Fatal("table_a chưa được forcedTableLevel khi totalRows <= 1000")
	}

	// Bước 3: Nạp thêm 200 khóa vào table_a -> tổng sẽ là 400 + 500 + 200 = 1100 > 1000 (vượt writeset_budget)
	// Bảng lớn nhất là table_a (500 + 200 = 700 vs 400 của table_b).
	// Do đó table_a phải chuyển sang khóa cấp bảng và map rows của table_a được dọn về 0.
	txnA2 := &model.Txn{}
	for i := 500; i < 700; i++ {
		txnA2.Rows = append(txnA2.Rows, model.RowChange{
			Table: tblA,
			Op:    model.OpInsert,
			Key:   []model.Value{i},
		})
	}
	if err := sched.Admit(ctx, txnA2); err != nil {
		t.Fatalf("admit txnA2: %v", err)
	}

	// Kiểm tra bảng table_a đã chuyển sang forcedTableLevel
	if !sched.IsForcedTableLevel("test.table_a") {
		t.Fatal("AC2 vi phạm: table_a (bảng lớn nhất) phải chuyển sang forcedTableLevel khi vượt writeset_budget")
	}
	// Map row của table_a phải được dọn về 0 (spec 02 §6)
	if sched.TableWritesetEntries("test.table_a") != 0 {
		t.Fatalf("AC2 vi phạm: map rows của table_a phải được dọn về 0, got %d", sched.TableWritesetEntries("test.table_a"))
	}
	// table_b không bị ảnh hưởng (vẫn còn nguyên 400 khóa)
	if sched.TableWritesetEntries("test.table_b") != 400 {
		t.Fatalf("table_b phải giữ nguyên 400 entries, got %d", sched.TableWritesetEntries("test.table_b"))
	}
	if sched.IsForcedTableLevel("test.table_b") {
		t.Fatal("table_b không được chuyển sang cấp bảng vì không phải bảng lớn nhất")
	}

	// Vì txnA2 là cấp bảng, nó phải phụ thuộc txnA1 (đang active trên table_a)
	select {
	case r := <-sched.Ready():
		t.Fatalf("txnA2 không được ready khi txnA1 chưa Done, got %d", r.Seq())
	case <-time.After(50 * time.Millisecond):
	}

	// Bước 4: Txn tiếp theo trên table_a (dù chỉ có 1 row) cũng phải là cấp bảng và phụ thuộc txnA2
	txnA3 := &model.Txn{
		Rows: []model.RowChange{
			{Table: tblA, Op: model.OpInsert, Key: []model.Value{99999}},
		},
	}
	_ = sched.Admit(ctx, txnA3)

	// Hoàn thành txnA1 -> txnA2 ready
	sched.Done(txnA1.Seq(), nil)
	rA2 := <-sched.Ready()
	if rA2.Seq() != txnA2.Seq() {
		t.Fatalf("kỳ vọng txnA2 ready, got %d", rA2.Seq())
	}

	// txnA3 vẫn phải chờ txnA2
	select {
	case r := <-sched.Ready():
		t.Fatalf("txnA3 không được ready khi txnA2 chưa Done, got %d", r.Seq())
	case <-time.After(50 * time.Millisecond):
	}

	sched.Done(txnA2.Seq(), nil)
	rA3 := <-sched.Ready()
	if rA3.Seq() != txnA3.Seq() {
		t.Fatalf("kỳ vọng txnA3 ready, got %d", rA3.Seq())
	}
	sched.Done(txnA3.Seq(), nil)
	sched.Done(txnB.Seq(), nil)

	t.Log("AC2 TestCO2 PASS: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng thành công")
}

// invariant: CO-2
// AC3: 100 Txn × 49.999 khóa không OOM (TestCO2_100Txn_49999Keys_NoOOM).
func TestCO2_100Txn_49999Keys_NoOOM(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig() // TableLevelThreshold: 50.000, WritesetBudget: 2.000.000, MaxHoles: 4.096
	sched := NewScheduler(cfg)
	defer sched.Close()

	numTxns := 100
	keysPerTxn := 49999 // < 50.000 (TableLevelThreshold)

	tbl := model.TableID{Schema: "test", Table: "huge_table"}

	var completedCount int64
	var wg sync.WaitGroup
	numWorkers := 8

	// Worker pool drain ready channel và gọi Done
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for txn := range sched.Ready() {
				sched.Done(txn.Seq(), nil)
				atomic.AddInt64(&completedCount, 1)
			}
		}()
	}

	// Nạp tuần tự 100 transaction, mỗi transaction có 49.999 khóa
	for i := 0; i < numTxns; i++ {
		rows := make([]model.RowChange, keysPerTxn)
		startKey := i * keysPerTxn
		for k := 0; k < keysPerTxn; k++ {
			rows[k] = model.RowChange{
				Table: tbl,
				Op:    model.OpInsert,
				Key:   []model.Value{startKey + k},
			}
		}
		txn := &model.Txn{
			Rows: rows,
		}
		if err := sched.Admit(ctx, txn); err != nil {
			t.Fatalf("admit txn %d: %v", i, err)
		}
	}

	// Chờ hoàn thành đủ 100 Txn
	for {
		if atomic.LoadInt64(&completedCount) == int64(numTxns) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(sched.ReadyCh())
	wg.Wait()

	// Đo đạc mức chiếm dụng bộ nhớ thực tế
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)

	heapAllocMB := float64(m.HeapAlloc) / (1024 * 1024)
	sysMB := float64(m.Sys) / (1024 * 1024)

	t.Logf("AC3 TestCO2 PASS: Đã xử lý thành công %d Txns × %d keys = %d keys",
		numTxns, keysPerTxn, numTxns*keysPerTxn)
	t.Logf("HeapAlloc: %.2f MB, Sys: %.2f MB (hoàn toàn dưới giới hạn RAM, không OOM)", heapAllocMB, sysMB)

	// Đảm bảo HeapAlloc sau GC rất nhỏ (< 500 MB)
	if heapAllocMB > 500 {
		t.Fatalf("Cảnh báo rò rỉ RAM: HeapAlloc quá cao: %.2f MB > 500 MB", heapAllocMB)
	}
}

// Lost wakeup: readyCh và overflow đầy, sau đó consumer rút sạch readyCh (không Done). Admit kế tiếp phải
// tự rút overflow sang readyCh rồi nhận; trước đây nó chỉ chờ spaceCh (chỉ được đóng khi Done/Admit rút
// overflow) nên kẹt vĩnh viễn — test package treo tới timeout 60 phút khi máy tải nặng.
func TestAdmitDrainsOverflowWhenReadyEmptiedWithoutDone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxHoles = 1 << 20
	s := NewScheduler(cfg)
	defer s.Close()
	n := 2 * cap(s.readyCh) // readyCh + overflow đầy
	for i := 0; i < n; i++ {
		if err := s.Admit(context.Background(), &model.Txn{}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < cap(s.readyCh); i++ { // consumer rút readyCh, không Done
		<-s.Ready()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Admit(ctx, &model.Txn{}); err != nil {
		t.Fatalf("Admit with an emptied readyCh and a full overflow: %v", err)
	}
}

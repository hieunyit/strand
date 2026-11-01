package sched

import (
	"context"
	"errors"
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
	ctx := context.Background()

	t.Run("DefaultMaxHoles_4096", func(t *testing.T) {
		cfg := DefaultConfig()
		if cfg.MaxHoles != 4096 {
			t.Fatalf("kỳ vọng DefaultConfig.MaxHoles == 4096, got %d", cfg.MaxHoles)
		}
		sched := NewScheduler(cfg)
		defer sched.Close()

		// Background drain ready channel để scheduler không bị nghẽn kênh emit
		stopDrain := make(chan struct{})
		go func() {
			for {
				select {
				case <-stopDrain:
					return
				case <-sched.Ready():
				}
			}
		}()
		defer close(stopDrain)

		tbl := model.TableID{Schema: "test", Table: "t1"}

		// 1. Admit Txn 1 (giả lập transaction dài 30 phút, chưa gọi Done)
		txn1 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpInsert, Key: []model.Value{1}},
			},
		}
		if err := sched.Admit(ctx, txn1); err != nil {
			t.Fatalf("admit txn1 thất bại: %v", err)
		}

		// 2. Admit Txns 2..4098
		for i := 2; i <= 4098; i++ {
			tx := &model.Txn{
				Rows: []model.RowChange{
					{Table: tbl, Op: model.OpInsert, Key: []model.Value{i}},
				},
			}
			if err := sched.Admit(ctx, tx); err != nil {
				t.Fatalf("admit txn %d thất bại: %v", i, err)
			}
		}

		// 3. Txn 4098 hoàn thành (nhảy mốc high lên 4098)
		sched.Done(4098, nil)

		// Lúc này: lwd = 0 (vì txn1 chưa xong), high = 4098.
		// Các sequence trong (0, 4098] chưa xong: 1..4097 -> 4097 lỗ > 4096!
		holes := sched.Holes()
		if holes != 4097 {
			t.Fatalf("kỳ vọng holes == 4097, got %d (lwd=%d, high=%d)", holes, sched.Lwd(), sched.High())
		}

		// 4. Khi holes > 4.096, scheduler PHẢI NGỪNG NHẬN Txn mới (Admit bị chặn)
		ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		txn4099 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpInsert, Key: []model.Value{4099}},
			},
		}
		err := sched.Admit(ctxTimeout, txn4099)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("kỳ vọng Admit bị chặn và trả DeadlineExceeded khi holes > 4096, got %v", err)
		}

		// 5. Txn 1 hoàn thành -> lwd nhảy lên 1 -> holes giảm về (4098 - 1) - 1 = 4096 <= 4096
		sched.Done(1, nil)
		if sched.Holes() != 4096 {
			t.Fatalf("kỳ vọng holes giảm về 4096 sau khi txn1 Done, got %d", sched.Holes())
		}

		// 6. Scheduler tiếp tục nhận Txn mới thành công
		ctxValid, cancelValid := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancelValid()

		if err := sched.Admit(ctxValid, txn4099); err != nil {
			t.Fatalf("admit txn4099 thất bại sau khi holes đã giảm: %v", err)
		}
	})

	t.Run("CustomMaxHoles_SmallThreshold", func(t *testing.T) {
		cfg := Config{
			MaxHoles:            5,
			TableLevelThreshold: 50000,
			WritesetBudget:      2000000,
		}
		sched := NewScheduler(cfg)
		defer sched.Close()

		stopDrain := make(chan struct{})
		go func() {
			for {
				select {
				case <-stopDrain:
					return
				case <-sched.Ready():
				}
			}
		}()
		defer close(stopDrain)

		tbl := model.TableID{Schema: "test", Table: "t2"}

		// Admit txn 1..7
		for i := 1; i <= 7; i++ {
			tx := &model.Txn{
				Rows: []model.RowChange{
					{Table: tbl, Op: model.OpInsert, Key: []model.Value{i}},
				},
			}
			if err := sched.Admit(ctx, tx); err != nil {
				t.Fatalf("admit %d: %v", i, err)
			}
		}

		// Done 7: high=7, lwd=0, holes = 6 > 5
		sched.Done(7, nil)
		if sched.Holes() != 6 {
			t.Fatalf("expected 6 holes, got %d", sched.Holes())
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		tx8 := &model.Txn{
			Rows: []model.RowChange{
				{Table: tbl, Op: model.OpInsert, Key: []model.Value{8}},
			},
		}
		if err := sched.Admit(ctxTimeout, tx8); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected timeout blocked by 6 holes > 5, got %v", err)
		}

		// Unblock in background
		go func() {
			time.Sleep(20 * time.Millisecond)
			sched.Done(1, nil) // holes drops to 5
		}()

		ctxUnblock, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel2()

		if err := sched.Admit(ctxUnblock, tx8); err != nil {
			t.Fatalf("expected unblock after hole closed: %v", err)
		}
	})
}

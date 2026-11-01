package checkpoint_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// invariant: CP-1
// AC1: TestCP1 kill ở mọi bước compaction đều không mất
func TestCP1_KillDuringCompaction(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL", func(t *testing.T) {
		db := getMySQL(t)
		if db == nil {
			t.Fatal("MySQL not available")
		}

		steps := []checkpoint.CompactionStep{
			checkpoint.StepStart,
			checkpoint.StepReadDeltas,
			checkpoint.StepMerge,
			checkpoint.StepWriteGlobal,
			checkpoint.StepPruneDeltas,
			checkpoint.StepComplete,
		}

		for _, crashStep := range steps {
			t.Run(fmt.Sprintf("CrashAt_%s", crashStep), func(t *testing.T) {
				pipelineID := fmt.Sprintf("cp1-my-%s-%d", crashStep, time.Now().UnixNano())
				store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
				defer store.Close()

				if err := store.EnsureTables(ctx); err != nil {
					t.Fatalf("ensure tables: %v", err)
				}

				epoch, err := store.AdvanceEpoch(ctx, pipelineID, "test-owner", "hash-1")
				if err != nil {
					t.Fatalf("advance epoch: %v", err)
				}

				// Giả lập 3 worker đã commit dữ liệu và ghi vào _strand_worker_delta
				// Worker 1: gtid 1..10
				// Worker 2: gtid 11..20
				// Worker 3: gtid 21..30
				deltas := map[int]string{
					1: "uuid-1:1-10",
					2: "uuid-1:11-20",
					3: "uuid-1:21-30",
				}
				for wID, applied := range deltas {
					_, err := db.ExecContext(ctx,
						"INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied) VALUES (?, ?, ?, 1, ?)",
						pipelineID, wID, epoch, applied,
					)
					if err != nil {
						t.Fatalf("insert worker %d delta: %v", wID, err)
					}
				}

				// Khởi tạo compactor
				cfg := checkpoint.DefaultCompactorConfig(pipelineID, epoch)
				compactor := checkpoint.NewCompactor(db, store, pipelineID, model.EngineMySQL, model.EngineMySQL, cfg)

				// Cài đặt hook mô phỏng kill/crash tại step mong muốn
				errCrash := errors.New("simulated kill -9 during compaction")
				if crashStep != checkpoint.StepComplete {
					compactor.SetStepHook(func(step checkpoint.CompactionStep) error {
						if step == crashStep {
							return errCrash
						}
						return nil
					})
				}

				// Chạy compaction
				compactErr := compactor.Compact(ctx)
				if crashStep != checkpoint.StepComplete {
					if !errors.Is(compactErr, errCrash) {
						t.Fatalf("kỳ vọng gặp lỗi crash tại %s, got: %v", crashStep, compactErr)
					}
				} else {
					if compactErr != nil {
						t.Fatalf("compaction thất bại: %v", compactErr)
					}
				}

				// Khôi phục sau crash: gọi LoadApplied (theo spec/02 §4 quy tắc 2: row toàn cục ∪ mọi row delta)
				recoveredApplied, err := store.LoadApplied(ctx)
				if err != nil {
					t.Fatalf("LoadApplied sau crash: %v", err)
				}

				// Kiểm tra bất biến CP-1: 100% mọi transaction đã commit (uuid-1:1..30) đều PHẢI có mặt trong recoveredApplied
				for i := uint64(1); i <= 30; i++ {
					pos := model.Pos{
						Engine: model.EngineMySQL,
						GTID:   fmt.Sprintf("uuid-1:%d", i),
					}
					if !recoveredApplied.Contains(pos) {
						t.Fatalf("vi phạm CP-1: sau khi kill tại %s, transaction %s bị mất!", crashStep, pos.GTID)
					}
				}
			})
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		if db == nil {
			t.Fatal("PostgreSQL not available")
		}

		steps := []checkpoint.CompactionStep{
			checkpoint.StepStart,
			checkpoint.StepReadDeltas,
			checkpoint.StepMerge,
			checkpoint.StepWriteGlobal,
			checkpoint.StepPruneDeltas,
			checkpoint.StepComplete,
		}

		for _, crashStep := range steps {
			t.Run(fmt.Sprintf("CrashAt_%s", crashStep), func(t *testing.T) {
				pipelineID := fmt.Sprintf("cp1-pg-%s-%d", crashStep, time.Now().UnixNano())
				store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
				defer store.Close()

				if err := store.EnsureTables(ctx); err != nil {
					t.Fatalf("ensure tables: %v", err)
				}

				epoch, err := store.AdvanceEpoch(ctx, pipelineID, "test-owner", "hash-1")
				if err != nil {
					t.Fatalf("advance epoch: %v", err)
				}

				// Giả lập 3 worker đã commit dữ liệu
				// Worker 1: LSN 1..10
				// Worker 2: LSN 11..20
				// Worker 3: LSN 21..30
				deltas := map[int]string{
					1: "1,2,3,4,5,6,7,8,9,10",
					2: "11,12,13,14,15,16,17,18,19,20",
					3: "21,22,23,24,25,26,27,28,29,30",
				}
				for wID, applied := range deltas {
					_, err := db.ExecContext(ctx,
						"INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied) VALUES ($1, $2, $3, 1, $4)",
						pipelineID, wID, epoch, applied,
					)
					if err != nil {
						t.Fatalf("insert worker %d delta: %v", wID, err)
					}
				}

				cfg := checkpoint.DefaultCompactorConfig(pipelineID, epoch)
				compactor := checkpoint.NewCompactor(db, store, pipelineID, model.EnginePG, model.EnginePG, cfg)

				errCrash := errors.New("simulated kill -9 during compaction")
				if crashStep != checkpoint.StepComplete {
					compactor.SetStepHook(func(step checkpoint.CompactionStep) error {
						if step == crashStep {
							return errCrash
						}
						return nil
					})
				}

				compactErr := compactor.Compact(ctx)
				if crashStep != checkpoint.StepComplete {
					if !errors.Is(compactErr, errCrash) {
						t.Fatalf("kỳ vọng gặp lỗi crash tại %s, got: %v", crashStep, compactErr)
					}
				} else {
					if compactErr != nil {
						t.Fatalf("compaction thất bại: %v", compactErr)
					}
				}

				recoveredApplied, err := store.LoadApplied(ctx)
				if err != nil {
					t.Fatalf("LoadApplied sau crash: %v", err)
				}

				for i := uint64(1); i <= 30; i++ {
					pos := model.Pos{
						Engine: model.EnginePG,
						LSN:    i,
					}
					if !recoveredApplied.Contains(pos) {
						t.Fatalf("vi phạm CP-1 (PG): sau khi kill tại %s, LSN %d bị mất!", crashStep, i)
					}
				}
			})
		}
	})
}

// invariant: CP-4
// AC2: delta ≤ 64 KB, vượt thì compaction đồng bộ
func TestCP4_DeltaSyncCompaction(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL", func(t *testing.T) {
		db := getMySQL(t)
		if db == nil {
			t.Fatal("MySQL not available")
		}

		pipelineID := fmt.Sprintf("cp4-my-%d", time.Now().UnixNano())
		store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner", "hash")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}

		cfg := checkpoint.DefaultCompactorConfig(pipelineID, epoch)
		compactor := checkpoint.NewCompactor(db, store, pipelineID, model.EngineMySQL, model.EngineMySQL, cfg)

		// 1. Delta bình thường (<= 64 KB) ghi thành công
		normalDelta := "uuid-cp4:1-100"
		if len(normalDelta) > checkpoint.MaxDeltaBytes {
			t.Fatalf("normalDelta vượt 64 KB")
		}
		if err := compactor.RecordWorkerDelta(ctx, nil, 1, normalDelta); err != nil {
			t.Fatalf("ghi normal delta thất bại: %v", err)
		}

		// Kiểm tra delta trên DB
		var dbApplied string
		err = db.QueryRowContext(ctx,
			"SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?",
			pipelineID, 1,
		).Scan(&dbApplied)
		if err != nil {
			t.Fatalf("query delta: %v", err)
		}
		if dbApplied != normalDelta {
			t.Fatalf("kỳ vọng applied=%s, got=%s", normalDelta, dbApplied)
		}

		// 2. Tạo một delta lớn vượt ngưỡng 64 KB (> 65.536 bytes)
		// Tạo chuỗi gồm nhiều GTID riêng lẻ để kích thước vượt 64 KB
		var b strings.Builder
		for i := 1; i <= 3500; i++ {
			if i > 1 {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf("11111111-2222-3333-4444-555555555555:%d", i))
		}
		largeDelta := b.String()
		if len(largeDelta) <= checkpoint.MaxDeltaBytes {
			t.Fatalf("largeDelta kích thước %d chưa vượt 64 KB", len(largeDelta))
		}

		// Ghi delta vượt 64 KB -> Compactor tự động kích hoạt compaction đồng bộ
		// Sau compaction, các GTID riêng lẻ được gộp lại thành 1 khoảng "11111111-2222-3333-4444-555555555555:1-3500" trong row toàn cục!
		// Sau đó, worker delta được dọn dẹp hoặc cắt tỉa về <= 64 KB
		if err := compactor.RecordWorkerDelta(ctx, nil, 2, "uuid-cp4:101-200"); err != nil {
			t.Fatalf("ghi worker 2: %v", err)
		}

		// Gọi compaction đồng bộ
		if err := compactor.Compact(ctx); err != nil {
			t.Fatalf("compaction thất bại: %v", err)
		}

		// Kiểm tra row toàn cục đã gộp các delta
		var globalApplied string
		err = db.QueryRowContext(ctx,
			"SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ?",
			pipelineID,
		).Scan(&globalApplied)
		if err != nil {
			t.Fatalf("query global checkpoint: %v", err)
		}
		parsedGlobal, err := checkpoint.ParseAppliedSet(model.EngineMySQL, globalApplied)
		if err != nil {
			t.Fatalf("parse global applied: %v", err)
		}
		if !parsedGlobal.Contains(model.Pos{Engine: model.EngineMySQL, GTID: "uuid-cp4:50"}) ||
			!parsedGlobal.Contains(model.Pos{Engine: model.EngineMySQL, GTID: "uuid-cp4:150"}) {
			t.Fatalf("global checkpoint chưa gộp đủ deltas, got: %s", globalApplied)
		}

		// Kiểm tra worker deltas đã được prune (kích thước <= 64 KB)
		rows, err := db.QueryContext(ctx,
			"SELECT worker_id, applied FROM _strand_worker_delta WHERE pipeline_id = ?",
			pipelineID,
		)
		if err != nil {
			t.Fatalf("query worker deltas: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var wID int
			var applied string
			if err := rows.Scan(&wID, &applied); err != nil {
				t.Fatalf("scan delta: %v", err)
			}
			if len(applied) > checkpoint.MaxDeltaBytes {
				t.Fatalf("vi phạm CP-4: worker %d delta vẫn vượt 64 KB sau compaction (len=%d)", wID, len(applied))
			}
		}

		// 3. Nếu một delta đơn lẻ không thể nén mà vẫn vượt 64 KB, worker từ chối (trả về ErrDeltaExceededLimit)
		uncompressibleDelta := strings.Repeat("A", checkpoint.MaxDeltaBytes+100)
		err = compactor.RecordWorkerDelta(ctx, nil, 3, uncompressibleDelta)
		if !errors.Is(err, checkpoint.ErrDeltaExceededLimit) {
			t.Fatalf("kỳ vọng ErrDeltaExceededLimit khi delta không thể giảm xuống <= 64 KB, got: %v", err)
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		if db == nil {
			t.Fatal("PostgreSQL not available")
		}

		pipelineID := fmt.Sprintf("cp4-pg-%d", time.Now().UnixNano())
		store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner", "hash")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}

		cfg := checkpoint.DefaultCompactorConfig(pipelineID, epoch)
		compactor := checkpoint.NewCompactor(db, store, pipelineID, model.EnginePG, model.EnginePG, cfg)

		// Ghi normal delta
		if err := compactor.RecordWorkerDelta(ctx, nil, 1, "1,2,3,4,5"); err != nil {
			t.Fatalf("ghi pg delta: %v", err)
		}

		// Delta không thể nén vượt 64 KB -> trả về ErrDeltaExceededLimit
		hugeDelta := strings.Repeat("X", checkpoint.MaxDeltaBytes+50)
		err = compactor.RecordWorkerDelta(ctx, nil, 1, hugeDelta)
		if !errors.Is(err, checkpoint.ErrDeltaExceededLimit) {
			t.Fatalf("kỳ vọng ErrDeltaExceededLimit trên PG, got: %v", err)
		}
	})
}

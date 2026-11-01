// Package seed hiện thực prefetch, archive catch-up và quản lý spool tràn theo spec/02 §9 và spec/design.md §1, §3.
package seed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
	"strand/internal/spool"
)

// ApplyConfig cấu hình cho quá trình apply seed kèm prefetch và catch-up.
// spec: 02 §9
type ApplyConfig struct {
	// PrefetchEnabled mặc định là true (spec: 02 §9: Prefetch, mặc định bật).
	PrefetchEnabled bool

	// Spool hàng đợi lưu trữ transaction (hỗ trợ phân tầng local và bucket).
	Spool spool.Spool

	// ArchiveReader đọc binlog từ file lưu trữ archive.
	ArchiveReader reader.Reader

	// SourceReader đọc tiếp binlog từ replica / primary khi archive đã hết.
	SourceReader reader.Reader

	// Assembler lắp ráp các event từ reader thành Txn hoàn chỉnh.
	Assembler *assemble.Assembler

	// OnRestore callback thực hiện restore dữ liệu từ backup (chạy song song hoặc giữa chừng).
	OnRestore func(ctx context.Context) error

	// OnAppliedTxn callback ghi nhận transaction được apply trong giai đoạn catch-up.
	OnAppliedTxn func(t *model.Txn) error
}

// ApplyResult tổng hợp kết quả của quá trình apply seed.
type ApplyResult struct {
	PrefetchedTxns int
	AppliedTxns    int
	SpoolUsage     spool.SpoolUsage
}

// ApplySeed thực hiện toàn bộ quy trình apply seed manifest:
// 1. Nếu PrefetchEnabled=true: khởi chạy prefetch đọc từ P vào spool (archive trước, source sau).
// 2. Chạy OnRestore (nếu có).
// 3. Tiến hành Catch-up:
//    - Nếu prefetch bật: catch-up đọc từ Spool (kể cả khi archive bị xóa giữa chừng).
//    - Nếu prefetch tắt: catch-up đọc trực tiếp từ Archive/Source; nếu thiếu log thì báo log_gap (exit 5).
// 4. Nếu bất kỳ tầng spool nào chạm hard limit: báo prefetch_exhausted (exit 5).
func ApplySeed(ctx context.Context, m *SeedManifest, cfg ApplyConfig) (*ApplyResult, error) {
	if m == nil {
		return nil, errors.New("seed manifest is nil")
	}

	startPos := model.Pos{
		Engine: model.EngineMySQL,
		GTID:   m.StartPosition.GTIDSet,
	}
	if m.Engine == "pg" {
		startPos.Engine = model.EnginePG
	}

	res := &ApplyResult{}

	if cfg.Assembler == nil {
		cfg.Assembler = assemble.New()
	}

	// 1. Trường hợp TẮT Prefetch (PrefetchEnabled == false)
	if !cfg.PrefetchEnabled {
		// Chạy restore dữ liệu trước
		if cfg.OnRestore != nil {
			if err := cfg.OnRestore(ctx); err != nil {
				return nil, fmt.Errorf("restore failed: %w", err)
			}
		}

		// Sau restore mới bắt đầu đọc archive log:
		// Nếu archive bị xóa hoặc thiếu log ở vị trí startPos -> báo log_gap ngay lập tức
		if cfg.ArchiveReader == nil {
			return nil, exitcode.New(
				exitcode.ErrLogGap,
				exitcode.DataError,
				"no archive reader provided and prefetch is disabled",
				nil,
			)
		}

		events, err := cfg.ArchiveReader.Start(ctx, nil)
		if err != nil {
			// Nếu lỗi mở archive hoặc không tìm thấy file
			var ecErr *exitcode.Error
			if errors.As(err, &ecErr) && ecErr.Code == exitcode.ErrLogGap {
				return nil, err
			}
			return nil, exitcode.New(
				exitcode.ErrLogGap,
				exitcode.DataError,
				fmt.Sprintf("log gap: cannot read archive log from %v: %v", startPos, err),
				err,
			)
		}

		// Đọc trực tiếp các event từ archive để catch-up
		for ev := range events {
			if ev.Type == reader.EventError {
				return nil, exitcode.New(
					exitcode.ErrLogGap,
					exitcode.DataError,
					fmt.Sprintf("log gap during direct catch-up: %v", ev.Err),
					ev.Err,
				)
			}

			txn, err := cfg.Assembler.Push(ev)
			if err != nil {
				return nil, fmt.Errorf("assemble error: %w", err)
			}
			if txn != nil {
				if cfg.OnAppliedTxn != nil {
					if err := cfg.OnAppliedTxn(txn); err != nil {
						return nil, err
					}
				}
				res.AppliedTxns++
			}
		}

		return res, nil
	}

	// 2. Trường hợp BẬT Prefetch (Mặc định)
	// Đọc từ P vào spool song song với restore
	if cfg.Spool == nil {
		return nil, errors.New("spool is required when prefetch is enabled")
	}

	var prefetchErr error
	var wg sync.WaitGroup
	ctxPrefetch, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancelPrefetch()

		if cfg.ArchiveReader != nil {
			events, err := cfg.ArchiveReader.Start(ctxPrefetch, nil)
			if err != nil {
				prefetchErr = err
				return
			}

			for ev := range events {
				if ev.Type == reader.EventError {
					prefetchErr = ev.Err
					return
				}

				txn, err := cfg.Assembler.Push(ev)
				if err != nil {
					prefetchErr = err
					return
				}
				if txn != nil {
					if appErr := cfg.Spool.Append(ctxPrefetch, txn); appErr != nil {
						// Nếu spool đầy (cả local và bucket), báo prefetch_exhausted
						prefetchErr = appErr
						return
					}
					res.PrefetchedTxns++
				}
			}
		}

		// Đọc tiếp từ source/replica nếu có
		if cfg.SourceReader != nil && prefetchErr == nil {
			events, err := cfg.SourceReader.Start(ctxPrefetch, nil)
			if err != nil {
				prefetchErr = err
				return
			}
			for ev := range events {
				if ev.Type == reader.EventError {
					prefetchErr = ev.Err
					return
				}
				txn, err := cfg.Assembler.Push(ev)
				if err != nil {
					prefetchErr = err
					return
				}
				if txn != nil {
					if appErr := cfg.Spool.Append(ctxPrefetch, txn); appErr != nil {
						prefetchErr = appErr
						return
					}
					res.PrefetchedTxns++
				}
			}
		}
	}()

	// Trong lúc prefetch đang đọc, thực hiện restore dữ liệu
	if cfg.OnRestore != nil {
		if err := cfg.OnRestore(ctx); err != nil {
			return nil, fmt.Errorf("restore callback failed: %w", err)
		}
	}

	// Đợi prefetch hoàn tất phần đọc archive/log ban đầu
	wg.Wait()

	if prefetchErr != nil {
		var ecErr *exitcode.Error
		if errors.As(prefetchErr, &ecErr) {
			return nil, prefetchErr
		}
		return nil, fmt.Errorf("prefetch error: %w", prefetchErr)
	}

	res.SpoolUsage = cfg.Spool.Usage()

	// Tiến hành Catch-up: đọc toàn bộ transaction từ Spool
	iter, err := cfg.Spool.Iter(ctx, model.Pos{})
	if err != nil {
		return nil, fmt.Errorf("failed to open spool iter for catch-up: %w", err)
	}
	defer iter.Close()

	for {
		txn, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("iter error during catch-up: %w", err)
		}

		if cfg.OnAppliedTxn != nil {
			if err := cfg.OnAppliedTxn(txn); err != nil {
				return nil, fmt.Errorf("failed to apply txn during catch-up: %w", err)
			}
		}
		res.AppliedTxns++
	}

	return res, nil
}

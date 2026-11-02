package promote

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"strand/internal/exitcode"
)

// Promoter điều khiển quy trình 6 bước promote theo spec/02 §8 và spec/05 §5.
type Promoter struct {
	cfg          Config
	stateStore   StateStore
	sourceDriver SourceDriver
	targetDriver TargetDriver
	logger       *slog.Logger
	sequences    []SequenceInfo
}

// NewPromoter khởi tạo Promoter.
func NewPromoter(
	cfg Config,
	stateStore StateStore,
	sourceDriver SourceDriver,
	targetDriver TargetDriver,
	sequences []SequenceInfo,
	logger *slog.Logger,
) *Promoter {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.PeakRate <= 0 {
		cfg.PeakRate = 1000 // Mặc định r_peak
	}
	if cfg.SyncIntervalSec <= 0 {
		cfg.SyncIntervalSec = 5 // Mặc định Delta t_sync
	}
	if cfg.SequenceOverrides == nil {
		cfg.SequenceOverrides = make(map[string]int64)
	}
	return &Promoter{
		cfg:          cfg,
		stateStore:   stateStore,
		sourceDriver: sourceDriver,
		targetDriver: targetDriver,
		sequences:    sequences,
		logger:       logger,
	}
}

// suggestIndependentSequences ghi giá trị gợi ý (lần đồng bộ cuối + margin) cho các
// sequence độc lập chưa có giá trị do người vận hành chỉ định, để in ra cùng lúc dừng hỏi.
func (p *Promoter) suggestIndependentSequences(progress *Progress) {
	for i, seq := range progress.Sequences {
		if seq.Type != SequenceTypeIndependent {
			continue
		}
		if _, ok := p.cfg.SequenceOverrides[seq.Name]; ok {
			continue
		}
		margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
		suggested := seq.LastSyncedValue + margin
		progress.SuggestedSequences[seq.Name] = suggested
		progress.Sequences[i].SuggestedValue = suggested
	}
}

// Run thực thi quy trình promote 6 bước theo spec/02 §8:
// 1. Fence source cũ
// 2. Giữ target read-only
// 3. Apply nốt phần log còn lại
// 4. Chạy setval cho mọi sequence và identity
// 5. Mở quyền ghi
// 6. Chuyển DNS hoặc VIP
func (p *Promoter) Run(ctx context.Context) (*Progress, error) {
	// Nạp hoặc khởi tạo tiến trình promote
	progress, err := p.stateStore.LoadProgress(ctx, p.cfg.PipelineID)
	if err != nil {
		return nil, fmt.Errorf("load promote progress: %w", err)
	}

	if progress == nil {
		progress = &Progress{
			PipelineID:         p.cfg.PipelineID,
			State:              StatePromoting,
			CurrentStep:        StepFenceSource,
			CompletedSteps:     make([]PromoteStep, 0),
			Sequences:          p.sequences,
			SuggestedSequences: make(map[string]int64),
			UpdatedAt:          time.Now(),
		}
	} else {
		p.logger.Info("resuming promote from interrupted step",
			"pipeline", progress.PipelineID,
			"current_step", progress.CurrentStep,
			"completed_steps", progress.CompletedSteps,
		)
		// Cập nhật lại danh sách sequences nếu mới
		if len(progress.Sequences) == 0 {
			progress.Sequences = p.sequences
		}
		if progress.SuggestedSequences == nil {
			progress.SuggestedSequences = make(map[string]int64)
		}
		progress.State = StatePromoting
	}

	// Chế độ DryRun: chỉ in kế hoạch và kiểm tra, không thay đổi DB
	if p.cfg.DryRun {
		p.logger.Info("dry-run mode: plan promote without modifying databases")
		return progress, nil
	}

	// spec: 02 §8 Bước 1: Fence source cũ
	if !progress.IsStepCompleted(StepFenceSource) {
		progress.CurrentStep = StepFenceSource
		_ = p.stateStore.SaveProgress(ctx, progress)

		fenced := false
		if p.sourceDriver != nil {
			err := p.sourceDriver.Fence(ctx)
			if err == nil {
				fenced = true
			} else if p.sourceDriver.IsReachable(ctx) {
				// Source còn truy cập được mà fence thất bại: lỗi thật, cờ xác nhận không thay được.
				return progress, fmt.Errorf("fence source failed: %w", err)
			} else {
				p.logger.Warn("source unreachable: cannot fence", "pipeline", p.cfg.PipelineID, "error", err)
			}
		}
		if !fenced {
			// Không fence được (không truy cập được, hoặc không có --fence). "Strand không thấy
			// source" không có nghĩa "source đã chết": mở ghi lúc này có thể sinh hai nơi nhận ghi.
			// Dừng hỏi người vận hành (quyết định P1-17, câu 2, hướng 1).
			if !p.cfg.SourceFencedExternally {
				p.suggestIndependentSequences(progress)
				// Đính kèm tóm tắt tập GTID đã apply ở target ngay ở lần dừng exit 10 để người vận hành
				// biết mức mất dữ liệu trước khi xác nhận (quyết định P1-17, câu 3 = "gộp").
				if summarizer, ok := p.targetDriver.(FailoverSummarizer); ok {
					summary, err := summarizer.FailoverSummary(ctx)
					if err != nil {
						summary = "unknown: " + err.Error()
					}
					progress.FailoverSummary = summary
				}
				progress.State = StateAwaitingOperator
				_ = p.stateStore.SaveProgress(ctx, progress)
				msg := "source could not be fenced: isolate the old source by other means (power off, STONITH, block application traffic), then re-run with --source-fenced-externally"
				if progress.FailoverSummary != "" {
					msg += " (" + progress.FailoverSummary + ")"
				}
				return progress, exitcode.New(
					exitcode.ErrPromoteNeedsDecision,
					exitcode.NeedsDecision,
					msg,
					nil,
				)
			}
			p.logger.Warn("operator confirmed the old source is fenced externally", "pipeline", p.cfg.PipelineID)
		}
		progress.MarkStepCompleted(StepFenceSource)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	// spec: 02 §8 Bước 2: Giữ target read-only (chặn quyền ghi của ứng dụng trước khi setval)
	if !progress.IsStepCompleted(StepHoldReadOnly) {
		progress.CurrentStep = StepHoldReadOnly
		_ = p.stateStore.SaveProgress(ctx, progress)

		if err := p.targetDriver.HoldReadOnly(ctx); err != nil {
			return progress, fmt.Errorf("hold target read-only failed: %w", err)
		}
		progress.MarkStepCompleted(StepHoldReadOnly)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	// spec: 02 §8 Bước 3: Apply nốt phần log còn lại
	if !progress.IsStepCompleted(StepApplyRemaining) {
		progress.CurrentStep = StepApplyRemaining
		_ = p.stateStore.SaveProgress(ctx, progress)

		if err := p.targetDriver.ApplyRemaining(ctx); err != nil {
			return progress, fmt.Errorf("apply remaining log failed: %w", err)
		}
		progress.MarkStepCompleted(StepApplyRemaining)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	// spec: 02 §8 Bước 4: Chạy setval cho mọi sequence và identity
	if !progress.IsStepCompleted(StepSetval) {
		progress.CurrentStep = StepSetval
		_ = p.stateStore.SaveProgress(ctx, progress)

		// Kiểm tra các sequence độc lập trước
		needsDecision := false
		for i, seq := range progress.Sequences {
			if seq.Type == SequenceTypeIndependent {
				// Nếu người dùng đã cung cấp giá trị ghi đè
				if overrideVal, ok := p.cfg.SequenceOverrides[seq.Name]; ok {
					progress.Sequences[i].FinalValue = overrideVal
					continue
				}

				// Kiểm tra khả năng đọc từ source
				sourceReachable := p.sourceDriver != nil && p.sourceDriver.IsReachable(ctx)
				if !sourceReachable {
					// spec: 02 §8: "Nếu sau khi fence không đọc được source, promote dừng và hỏi người vận hành,
					// kèm giá trị gợi ý (lần đồng bộ cuối cộng margin). MySQL dùng AUTO_INCREMENT nên không gặp vấn đề này."
					margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
					suggested := seq.LastSyncedValue + margin
					progress.SuggestedSequences[seq.Name] = suggested
					progress.Sequences[i].SuggestedValue = suggested
					needsDecision = true
				} else {
					lastVal, err := p.sourceDriver.ReadSequenceLastValue(ctx, seq.Name)
					if err != nil {
						margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
						suggested := seq.LastSyncedValue + margin
						progress.SuggestedSequences[seq.Name] = suggested
						progress.Sequences[i].SuggestedValue = suggested
						needsDecision = true
					} else {
						margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
						progress.Sequences[i].CurrentValue = lastVal
						progress.Sequences[i].FinalValue = lastVal + margin
					}
				}
			}
		}

		if needsDecision {
			progress.State = StateAwaitingOperator
			_ = p.stateStore.SaveProgress(ctx, progress)
			return progress, exitcode.New(
				exitcode.ErrPromoteNeedsDecision,
				exitcode.NeedsDecision,
				"source unreachable after fence: operator decision required for independent sequences",
				nil,
			)
		}

		// Thực thi setval cho mọi sequence (cột và độc lập)
		for i, seq := range progress.Sequences {
			var val int64
			if seq.Type == SequenceTypeColumn {
				maxCol, err := p.targetDriver.FindColumnMax(ctx, seq.Table, seq.Column)
				if err != nil {
					return progress, fmt.Errorf("find max column for %s.%s: %w", seq.Table, seq.Column, err)
				}
				margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
				val = maxCol + margin
				progress.Sequences[i].CurrentValue = maxCol
				progress.Sequences[i].FinalValue = val
			} else {
				val = progress.Sequences[i].FinalValue
			}

			if err := p.targetDriver.Setval(ctx, progress.Sequences[i], val); err != nil {
				return progress, fmt.Errorf("setval failed for %s: %w", seq.Name, err)
			}
		}

		progress.MarkStepCompleted(StepSetval)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	// spec: 02 §8 Bước 5: Mở quyền ghi (chỉ mở sau khi Bước 4 setval đã hoàn thành)
	if !progress.IsStepCompleted(StepOpenWrites) {
		progress.CurrentStep = StepOpenWrites
		_ = p.stateStore.SaveProgress(ctx, progress)

		if err := p.targetDriver.OpenWrites(ctx); err != nil {
			return progress, fmt.Errorf("open target writes failed: %w", err)
		}
		progress.MarkStepCompleted(StepOpenWrites)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	// spec: 02 §8 Bước 6: Chuyển DNS hoặc VIP
	if !progress.IsStepCompleted(StepSwitchDNS) {
		progress.CurrentStep = StepSwitchDNS
		progress.MarkStepCompleted(StepSwitchDNS)
		progress.State = StatePromoted
		p.logger.Info("promote completed successfully: DNS/VIP switchover ready", "pipeline", p.cfg.PipelineID)
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}
	}

	return progress, nil
}

package promote

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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

func findSequenceOverride(overrides map[string]int64, name string) (int64, bool) {
	if overrides == nil {
		return 0, false
	}
	if v, ok := overrides[name]; ok {
		return v, true
	}
	if idx := strings.Index(name, "."); idx != -1 {
		if v, ok := overrides[name[idx+1:]]; ok {
			return v, true
		}
	} else {
		if v, ok := overrides["public."+name]; ok {
			return v, true
		}
	}
	return 0, false
}

// mergeSequences lấy danh sách sequence vừa khám phá (fresh) làm chuẩn và giữ lại phần trạng thái
// đã lưu (LastSyncedValue, giá trị đã chốt, gợi ý) theo tên. fresh rỗng thì giữ nguyên danh sách đã lưu.
func mergeSequences(saved, fresh []SequenceInfo) []SequenceInfo {
	if len(fresh) == 0 {
		return saved
	}
	byName := make(map[string]SequenceInfo, len(saved))
	for _, s := range saved {
		byName[s.Name] = s
	}
	out := make([]SequenceInfo, len(fresh))
	for i, f := range fresh {
		if s, ok := byName[f.Name]; ok {
			f.LastSyncedValue = further(f, f.LastSyncedValue, s.LastSyncedValue)
			f.FinalValue = further(f, f.FinalValue, s.FinalValue)
			f.Decided = f.Decided || s.Decided
			if f.SuggestedValue == 0 {
				f.SuggestedValue = s.SuggestedValue
			}
			if f.CurrentValue == 0 {
				f.CurrentValue = s.CurrentValue
			}
		}
		out[i] = f
	}
	return out
}

// sourceHasNoSequences: source không có sequence (MySQL), xem SequencelessSource.
func (p *Promoter) sourceHasNoSequences() bool {
	sl, ok := p.sourceDriver.(SequencelessSource)
	return ok && !sl.HasSequences()
}

// feedsBase trả giá trị xa nhất (theo chiều của sequence) đã dùng ở các cột target mà sequence độc lập
// cấp giá trị: max(cột) với sequence tăng, min(cột) với sequence giảm dần. found = false khi sequence
// không cấp cho cột nào (hoặc mọi cột đều rỗng với sequence giảm dần).
func (p *Promoter) feedsBase(ctx context.Context, seq SequenceInfo) (base int64, found bool, err error) {
	for _, f := range seq.Feeds {
		var v int64
		if isDescending(seq) {
			mf, ok := p.targetDriver.(ColumnMinFinder)
			if !ok {
				return 0, false, fmt.Errorf("target driver cannot read min(%s.%s) for descending sequence %s", f.Table, f.Column, seq.Name)
			}
			if v, err = mf.FindColumnMin(ctx, f.Table, f.Column); err != nil {
				return 0, false, fmt.Errorf("find min column for %s.%s: %w", f.Table, f.Column, err)
			}
		} else if v, err = p.targetDriver.FindColumnMax(ctx, f.Table, f.Column); err != nil {
			return 0, false, fmt.Errorf("find max column for %s.%s: %w", f.Table, f.Column, err)
		}
		if !found {
			base, found = v, true
			continue
		}
		base = further(seq, base, v)
	}
	return base, found, nil
}

// suggestIndependentSequences ghi giá trị gợi ý (lần đồng bộ cuối + margin) cho các
// sequence độc lập chưa có giá trị do người vận hành chỉ định, để in ra cùng lúc dừng hỏi.
func (p *Promoter) suggestIndependentSequences(progress *Progress) {
	for i, seq := range progress.Sequences {
		if seq.Type != SequenceTypeIndependent || seq.Decided {
			continue
		}
		if _, ok := findSequenceOverride(p.cfg.SequenceOverrides, seq.Name); ok {
			continue
		}
		margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)
		suggested := stepAhead(seq, seq.LastSyncedValue, margin)
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
	// Nạp hoặc khởi tạo tiến trình promote. Dry-run không được ghi vào target, kể cả CREATE TABLE
	// _strand_promote mà LoadProgress của DBStateStore làm (review R31): dùng bản nạp chỉ-đọc.
	load := p.stateStore.LoadProgress
	if ro, ok := p.stateStore.(ReadOnlyProgressLoader); ok && p.cfg.DryRun {
		load = ro.LoadProgressReadOnly
	}
	progress, err := load(ctx, p.cfg.PipelineID)
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
		// Danh sách sequence vừa khám phá là chuẩn; giữ lại phần đã lưu (lần đồng bộ cuối, giá trị đã chốt).
		progress.Sequences = mergeSequences(progress.Sequences, p.sequences)
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

		// Chốt giá trị cho từng sequence trước, lưu kế hoạch, rồi mới setval: nếu bị ngắt giữa chừng,
		// lần chạy lại dùng đúng các giá trị đã chốt (không hỏi lại, không hạ xuống).
		needsDecision := false
		for i := range progress.Sequences {
			seq := &progress.Sequences[i]
			margin := CalculateMargin(seq.CacheSize, seq.IncrementBy, p.cfg.PeakRate, p.cfg.SyncIntervalSec)

			if seq.Type == SequenceTypeColumn {
				if isDescending(*seq) {
					// Sequence giảm dần: giá trị kế tiếp phải THẤP hơn mọi giá trị đã dùng → min(cột) - margin
					// (max(cột) + margin sẽ đưa sequence quay lại vùng đã cấp → trùng khóa).
					mf, ok := p.targetDriver.(ColumnMinFinder)
					if !ok {
						return progress, exitcode.New(
							exitcode.ErrPromoteNeedsDecision,
							exitcode.NeedsDecision,
							fmt.Sprintf("sequence %s has negative increment %d (descending) but the target driver cannot read min(%s.%s): operator decision required",
								seq.Name, seq.IncrementBy, seq.Table, seq.Column),
							nil,
						)
					}
					minCol, err := mf.FindColumnMin(ctx, seq.Table, seq.Column)
					if err != nil {
						return progress, fmt.Errorf("find min column for %s.%s: %w", seq.Table, seq.Column, err)
					}
					seq.CurrentValue = minCol
					seq.FinalValue = further(*seq, seq.FinalValue, stepAhead(*seq, minCol, margin))
					continue
				}
				// spec: 02 §8: sequence gắn cột (pg_depend) lấy max(cột) + margin.
				maxCol, err := p.targetDriver.FindColumnMax(ctx, seq.Table, seq.Column)
				if err != nil {
					return progress, fmt.Errorf("find max column for %s.%s: %w", seq.Table, seq.Column, err)
				}
				seq.CurrentValue = maxCol
				seq.FinalValue = further(*seq, seq.FinalValue, stepAhead(*seq, maxCol, margin))
				continue
			}

			// spec: 02 §8: sequence độc lập lấy last_value đọc từ source cộng margin.
			if _, ok := findSequenceOverride(p.cfg.SequenceOverrides, seq.Name); !ok && p.sourceHasNoSequences() {
				// Source MySQL: sequence độc lập chỉ tồn tại ở target, không có last_value nào ở source để đọc.
				// Giá trị đã dùng nằm trong các cột target mà nó cấp (sau bước 3 target đã có mọi row của
				// source), nên xử lý như sequence gắn cột: max(cột) + margin; không cấp cột nào thì chỉ cần
				// không lùi (Setval dùng GREATEST/LEAST với last_value hiện tại).
				base, _, err := p.feedsBase(ctx, *seq)
				if err != nil {
					return progress, fmt.Errorf("independent sequence %s: %w", seq.Name, err)
				}
				seq.CurrentValue = base
				seq.FinalValue = further(*seq, seq.FinalValue, stepAhead(*seq, base, margin))
				seq.Decided = true
				continue
			}
			if overrideVal, ok := findSequenceOverride(p.cfg.SequenceOverrides, seq.Name); ok {
				suggested := stepAhead(*seq, seq.LastSyncedValue, margin)
				if behind(*seq, overrideVal, suggested) {
					p.logger.Warn("operator override is behind the suggested value (last sync + margin in the sequence direction)",
						"sequence", seq.Name, "override", overrideVal, "suggested", suggested, "increment_by", seq.IncrementBy)
				}
				seq.FinalValue = further(*seq, seq.FinalValue, overrideVal)
				seq.Decided = true
				continue
			}
			var readErr error
			if p.sourceDriver != nil && p.sourceDriver.IsReachable(ctx) {
				lastVal, err := p.sourceDriver.ReadSequenceLastValue(ctx, seq.Name)
				if err == nil {
					seq.CurrentValue = lastVal
					seq.FinalValue = further(*seq, seq.FinalValue, stepAhead(*seq, lastVal, margin))
					seq.Decided = true
					continue
				}
				readErr = err
			} else {
				readErr = fmt.Errorf("source unreachable")
			}
			if seq.Decided {
				// Giá trị đã chốt ở lần chạy trước (đọc từ source hoặc override): dùng tiếp.
				p.logger.Warn("cannot read independent sequence from source; using the value decided in a previous run",
					"sequence", seq.Name, "final_value", seq.FinalValue, "error", readErr)
				continue
			}
			// spec: 02 §8: "Nếu sau khi fence không đọc được source, promote dừng và hỏi người vận hành,
			// kèm giá trị gợi ý (lần đồng bộ cuối cộng margin)."
			p.logger.Warn("cannot read independent sequence from source after fence", "sequence", seq.Name, "error", readErr)
			suggested := stepAhead(*seq, seq.LastSyncedValue, margin)
			progress.SuggestedSequences[seq.Name] = suggested
			seq.SuggestedValue = suggested
			needsDecision = true
		}

		if needsDecision {
			progress.State = StateAwaitingOperator
			_ = p.stateStore.SaveProgress(ctx, progress)
			return progress, exitcode.New(
				exitcode.ErrPromoteNeedsDecision,
				exitcode.NeedsDecision,
				"independent sequences could not be read from the source after fence: operator decision required (re-run with --override-sequences)",
				nil,
			)
		}
		if err := p.stateStore.SaveProgress(ctx, progress); err != nil {
			return progress, err
		}

		// Setval cho mọi sequence (cột và độc lập); driver không bao giờ hạ sequence xuống.
		for _, seq := range progress.Sequences {
			if err := p.targetDriver.Setval(ctx, seq, seq.FinalValue); err != nil {
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

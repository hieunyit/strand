package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"strand/internal/model"
)

// spec: 02 §6, design §6.3; invariant: CP-5

// CommitOrderer điều phối thứ tự commit giữa các worker theo chế độ commit_order (strict hoặc relaxed).
// Ở chế độ strict:
// Worker chuẩn bị song song nhưng commit theo đúng seq, qua một ticket là bộ đếm nextCommit.
// Trạng thái target luôn là một prefix của thứ tự commit ở source (CP-5).
type CommitOrderer struct {
	mode       string // "strict" | "relaxed"
	mu         sync.Mutex
	waitCh     chan struct{}
	nextCommit uint64
	failed     error // khi một commit thất bại, mọi worker đang chờ nhận lỗi thay vì treo

	// parallel (strict song song, SetParallel): writer ghi dữ liệu TRƯỚC khi đến lượt rồi chờ lượt để
	// ghi row toàn cục và commit (như Parallel Replicat của GoldenGate, replica_preserve_commit_order của
	// MySQL). Bộ điều phối chỉ giao song song các batch không chung khóa writeset, nhưng khóa ngoài
	// writeset (gap lock InnoDB, ràng buộc chỉ có ở target) vẫn có thể làm batch đến lượt chờ lock của
	// batch đang chờ lượt: vòng chờ đó DB không thấy. applying ghi thời điểm batch (theo seq đầu) bắt
	// đầu ghi dữ liệu; batch đang chờ lượt nhường (rollback, chờ lượt không giữ lock, ghi lại) khi batch
	// đến lượt ghi lâu bất thường (yieldAfter).
	parallel   bool
	applying   map[uint64]time.Time
	avgApply   time.Duration // EWMA thời gian ghi dữ liệu của một batch
	yieldAfter time.Duration // sàn của ngưỡng nhường (mặc định 50ms)
	yields     int64
}

// SetParallel bật strict song song (chỉ có nghĩa ở mode strict). minYield: sàn của ngưỡng nhường; ≤ 0
// dùng 50ms. Ngưỡng thực = max(minYield, 4 × thời gian ghi trung bình của một batch).
func (co *CommitOrderer) SetParallel(minYield time.Duration) {
	if co == nil {
		return
	}
	if minYield <= 0 {
		minYield = 50 * time.Millisecond
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	co.parallel = co.mode == "strict"
	co.applying = make(map[uint64]time.Time)
	co.yieldAfter = minYield
}

// Parallel: strict song song đang bật.
func (co *CommitOrderer) Parallel() bool {
	if co == nil {
		return false
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	return co.parallel
}

// Yields: số lần một batch đã nhường lượt (rollback rồi ghi lại).
func (co *CommitOrderer) Yields() int64 {
	if co == nil {
		return 0
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	return co.yields
}

// BeginApply ghi nhận batch có seq đầu seq bắt đầu ghi dữ liệu (strict song song).
func (co *CommitOrderer) BeginApply(seq uint64) {
	if co == nil {
		return
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	if co.parallel {
		co.applying[seq] = time.Now()
	}
}

// EndApply: batch seq đã ghi xong dữ liệu (đang chờ lượt hoặc đã bỏ); cập nhật thời gian ghi trung bình.
func (co *CommitOrderer) EndApply(seq uint64) {
	if co == nil {
		return
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	if start, ok := co.applying[seq]; ok {
		d := time.Since(start)
		if co.avgApply == 0 {
			co.avgApply = d
		} else {
			co.avgApply = (co.avgApply*7 + d) / 8
		}
		delete(co.applying, seq)
	}
}

// WaitTurnOrYield (strict song song) chờ lượt commit của seq trong khi batch đã giữ lock. Trả
// yield = true khi batch đến lượt đang ghi dữ liệu lâu hơn ngưỡng: có thể nó chờ chính lock mà batch
// này giữ, nên batch này phải rollback, chờ lượt bằng WaitTurn (không giữ lock) rồi ghi lại.
func (co *CommitOrderer) WaitTurnOrYield(ctx context.Context, seq uint64) (yield bool, err error) {
	if co == nil || co.mode != "strict" {
		return false, nil
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		co.mu.Lock()
		if co.failed != nil {
			err := co.failed
			co.mu.Unlock()
			return false, fmt.Errorf("strict commit order halted: %w", err)
		}
		if co.nextCommit == seq {
			co.mu.Unlock()
			return false, nil
		}
		if co.nextCommit > seq {
			next := co.nextCommit
			co.mu.Unlock()
			return false, fmt.Errorf("strict commit order: seq %d already behind next commit %d", seq, next)
		}
		limit := max(co.yieldAfter, 4*co.avgApply)
		if start, ok := co.applying[co.nextCommit]; ok && time.Since(start) > limit {
			co.yields++
			co.mu.Unlock()
			return true, nil
		}
		ch := co.waitCh
		co.mu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ch:
		case <-tick.C:
		}
	}
}

// NewCommitOrderer khởi tạo CommitOrderer mới với mode ("strict" hoặc "relaxed").
func NewCommitOrderer(mode string, startSeq uint64) *CommitOrderer {
	if mode == "" {
		mode = "strict"
	}
	if startSeq == 0 {
		startSeq = 1
	}
	return &CommitOrderer{
		mode:       mode,
		waitCh:     make(chan struct{}),
		nextCommit: startSeq,
	}
}

// Mode trả về chế độ commit ("strict" hoặc "relaxed").
func (co *CommitOrderer) Mode() string {
	return co.mode
}

// NextCommit trả về sequence tiếp theo được phép commit.
func (co *CommitOrderer) NextCommit() uint64 {
	co.mu.Lock()
	defer co.mu.Unlock()
	return co.nextCommit
}

// WaitTurn chờ đến lượt commit cho sequence seq (chỉ chặn ở chế độ strict).
func (co *CommitOrderer) WaitTurn(ctx context.Context, seq uint64) error {
	if co == nil || co.mode != "strict" {
		return nil
	}
	for {
		co.mu.Lock()
		if co.failed != nil {
			err := co.failed
			co.mu.Unlock()
			return fmt.Errorf("strict commit order halted: %w", err)
		}
		if co.nextCommit == seq {
			co.mu.Unlock()
			return nil
		}
		if co.nextCommit > seq {
			next := co.nextCommit
			co.mu.Unlock()
			return fmt.Errorf("strict commit order: seq %d already behind next commit %d", seq, next)
		}
		ch := co.waitCh
		co.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// CommitDone thông báo sequence seq (hoặc batch kết thúc ở seq) đã commit xong.
func (co *CommitOrderer) CommitDone(seq uint64) {
	if co == nil || co.mode != "strict" {
		return
	}
	co.mu.Lock()
	defer co.mu.Unlock()

	if seq >= co.nextCommit {
		co.nextCommit = seq + 1
		close(co.waitCh)
		co.waitCh = make(chan struct{})
	}
}

// Fail dừng orderer: mọi WaitTurn hiện tại và sau này trả lỗi (CP-5: không commit
// Txn sau khi Txn trước nó thất bại).
func (co *CommitOrderer) Fail(err error) {
	if co == nil || err == nil {
		return
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	if co.failed == nil {
		co.failed = err
		close(co.waitCh)
		co.waitCh = make(chan struct{})
	}
}

// Skip đánh dấu seq không cần commit (ví dụ Txn đã có trong tập applied, CP-2),
// để các seq sau không chờ mãi.
func (co *CommitOrderer) Skip(seq uint64) {
	co.CommitDone(seq)
}

// GroupCommitConfig cấu hình giới hạn cho group commit.
// spec: 02 §6
type GroupCommitConfig struct {
	MaxRows  int           `json:"max_rows" yaml:"max_rows"`
	MaxBytes int64         `json:"max_bytes" yaml:"max_bytes"`
	MaxWait  time.Duration `json:"max_wait" yaml:"max_wait"`
}

// DefaultGroupCommitConfig trả về cấu hình group commit mặc định theo spec 02 §6.
func DefaultGroupCommitConfig() GroupCommitConfig {
	return GroupCommitConfig{
		MaxRows:  2000,
		MaxBytes: 4 * 1024 * 1024, // 4 MiB
		MaxWait:  50 * time.Millisecond,
	}
}

// BatchTxns gộp các transaction liên tiếp thành các batch theo giới hạn MaxRows và MaxBytes.
// spec: 02 §6: Một Txn nguồn không bao giờ bị tách thành nhiều lần commit ở target.
func BatchTxns(txns []*model.Txn, cfg GroupCommitConfig) [][]*model.Txn {
	if len(txns) == 0 {
		return nil
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 2000
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 4 * 1024 * 1024
	}

	var batches [][]*model.Txn
	var curBatch []*model.Txn
	var curRows int
	var curBytes int64

	for _, txn := range txns {
		tRows := int(txn.TotalRows())
		tBytes := txn.Bytes

		// Nếu batch hiện tại đã có transaction và thêm txn này sẽ vượt trần max_rows hoặc max_bytes
		// thì đóng batch hiện tại và mở batch mới.
		// BẤT BIẾN: Một Txn nguồn KHÔNG BAO GIỜ bị chia nhỏ, nó luôn được giữ nguyên vẹn trong một batch.
		if len(curBatch) > 0 && (curRows+tRows > cfg.MaxRows || curBytes+tBytes > cfg.MaxBytes) {
			batches = append(batches, curBatch)
			curBatch = nil
			curRows = 0
			curBytes = 0
		}

		curBatch = append(curBatch, txn)
		curRows += tRows
		curBytes += tBytes
	}

	if len(curBatch) > 0 {
		batches = append(batches, curBatch)
	}

	return batches
}

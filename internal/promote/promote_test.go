package promote_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/promote"
)

// mockSourceDriver giả lập source driver.
type mockSourceDriver struct {
	mu          sync.Mutex
	reachable   bool
	fenceCalls  int
	readCalls   map[string]int
	lastValues  map[string]int64
	fenceHook   func() error
}

func newMockSourceDriver(reachable bool) *mockSourceDriver {
	return &mockSourceDriver{
		reachable:  reachable,
		readCalls:  make(map[string]int),
		lastValues: make(map[string]int64),
	}
}

func (m *mockSourceDriver) Fence(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceCalls++
	if m.fenceHook != nil {
		return m.fenceHook()
	}
	if !m.reachable {
		return errors.New("source db connection refused")
	}
	return nil
}

func (m *mockSourceDriver) IsReachable(ctx context.Context) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reachable
}

func (m *mockSourceDriver) ReadSequenceLastValue(ctx context.Context, seqName string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readCalls[seqName]++
	if !m.reachable {
		return 0, errors.New("source db connection refused")
	}
	val, ok := m.lastValues[seqName]
	if !ok {
		return 1000, nil
	}
	return val, nil
}

// mockTargetDriver giả lập target driver kiểm tra trạng thái read-only.
type mockTargetDriver struct {
	mu             sync.Mutex
	readOnly       bool
	holdCalls      int
	applyCalls     int
	openCalls      int
	setvalCalls    int
	maxColValues   map[string]int64
	setvalValues   map[string]int64
	applyHook      func() error
	beforeSetvalHook func() error
}

func newMockTargetDriver() *mockTargetDriver {
	return &mockTargetDriver{
		readOnly:     false,
		maxColValues: make(map[string]int64),
		setvalValues: make(map[string]int64),
	}
}

func (m *mockTargetDriver) HoldReadOnly(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.holdCalls++
	m.readOnly = true
	return nil
}

func (m *mockTargetDriver) IsReadOnly(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, nil
}

func (m *mockTargetDriver) ApplyRemaining(ctx context.Context) error {
	if m.applyHook != nil {
		if err := m.applyHook(); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyCalls++
	return nil
}

func (m *mockTargetDriver) FindColumnMax(ctx context.Context, table, column string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s.%s", table, column)
	if val, ok := m.maxColValues[key]; ok {
		return val, nil
	}
	return 100, nil
}

func (m *mockTargetDriver) Setval(ctx context.Context, seq promote.SequenceInfo, val int64) error {
	if m.beforeSetvalHook != nil {
		if err := m.beforeSetvalHook(); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setvalCalls++
	m.setvalValues[seq.Name] = val
	return nil
}

func (m *mockTargetDriver) OpenWrites(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openCalls++
	m.readOnly = false
	return nil
}

// AppWriteAttempt giả lập ứng dụng cố gắng ghi vào target DB.
func (m *mockTargetDriver) AppWriteAttempt() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readOnly {
		return errors.New("ERROR 1290 (HY000): The MySQL server is running with the --super-read-only option so it cannot execute this statement")
	}
	return nil
}

// TestPM1_AppCannotWriteBeforeSetval kiểm tra AC1:
// Ứng dụng không thể ghi vào target trước khi setval hoàn thành.
func TestPM1_AppCannotWriteBeforeSetval(t *testing.T) {
	ctx := context.Background()
	source := newMockSourceDriver(true)
	target := newMockTargetDriver()
	store := promote.NewMemoryStateStore()

	// Trước khi promote: ứng dụng có thể ghi
	if err := target.AppWriteAttempt(); err != nil {
		t.Fatalf("trước promote ứng dụng phải ghi được: %v", err)
	}

	seqs := []promote.SequenceInfo{
		{
			Name:        "orders_id_seq",
			Table:       "orders",
			Column:      "id",
			Type:        promote.SequenceTypeColumn,
			CacheSize:   1,
			IncrementBy: 1,
		},
	}

	// Đăng ký hook kiểm tra ứng dụng KHÔNG ghi được khi setval đang chạy
	setvalChecked := false
	target.beforeSetvalHook = func() error {
		// Lúc này target phải ở chế độ read-only
		ro, err := target.IsReadOnly(ctx)
		if err != nil || !ro {
			return fmt.Errorf("target phải đang read-only khi chuẩn bị setval")
		}
		// Ứng dụng cố gắng ghi phải bị từ chối
		if err := target.AppWriteAttempt(); err == nil {
			return fmt.Errorf("lỗi: ứng dụng vẫn ghi được trước khi setval xong")
		}
		setvalChecked = true
		return nil
	}

	cfg := promote.Config{
		PipelineID:      "test-pipe-ac1",
		PeakRate:        1000,
		SyncIntervalSec: 5,
	}

	promoter := promote.NewPromoter(cfg, store, source, target, seqs, nil)
	progress, err := promoter.Run(ctx)
	if err != nil {
		t.Fatalf("promote thất bại: %v", err)
	}

	if !setvalChecked {
		t.Fatalf("chưa kiểm tra được việc ứng dụng bị chặn ghi trước khi setval")
	}

	if progress.State != promote.StatePromoted {
		t.Fatalf("trạng thái sau promote mong muốn %s, nhận được %s", promote.StatePromoted, progress.State)
	}

	// Sau khi promote hoàn thành (Bước 5 đã mở quyền ghi): ứng dụng ghi thành công
	if err := target.AppWriteAttempt(); err != nil {
		t.Fatalf("sau khi promote hoàn tất, ứng dụng phải ghi được: %v", err)
	}
}

// TestPM2_ResumableFromInterruptedStep kiểm tra AC2:
// Promote có thể chạy lại từ bước đang dở mà không lặp lại các bước đã xong.
func TestPM2_ResumableFromInterruptedStep(t *testing.T) {
	ctx := context.Background()
	source := newMockSourceDriver(true)
	target := newMockTargetDriver()
	store := promote.NewMemoryStateStore()

	seqs := []promote.SequenceInfo{
		{
			Name:        "users_id_seq",
			Table:       "users",
			Column:      "id",
			Type:        promote.SequenceTypeColumn,
			CacheSize:   1,
			IncrementBy: 1,
		},
	}

	// Cố ý làm Step 3 (ApplyRemaining) bị lỗi ở lần chạy đầu
	firstRun := true
	target.applyHook = func() error {
		if firstRun {
			firstRun = false
			return errors.New("simulated network glitch during apply log")
		}
		return nil
	}

	cfg := promote.Config{
		PipelineID:      "test-pipe-ac2",
		PeakRate:        500,
		SyncIntervalSec: 2,
	}

	promoter := promote.NewPromoter(cfg, store, source, target, seqs, nil)

	// Lần 1: Chạy bị lỗi giữa chừng ở Step 3
	_, err := promoter.Run(ctx)
	if err == nil {
		t.Fatalf("lần 1 mong muốn lỗi nhưng lại thành công")
	}

	// Kiểm tra trạng thái đã lưu: Step 1 và 2 đã hoàn thành, Step 3 là current
	savedP, err := store.LoadProgress(ctx, cfg.PipelineID)
	if err != nil || savedP == nil {
		t.Fatalf("không tải được progress đã lưu: %v", err)
	}
	if !savedP.IsStepCompleted(promote.StepFenceSource) {
		t.Fatalf("Step 1 (fence) phải được đánh dấu hoàn thành")
	}
	if !savedP.IsStepCompleted(promote.StepHoldReadOnly) {
		t.Fatalf("Step 2 (hold read-only) phải được đánh dấu hoàn thành")
	}
	if savedP.IsStepCompleted(promote.StepApplyRemaining) {
		t.Fatalf("Step 3 (apply) không được đánh dấu hoàn thành khi bị lỗi")
	}

	initialFenceCalls := source.fenceCalls
	initialHoldCalls := target.holdCalls

	// Lần 2: Chạy lại đúng lệnh promote
	promoter2 := promote.NewPromoter(cfg, store, source, target, seqs, nil)
	resumedP, err := promoter2.Run(ctx)
	if err != nil {
		t.Fatalf("lần 2 chạy lại từ bước đang dở thất bại: %v", err)
	}

	// Đảm bảo không gọi lại Step 1 và Step 2
	if source.fenceCalls != initialFenceCalls {
		t.Fatalf("lần 2 không được lặp lại fence: gọi %d lần (mong muốn %d)", source.fenceCalls, initialFenceCalls)
	}
	if target.holdCalls != initialHoldCalls {
		t.Fatalf("lần 2 không được lặp lại hold read-only: gọi %d lần (mong muốn %d)", target.holdCalls, initialHoldCalls)
	}

	if resumedP.State != promote.StatePromoted {
		t.Fatalf("sau khi hoàn thành trạng thái phải là %s, nhận được %s", promote.StatePromoted, resumedP.State)
	}
	if !resumedP.IsStepCompleted(promote.StepOpenWrites) {
		t.Fatalf("mọi bước phải hoàn tất")
	}
}

// TestPM3_UnreachableSource_Exit10PromoteNeedsDecision kiểm tra AC3:
// Khi source không truy cập được thì trả exit 10 promote_needs_decision kèm giá trị gợi ý,
// và tiếp tục được khi người vận hành chỉ định giá trị.
func TestPM3_UnreachableSource_Exit10PromoteNeedsDecision(t *testing.T) {
	ctx := context.Background()
	// Giả lập source không truy cập được (failover khẩn cấp)
	source := newMockSourceDriver(false)
	target := newMockTargetDriver()
	store := promote.NewMemoryStateStore()

	lastSynced := int64(8000)
	cacheSize := int64(20)
	incrementBy := int64(1)
	peakRate := float64(100)
	syncIntervalSec := float64(5)

	seqs := []promote.SequenceInfo{
		{
			Name:            "global_txn_seq",
			Type:            promote.SequenceTypeIndependent,
			CacheSize:       cacheSize,
			IncrementBy:     incrementBy,
			LastSyncedValue: lastSynced,
		},
	}

	cfg := promote.Config{
		PipelineID:      "test-pipe-ac3",
		PeakRate:        peakRate,
		SyncIntervalSec: syncIntervalSec,
	}

	promoter := promote.NewPromoter(cfg, store, source, target, seqs, nil)

	// Lần 1: Chạy khi source không thể truy cập
	progress, err := promoter.Run(ctx)
	if err == nil {
		t.Fatalf("mong muốn lỗi exit 10 promote_needs_decision nhưng lại thành công")
	}

	// Kiểm tra exit code = 10 và error code = promote_needs_decision
	var exitErr *exitcode.Error
	if !errors.As(err, &exitErr) {
		t.Fatalf("lỗi phải là exitcode.Error, nhận được: %T (%v)", err, err)
	}
	if exitErr.ExitCode != exitcode.NeedsDecision {
		t.Fatalf("mong muốn exit code %d (NeedsDecision), nhận được %d", exitcode.NeedsDecision, exitErr.ExitCode)
	}
	if exitErr.Code != exitcode.ErrPromoteNeedsDecision {
		t.Fatalf("mong muốn mã lỗi %s, nhận được %s", exitcode.ErrPromoteNeedsDecision, exitErr.Code)
	}

	// Kiểm tra pipeline state chuyển sang awaiting_operator
	if progress.State != promote.StateAwaitingOperator {
		t.Fatalf("trạng thái mong muốn %s, nhận được %s", promote.StateAwaitingOperator, progress.State)
	}

	// Kiểm tra giá trị gợi ý: last_synced + margin
	expectedMargin := promote.CalculateMargin(cacheSize, incrementBy, peakRate, syncIntervalSec)
	expectedSuggested := lastSynced + expectedMargin
	suggestedVal, ok := progress.SuggestedSequences["global_txn_seq"]
	if !ok || suggestedVal != expectedSuggested {
		t.Fatalf("giá trị gợi ý không đúng: mong muốn %d, nhận được %d", expectedSuggested, suggestedVal)
	}

	// Người vận hành quyết định: chấp nhận giá trị gợi ý và chạy lại
	cfg.SequenceOverrides = map[string]int64{
		"global_txn_seq": suggestedVal + 500, // người vận hành có thể chấp nhận hoặc nâng cao hơn
	}

	promoter2 := promote.NewPromoter(cfg, store, source, target, seqs, nil)
	resumedP, err := promoter2.Run(ctx)
	if err != nil {
		t.Fatalf("chạy lại sau khi có quyết định người vận hành thất bại: %v", err)
	}

	if resumedP.State != promote.StatePromoted {
		t.Fatalf("sau khi giải quyết, trạng thái phải là %s, nhận được %s", promote.StatePromoted, resumedP.State)
	}

	if target.setvalValues["global_txn_seq"] != suggestedVal+500 {
		t.Fatalf("giá trị setval không đúng: mong muốn %d, nhận được %d", suggestedVal+500, target.setvalValues["global_txn_seq"])
	}
}

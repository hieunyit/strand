package sched

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"strand/internal/model"
)

// spec: 02 §6

type Config struct {
	TableLevelThreshold int // Mặc định: 50.000 khóa/bảng/Txn
	WritesetBudget      int // Mặc định: 2.000.000 entry
	MaxHoles            int // Mặc định: 4.096
	// MaxPending / MaxPendingBytes: trần số Txn và tổng Txn.Bytes đang chờ phụ thuộc trong DAG (chưa
	// emit). Cùng với trần entry khóa giữ theo seq (≤ WritesetBudget), đây là backpressure của
	// Admit (invariant: CO-2, review R09): một Txn kẹt ở target không làm RAM tăng vô hạn.
	MaxPending      int   // Mặc định: 65.536
	MaxPendingBytes int64 // Mặc định: 512 MiB
	// Serial: mọi Txn được phát ngay theo thứ tự seq, không dựng DAG writeset. Dùng cho
	// commit_order=strict với một writer: Txn được apply tuần tự đúng thứ tự nguồn (kể cả khi
	// chung một batch), nên phụ thuộc writeset là thừa và chỉ làm batch nhỏ lại.
	Serial bool
}

func DefaultConfig() Config {
	return Config{
		TableLevelThreshold: 50000,
		WritesetBudget:      2000000,
		MaxHoles:            4096,
		MaxPending:          65536,
		MaxPendingBytes:     512 << 20,
	}
}

type tableKey struct {
	table string
	key   uint64
}

type tableState struct {
	rows             map[uint64]uint64   // hash khóa -> seq của writer cuối
	tableWriter      uint64              // seq của Txn cấp bảng gần nhất
	active           map[uint64]struct{} // seq đang chạy có chạm bảng này
	forcedTableLevel bool                // bật khi vượt writeset_budget
}

func newTableState() *tableState {
	return &tableState{
		rows:   make(map[uint64]uint64),
		active: make(map[uint64]struct{}),
	}
}

// Scheduler dựng DAG phụ thuộc từ writeset và điều phối transaction cho workers.
// invariant: CO-1
type Scheduler struct {
	cfg Config

	mu          sync.Mutex
	tables      map[string]*tableState
	nextSeq     uint64
	activeSeqs  map[uint64]struct{}   // seq đang chạy trong scheduler (chưa Done)
	pendingTxns map[uint64]*model.Txn // Txn đang chờ phụ thuộc trong DAG (chưa emit)
	inDegree    map[uint64]int
	dependents  map[uint64][]uint64
	readyCh     chan *model.Txn
	overflow    []*model.Txn  // Txn sẵn sàng khi readyCh đầy; không bao giờ gửi blocking khi giữ mu
	spaceCh     chan struct{} // đóng/tạo lại mỗi khi overflow được rút bớt
	totalRows   int
	isClosed    bool
	failed      error // lỗi apply đầu tiên; sau đó scheduler ngừng nhận Txn

	// Theo dõi khóa và bảng của từng seq để Done chỉ tốn O(số khóa của Txn).
	keysBySeq    map[uint64][]tableKey
	tablesBySeq  map[uint64][]string
	retainedKeys int   // tổng len(keysBySeq[*])
	pendingBytes int64 // tổng Txn.Bytes của pendingTxns

	// Holes tracking (spec 02 §1, §2 CP-4, §6)
	lwd        uint64
	high       uint64
	completed  map[uint64]struct{}
	holeWaitCh chan struct{}

	// Metrics
	readyWidthHist map[int]int
	hotRowCount    int64
	totalKeyCount  int64
}

// NewScheduler khởi tạo Scheduler với cấu hình.
func NewScheduler(cfg Config) *Scheduler {
	if cfg.TableLevelThreshold <= 0 {
		cfg.TableLevelThreshold = 50000
	}
	if cfg.WritesetBudget <= 0 {
		cfg.WritesetBudget = 2000000
	}
	if cfg.MaxHoles <= 0 {
		cfg.MaxHoles = 4096
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 65536
	}
	if cfg.MaxPendingBytes <= 0 {
		cfg.MaxPendingBytes = 512 << 20
	}

	return &Scheduler{
		cfg:            cfg,
		tables:         make(map[string]*tableState),
		activeSeqs:     make(map[uint64]struct{}),
		pendingTxns:    make(map[uint64]*model.Txn),
		inDegree:       make(map[uint64]int),
		dependents:     make(map[uint64][]uint64),
		readyCh:        make(chan *model.Txn, 1024),
		spaceCh:        make(chan struct{}),
		keysBySeq:      make(map[uint64][]tableKey),
		tablesBySeq:    make(map[uint64][]string),
		readyWidthHist: make(map[int]int),
		completed:      make(map[uint64]struct{}),
		holeWaitCh:     make(chan struct{}),
	}
}

func (s *Scheduler) holesLocked() int {
	if s.high <= s.lwd {
		return 0
	}
	holes := int(s.high-s.lwd) - len(s.completed)
	if holes < 0 {
		return 0
	}
	return holes
}

// Holes trả về số lượng lỗ hiện tại trong khoảng (lwd, high] (spec 02 §1, §6).
func (s *Scheduler) Holes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.holesLocked()
}

// Lwd trả về mốc hoàn thành liên tục thấp nhất hiện tại.
func (s *Scheduler) Lwd() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lwd
}

// High trả về mốc sequence cao nhất đã hoàn thành.
func (s *Scheduler) High() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.high
}

func (s *Scheduler) Ready() <-chan *model.Txn {
	return s.readyCh
}

// ReadyCh trả về kênh sẵn sàng dạng read/write (để close khi shutdown).
func (s *Scheduler) ReadyCh() chan *model.Txn {
	return s.readyCh
}

func (s *Scheduler) table(tbl string) *tableState {
	st, ok := s.tables[tbl]
	if !ok {
		st = newTableState()
		s.tables[tbl] = st
	}
	return st
}

// TableWritesetEntries trả về số lượng entry trong map rows của bảng (spec 02 §11 metric strand_writeset_entries).
func (s *Scheduler) TableWritesetEntries(tbl string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.tables[tbl]; ok {
		return len(st.rows)
	}
	return 0
}

// TotalWritesetEntries trả về tổng số entry trong map rows của tất cả các bảng.
func (s *Scheduler) TotalWritesetEntries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalRows
}

// IsForcedTableLevel kiểm tra bảng có đang ở chế độ forcedTableLevel không.
func (s *Scheduler) IsForcedTableLevel(tbl string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.tables[tbl]; ok {
		return st.forcedTableLevel
	}
	return false
}

// ComputeWriteset tính toán các hash khóa xxh3 theo từng bảng cho Txn theo spec 02 §6.
func ComputeWriteset(t *model.Txn) map[string][]uint64 {
	if t == nil {
		return make(map[string][]uint64)
	}
	return t.WritesetByTable()
}

func (s *Scheduler) checkBudgetLocked() {
	if s.totalRows > s.cfg.WritesetBudget {
		var maxTbl string
		var maxCount int
		for tbl, st := range s.tables {
			if len(st.rows) > maxCount {
				maxCount = len(st.rows)
				maxTbl = tbl
			}
		}
		if maxTbl != "" && maxCount > 0 {
			s.tables[maxTbl].forcedTableLevel = true
		}
	}
}

// overBudgetLocked: nhận t lúc này có vượt trần RAM của DAG không (CO-2, review R09). Trần entry khóa
// giữ theo seq là WritesetBudget — map row có thể chỉ có một khóa nóng trong khi mỗi Txn chờ vẫn giữ
// bản sao khóa của mình. Khi DAG không còn Txn nào chờ thì luôn nhận (Txn đơn lẻ lớn hơn trần vẫn
// đi qua, nó đã bị nâng cấp bảng ở Admit), nên không bao giờ tự khóa chết: Txn đang chờ chỉ phụ
// thuộc Txn đã nhận trước đó, và mỗi Done đánh thức Admit.
func (s *Scheduler) overBudgetLocked(t *model.Txn) bool {
	if s.cfg.Serial || len(s.pendingTxns) == 0 {
		return false
	}
	if len(s.pendingTxns) >= s.cfg.MaxPending || s.pendingBytes+t.Bytes > s.cfg.MaxPendingBytes {
		return true
	}
	return s.retainedKeys+len(t.Rows) > s.cfg.WritesetBudget // ≥ 1 khóa mỗi row (Txn tràn đĩa: cấp bảng, không giữ khóa)
}

// Err trả về lỗi apply đầu tiên được báo qua Done (nil nếu chưa có).
func (s *Scheduler) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// Admit tiếp nhận một Txn theo đúng thuật toán spec 02 §6.
func (s *Scheduler) Admit(ctx context.Context, t *model.Txn) error {
	if t.IsSpilled() && len(t.SpilledTables) == 0 {
		return fmt.Errorf("spilled txn %v has no table list: cannot build table-level dependencies (CO-1)", t.SourcePos)
	}

	s.mu.Lock()

	// Chặn nhận Txn mới khi số holes vượt ngưỡng max_holes (spec 02 §2 CP-4, §6),
	// hoặc khi hàng đợi sẵn sàng đã đầy (back-pressure, không giữ mu khi chờ).
	for s.failed == nil && !s.isClosed &&
		(s.holesLocked() > s.cfg.MaxHoles || len(s.overflow) >= cap(s.readyCh) || s.overBudgetLocked(t)) {
		holeCh, spaceCh := s.holeWaitCh, s.spaceCh
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-holeCh:
		case <-spaceCh:
		}

		s.mu.Lock()
	}

	if s.failed != nil {
		err := s.failed
		s.mu.Unlock()
		return fmt.Errorf("scheduler stopped after apply failure: %w", err)
	}
	if s.isClosed {
		s.mu.Unlock()
		return fmt.Errorf("scheduler is closed")
	}
	defer s.mu.Unlock()
	s.drainOverflowLocked()

	// Tự động gán seq nếu chưa có
	if t.Seq() == 0 {
		s.nextSeq++
		t.SetSeq(s.nextSeq)
	}
	seq := t.Seq()

	if s.cfg.Serial {
		s.activeSeqs[seq] = struct{}{}
		s.emitReady(t)
		return nil
	}

	s.checkBudgetLocked()

	writeset := t.WritesetByTable()
	spilledTables := make(map[string]bool)
	if t.IsSpilled() {
		// Txn tràn đĩa không có Rows trong RAM: luôn nâng lên cấp bảng cho mọi bảng nó chạm.
		for _, tid := range t.SpilledTables {
			name := tid.String()
			spilledTables[name] = true
			if _, ok := writeset[name]; !ok {
				writeset[name] = nil
			}
		}
	}
	var deps []uint64

	for tbl, keys := range writeset {
		st := s.table(tbl)
		s.totalKeyCount += int64(len(keys))
		s.tablesBySeq[seq] = append(s.tablesBySeq[seq], tbl)

		if st.tableWriter != 0 {
			deps = append(deps, st.tableWriter)
		}

		isTableLevel := spilledTables[tbl] || len(keys) > s.cfg.TableLevelThreshold || st.forcedTableLevel
		if !isTableLevel && s.totalRows+len(keys) > s.cfg.WritesetBudget {
			// Bảng chiếm nhiều entry nhất chuyển sang cấp bảng
			isMax := true
			for otherTbl, otherSt := range s.tables {
				if otherTbl != tbl && len(otherSt.rows) > len(st.rows)+len(keys) {
					isMax = false
					break
				}
			}
			if isMax {
				isTableLevel = true
				st.forcedTableLevel = true
			}
		}

		if isTableLevel {
			// Nâng thành khóa cấp bảng: phụ thuộc mọi Txn đang chạy trên bảng
			for p := range st.active {
				if p < seq {
					deps = append(deps, p)
				}
			}
			st.tableWriter = seq
			// Dọn sạch map row của bảng (spec 02 §6)
			s.totalRows -= len(st.rows)
			clear(st.rows)
		} else {
			for _, k := range keys {
				if p, ok := st.rows[k]; ok {
					if p < seq {
						deps = append(deps, p)
						s.hotRowCount++
					}
				}
				if _, exists := st.rows[k]; !exists {
					s.totalRows++
				}
				st.rows[k] = seq
				s.keysBySeq[seq] = append(s.keysBySeq[seq], tableKey{table: tbl, key: k})
				s.retainedKeys++
			}
		}
		st.active[seq] = struct{}{}
	}

	s.checkBudgetLocked()

	// Lọc bỏ dep trùng và chỉ giữ các dep còn đang active trong scheduler
	deps = dedup(deps)
	var activeDeps []uint64
	for _, dep := range deps {
		if _, ok := s.activeSeqs[dep]; ok {
			activeDeps = append(activeDeps, dep)
		}
	}

	s.activeSeqs[seq] = struct{}{}

	if len(activeDeps) == 0 {
		// Không có phụ thuộc, sẵn sàng chạy ngay
		s.emitReady(t)
	} else {
		s.pendingTxns[seq] = t
		s.pendingBytes += t.Bytes
		s.inDegree[seq] = len(activeDeps)
		for _, dep := range activeDeps {
			s.dependents[dep] = append(s.dependents[dep], seq)
		}
	}

	return nil
}

func (s *Scheduler) emitReady(t *model.Txn) {
	// Ghi nhận histogram độ rộng DAG (số txn trong ready queue)
	curWidth := len(s.readyCh) + len(s.overflow) + 1
	bucket := (curWidth / 4) * 4
	s.readyWidthHist[bucket]++

	// Không bao giờ gửi blocking khi đang giữ mu: nếu readyCh đầy thì đưa vào overflow,
	// overflow được rút ở Done/Admit kế tiếp (worker luôn gọi Done sau khi lấy Txn).
	if len(s.overflow) == 0 {
		select {
		case s.readyCh <- t:
			return
		default:
		}
	}
	s.overflow = append(s.overflow, t)
}

// drainOverflowLocked chuyển Txn từ overflow sang readyCh khi còn chỗ (không blocking).
func (s *Scheduler) drainOverflowLocked() {
	moved := false
	for len(s.overflow) > 0 {
		select {
		case s.readyCh <- s.overflow[0]:
			s.overflow[0] = nil
			s.overflow = s.overflow[1:]
			moved = true
		default:
			goto out
		}
	}
out:
	if moved {
		close(s.spaceCh)
		s.spaceCh = make(chan struct{})
	}
}

// Done thông báo một Txn đã hoàn thành apply.
// Nếu err != nil, Txn KHÔNG được coi là đã apply: các Txn phụ thuộc vẫn bị chặn,
// lwd không vượt qua nó, và scheduler ngừng nhận Txn mới (Err() trả về lỗi).
func (s *Scheduler) Done(seq uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		if s.failed == nil {
			s.failed = fmt.Errorf("txn seq=%d failed: %w", seq, err)
		}
		// Đánh thức các Admit đang chờ để chúng trả lỗi.
		if s.holeWaitCh != nil {
			close(s.holeWaitCh)
			s.holeWaitCh = make(chan struct{})
		}
		return
	}

	// Dọn khóa cấp row của chính Txn này: O(số khóa của Txn).
	for _, tk := range s.keysBySeq[seq] {
		st := s.tables[tk.table]
		if st == nil {
			continue
		}
		if w, ok := st.rows[tk.key]; ok && w == seq {
			delete(st.rows, tk.key)
			s.totalRows--
		}
	}
	s.retainedKeys -= len(s.keysBySeq[seq])
	delete(s.keysBySeq, seq)

	for _, tbl := range s.tablesBySeq[seq] {
		st := s.tables[tbl]
		if st == nil {
			continue
		}
		delete(st.active, seq)
		if st.tableWriter == seq {
			st.tableWriter = 0
		}
	}
	delete(s.tablesBySeq, seq)

	for _, st := range s.tables {
		if st.forcedTableLevel && len(st.rows) == 0 && st.tableWriter == 0 && s.totalRows <= s.cfg.WritesetBudget {
			st.forcedTableLevel = false
		}
	}
	s.checkBudgetLocked()

	// Giải phóng các transaction phụ thuộc trong DAG
	if depList, ok := s.dependents[seq]; ok {
		for _, depSeq := range depList {
			s.inDegree[depSeq]--
			if s.inDegree[depSeq] == 0 {
				delete(s.inDegree, depSeq)
				if readyTxn, exists := s.pendingTxns[depSeq]; exists {
					delete(s.pendingTxns, depSeq)
					s.pendingBytes -= readyTxn.Bytes
					s.emitReady(readyTxn)
				}
			}
		}
		delete(s.dependents, seq)
	}

	if t, ok := s.pendingTxns[seq]; ok {
		s.pendingBytes -= t.Bytes
		delete(s.pendingTxns, seq)
	}
	delete(s.activeSeqs, seq)

	// Cập nhật mốc hoàn thành và holes
	if seq > s.lwd {
		s.completed[seq] = struct{}{}
		if seq > s.high {
			s.high = seq
		}
		for {
			next := s.lwd + 1
			if _, ok := s.completed[next]; ok {
				delete(s.completed, next)
				s.lwd = next
			} else {
				break
			}
		}
		// Đánh thức các goroutine đang chờ Admit khi holes giảm
		if s.holeWaitCh != nil {
			close(s.holeWaitCh)
			s.holeWaitCh = make(chan struct{})
		}
	}

	s.drainOverflowLocked()
}

// Close đóng scheduler khi không còn transaction mới.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isClosed = true
	if s.holeWaitCh != nil {
		select {
		case <-s.holeWaitCh:
		default:
			close(s.holeWaitCh)
		}
	}
}

// Stats trả về số liệu thống kê.
func (s *Scheduler) Stats() (dagHist map[string]int, hotRowRatio float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dagHist = make(map[string]int)
	for bucket, count := range s.readyWidthHist {
		key := fmt.Sprintf("[%d-%d]", bucket, bucket+3)
		dagHist[key] = count
	}

	if s.totalKeyCount > 0 {
		hotRowRatio = float64(s.hotRowCount) / float64(s.totalKeyCount)
	}
	return dagHist, hotRowRatio
}

func dedup(src []uint64) []uint64 {
	if len(src) <= 1 {
		return src
	}
	sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
	out := make([]uint64, 0, len(src))
	out = append(out, src[0])
	for i := 1; i < len(src); i++ {
		if src[i] != src[i-1] {
			out = append(out, src[i])
		}
	}
	return out
}

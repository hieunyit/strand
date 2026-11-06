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
	// MaxPending / MaxPendingBytes: trần số Txn và tổng phần RAM (heldBytes: Txn.Bytes, hoặc ước lượng
	// khóa + metadata với Txn tràn đĩa) của Txn đang chờ phụ thuộc trong DAG (chưa emit). Cùng với trần
	// entry khóa giữ theo seq (≤ WritesetBudget), đây là backpressure của Admit (invariant: CO-2,
	// review R09): một Txn kẹt ở target không làm RAM tăng vô hạn.
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
	pendingBytes int64 // tổng heldBytes của pendingTxns

	// Làn tuần tự (model.Txn.Lane): Txn làn không phụ thuộc nhau qua writeset — thứ tự giữa chúng là
	// thứ tự Admit, do một worker riêng apply (NextLaneBatch); phụ thuộc với Txn thường giữ nguyên ở
	// cả hai chiều (invariant: CO-1). Không deadlock: đầu làn chỉ chờ Txn thường có seq nhỏ hơn nó, và
	// Txn đó không phụ thuộc Txn làn nào đứng sau (phụ thuộc chỉ trỏ về seq nhỏ hơn).
	laneQ      []uint64              // seq của Txn làn chưa được lấy, theo thứ tự Admit
	laneReady  map[uint64]*model.Txn // Txn làn đã hết phụ thuộc, chờ tới lượt
	laneActive map[uint64]struct{}   // seq Txn làn chưa Done
	laneWake   chan struct{}         // đóng/tạo lại khi đầu làn có thể đã sẵn sàng
	// laneHeld: Txn làn đã ra khỏi DAG (laneReady, hoặc đã được NextLaneBatch lấy) mà chưa Done → Bytes.
	// Tính vào trần MaxPending/MaxPendingBytes của Admit cùng pendingTxns (review N02): không thì
	// worker làn kẹt ở target + luồng toàn Txn làn làm laneQ/laneReady tăng vô hạn.
	laneHeld      map[uint64]int64
	laneHeldBytes int64

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
		laneReady:      make(map[uint64]*model.Txn),
		laneActive:     make(map[uint64]struct{}),
		laneHeld:       make(map[uint64]int64),
		laneWake:       make(chan struct{}),
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

// Config trả cấu hình hiệu lực (đã điền mặc định) của scheduler.
func (s *Scheduler) Config() Config { return s.cfg }

// writesetKeys: số entry khóa mà t sẽ giữ theo seq ở Admit (cận trên): ≥ 1 khóa mỗi row trong RAM;
// Txn tràn đĩa chỉ giữ khóa đã gom trong SpillWriteset (bảng cấp bảng không giữ khóa).
func writesetKeys(t *model.Txn) int {
	if t.IsSpilled() {
		return t.SpillWritesetKeys()
	}
	return len(t.Rows)
}

// Ước lượng RAM của một Txn tràn đĩa khi nằm trong DAG: metadata cố định, mỗi bảng, mỗi khóa (hash trong
// SpillWriteset + tableKey trong keysBySeq + entry map row).
const (
	spilledTxnBaseBytes = 512
	spilledTableBytes   = 64
	spilledWsEntryBytes = 64
)

// heldBytes: phần RAM của t tính vào MaxPendingBytes. Txn tràn đĩa có payload trên đĩa, chỉ khóa và
// metadata nằm trong RAM — tính Txn.Bytes (kích thước payload) sẽ làm backpressure đếm thừa.
// Hàm thuần theo các trường không đổi sau Admit, nên cộng/trừ luôn khớp.
func heldBytes(t *model.Txn) int64 {
	if !t.IsSpilled() {
		return t.Bytes
	}
	return spilledTxnBaseBytes + int64(len(t.SpilledTables))*spilledTableBytes +
		int64(t.SpillWritesetKeys())*spilledWsEntryBytes
}

// overBudgetLocked: nhận t lúc này có vượt trần RAM của DAG không (CO-2, review R09). Trần entry khóa
// giữ theo seq là WritesetBudget — map row có thể chỉ có một khóa nóng trong khi mỗi Txn chờ vẫn giữ
// bản sao khóa của mình. Khi DAG không còn Txn nào chờ thì luôn nhận (Txn đơn lẻ lớn hơn trần vẫn
// đi qua, nó đã bị nâng cấp bảng ở Admit), nên không bao giờ tự khóa chết: Txn đang chờ chỉ phụ
// thuộc Txn đã nhận trước đó, và mỗi Done đánh thức Admit.
func (s *Scheduler) overBudgetLocked(t *model.Txn) bool {
	// review N02: Txn làn đang giữ (chưa Done) cũng tính vào trần; vẫn luôn nhận khi không còn gì giữ.
	queued := len(s.pendingTxns) + len(s.laneHeld)
	if s.cfg.Serial || queued == 0 {
		return false
	}
	if queued >= s.cfg.MaxPending || s.pendingBytes+s.laneHeldBytes+heldBytes(t) > s.cfg.MaxPendingBytes {
		return true
	}
	return s.retainedKeys+writesetKeys(t) > s.cfg.WritesetBudget
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
		// Txn tràn đĩa không có Rows trong RAM: bảng có khóa trong SpillWriteset giữ cấp row như Txn
		// trong RAM (ngưỡng table_level_threshold / writeset_budget bên dưới vẫn áp dụng); bảng không có
		// khóa (vượt trần khi gom, hoặc Txn không được gom writeset) thì nâng lên cấp bảng.
		for _, tid := range t.SpilledTables {
			name := tid.String()
			if _, ok := writeset[name]; !ok {
				spilledTables[name] = true
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
			if _, lane := s.laneActive[dep]; lane && !t.Lane && !s.cfg.Serial {
				// Lây làn: Txn thường phụ thuộc một Txn làn chưa xong đi luôn vào làn. Không thì chuỗi
				// khóa nóng xen kẽ làn ↔ worker thường thành một commit cho mỗi bước (mỗi bên chờ bên kia
				// commit); trong làn cả chuỗi ra cùng một batch (group commit). Chỉ thêm ràng buộc thứ tự.
				t.Lane = true
			}
		}
	}
	if t.Lane {
		kept := activeDeps[:0]
		for _, dep := range activeDeps {
			if _, lane := s.laneActive[dep]; !lane {
				kept = append(kept, dep) // thứ tự giữa hai Txn làn do làn bảo đảm (NextLaneBatch)
			}
		}
		activeDeps = kept
	}

	s.activeSeqs[seq] = struct{}{}
	if t.Lane {
		s.laneQ = append(s.laneQ, seq)
		s.laneActive[seq] = struct{}{}
	}

	if len(activeDeps) == 0 {
		// Không có phụ thuộc, sẵn sàng chạy ngay
		s.emitReady(t)
	} else {
		s.pendingTxns[seq] = t
		s.pendingBytes += heldBytes(t)
		s.inDegree[seq] = len(activeDeps)
		for _, dep := range activeDeps {
			s.dependents[dep] = append(s.dependents[dep], seq)
		}
	}

	return nil
}

func (s *Scheduler) emitReady(t *model.Txn) {
	if t.Lane && !s.cfg.Serial {
		s.laneReady[t.Seq()] = t
		b := heldBytes(t)
		s.laneHeld[t.Seq()] = b
		s.laneHeldBytes += b
		s.wakeLaneLocked()
		return
	}
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
		s.wakeLaneLocked()
		return
	}
	delete(s.laneActive, seq)
	if b, ok := s.laneHeld[seq]; ok {
		// review N02: nhả phần trần của Txn làn; Admit được đánh thức qua holeWaitCh bên dưới.
		delete(s.laneHeld, seq)
		s.laneHeldBytes -= b
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
			if _, ok := s.inDegree[depSeq]; !ok {
				continue // đã được TakeChained lấy cùng batch với seq (không còn chờ trong DAG)
			}
			s.inDegree[depSeq]--
			if s.inDegree[depSeq] == 0 {
				delete(s.inDegree, depSeq)
				if readyTxn, exists := s.pendingTxns[depSeq]; exists {
					delete(s.pendingTxns, depSeq)
					s.pendingBytes -= heldBytes(readyTxn)
					s.emitReady(readyTxn)
				}
			}
		}
		delete(s.dependents, seq)
	}

	if t, ok := s.pendingTxns[seq]; ok {
		s.pendingBytes -= heldBytes(t)
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

// TakeChained lấy ra khỏi DAG các Txn đang chờ mà MỌI phụ thuộc còn lại đều nằm trong batch — các
// Txn mà worker gọi đã lấy (từ Ready hoặc từ lần TakeChained trước) và sẽ apply trong CÙNG một
// transaction ở target theo thứ tự trả về. Kết quả xếp sau mọi phụ thuộc của nó (duyệt theo thứ tự
// batch, mỗi Txn chỉ được lấy khi phụ thuộc cuối cùng đã nằm trong batch), nên apply batch theo thứ tự
// vẫn giữ đúng thứ tự giữa các Txn phụ thuộc nhau (CO-1); worker phải gọi Done cho mọi Txn lấy ra.
//
// Lý do (G2-11 hot row): một row cập nhật liên tục tạo một chuỗi Txn phụ thuộc nối tiếp. Không gom
// chuỗi thì mỗi Txn là một commit riêng, và Txn kế tiếp chỉ sẵn sàng sau khi Txn trước Done — worker
// còn chờ thêm target.batch.max_wait (50 ms) mà không Txn nào tới được: ~20 Txn/giây cho cả pipeline,
// tụt hậu vô hạn sau source 1.000 Txn/giây. Làn tuần tự (PG) đã gom chuỗi kiểu này; đây là bản cho
// worker thường.
//
// accept được gọi (khi đang giữ khóa scheduler) trước khi lấy từng Txn; trả false thì dừng (batch đầy).
// Txn làn không bao giờ được lấy (làn có worker riêng). Serial: không có DAG, trả nil.
func (s *Scheduler) TakeChained(batch []*model.Txn, accept func(*model.Txn) bool) []*model.Txn {
	if len(batch) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Serial || s.failed != nil || len(s.pendingTxns) == 0 {
		return nil
	}
	members := make([]uint64, 0, len(batch))
	for _, t := range batch {
		members = append(members, t.Seq())
	}
	// cnt[d]: số phụ thuộc của d đã nằm trong batch. Mỗi cạnh x → d được đếm đúng một lần (khi duyệt x);
	// cnt[d] == inDegree[d] nghĩa là mọi phụ thuộc còn lại của d là thành viên batch.
	cnt := map[uint64]int{}
	var out []*model.Txn
	for i := 0; i < len(members); i++ {
		for _, d := range s.dependents[members[i]] {
			need, waiting := s.inDegree[d]
			if !waiting {
				continue
			}
			cnt[d]++
			if cnt[d] != need {
				continue
			}
			t, ok := s.pendingTxns[d]
			if !ok || t.Lane {
				continue
			}
			if !accept(t) {
				return out
			}
			delete(s.pendingTxns, d)
			delete(s.inDegree, d) // Done của các phụ thuộc bỏ qua d (xem Done)
			s.pendingBytes -= heldBytes(t)
			members = append(members, d)
			out = append(out, t)
		}
	}
	return out
}

func (s *Scheduler) wakeLaneLocked() {
	close(s.laneWake)
	s.laneWake = make(chan struct{})
}

// NextLaneBatch trả các Txn làn liên tiếp tính từ đầu làn đã hết phụ thuộc, đúng thứ tự Admit — tối
// đa maxTxns Txn và (nếu maxRows > 0) tới khi đạt maxRows row — để apply trong MỘT transaction ở target
// (group commit). Chờ tới khi đầu làn sẵn sàng; trả nil khi ctx hết, scheduler lỗi hoặc đóng. Chỉ một
// goroutine được gọi (một worker làn): thứ tự apply giữa các Txn làn là thứ tự trả về.
func (s *Scheduler) NextLaneBatch(ctx context.Context, maxTxns int, maxRows int64) []*model.Txn {
	if maxTxns <= 0 {
		maxTxns = 256
	}
	s.mu.Lock()
	for {
		if s.failed != nil || ctx.Err() != nil {
			s.mu.Unlock()
			return nil
		}
		if len(s.laneQ) > 0 {
			if _, ok := s.laneReady[s.laneQ[0]]; ok {
				break
			}
		} else if s.isClosed {
			s.mu.Unlock()
			return nil
		}
		wake := s.laneWake
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		}
		s.mu.Lock()
	}
	var out []*model.Txn
	var rows int64
	for len(s.laneQ) > 0 && len(out) < maxTxns && (maxRows <= 0 || rows < maxRows) {
		t, ok := s.laneReady[s.laneQ[0]]
		if !ok {
			break
		}
		delete(s.laneReady, s.laneQ[0])
		s.laneQ = s.laneQ[1:]
		out = append(out, t)
		rows += t.TotalRows()
	}
	s.mu.Unlock()
	return out
}

// LanePending: số Txn làn chưa được lấy (metric/test).
func (s *Scheduler) LanePending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.laneQ)
}

// Close đóng scheduler khi không còn transaction mới.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isClosed = true
	s.wakeLaneLocked()
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

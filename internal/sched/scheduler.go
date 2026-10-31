package sched

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/zeebo/xxh3"
	"strand/internal/model"
)

// spec: 02 §6

type Config struct {
	TableLevelThreshold int // Mặc định: 50.000 khóa/bảng/Txn
	WritesetBudget      int // Mặc định: 2.000.000 entry
	MaxHoles            int // Mặc định: 4.096
}

func DefaultConfig() Config {
	return Config{
		TableLevelThreshold: 50000,
		WritesetBudget:      2000000,
		MaxHoles:            4096,
	}
}

type tableState struct {
	rows             map[uint64]uint64   // hash khóa -> seq của writer cuối
	tableWriter      uint64              // seq của Txn cấp bảng gần nhất
	active           map[uint64]struct{} // seq đang chạy có chạm bảng này
	forcedTableLevel bool               // bật khi vượt writeset_budget
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
	activeTxns  map[uint64]*model.Txn
	inDegree    map[uint64]int
	dependents  map[uint64][]uint64
	readyCh     chan *model.Txn
	totalRows   int
	isClosed    bool

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

	return &Scheduler{
		cfg:            cfg,
		tables:         make(map[string]*tableState),
		activeTxns:     make(map[uint64]*model.Txn),
		inDegree:       make(map[uint64]int),
		dependents:     make(map[uint64][]uint64),
		readyCh:        make(chan *model.Txn, 1024),
		readyWidthHist: make(map[int]int),
	}
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

// ComputeWriteset tính toán các hash khóa xxh3 theo từng bảng cho Txn.
func ComputeWriteset(t *model.Txn) map[string][]uint64 {
	ws := make(map[string][]uint64)
	for _, row := range t.Rows {
		tbl := row.Table.String()
		if len(row.Key) > 0 {
			keyStr := fmt.Sprintf("%v", row.Key)
			h := xxh3.HashString(tbl + "|" + keyStr)
			ws[tbl] = append(ws[tbl], h)
		}
		if len(row.OldKey) > 0 {
			oldStr := fmt.Sprintf("%v", row.OldKey)
			hOld := xxh3.HashString(tbl + "|" + oldStr)
			ws[tbl] = append(ws[tbl], hOld)
		}
	}
	return ws
}

// Admit tiếp nhận một Txn theo đúng thuật toán spec 02 §6.
func (s *Scheduler) Admit(ctx context.Context, t *model.Txn) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return fmt.Errorf("scheduler is closed")
	}

	// Tự động gán seq nếu chưa có
	if t.Seq() == 0 {
		s.nextSeq++
		t.SetSeq(s.nextSeq)
	}
	seq := t.Seq()

	writeset := ComputeWriteset(t)
	var deps []uint64

	for tbl, keys := range writeset {
		st := s.table(tbl)
		s.totalKeyCount += int64(len(keys))

		if st.tableWriter != 0 {
			deps = append(deps, st.tableWriter)
		}

		if len(keys) > s.cfg.TableLevelThreshold || st.forcedTableLevel {
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
			}
		}
		st.active[seq] = struct{}{}
	}

	// Kiểm tra writeset_budget: nếu vượt, đặt cờ forcedTableLevel cho bảng chiếm nhiều entry nhất
	if s.totalRows > s.cfg.WritesetBudget {
		var maxTbl string
		var maxCount int
		for tbl, st := range s.tables {
			if len(st.rows) > maxCount {
				maxCount = len(st.rows)
				maxTbl = tbl
			}
		}
		if maxTbl != "" {
			s.tables[maxTbl].forcedTableLevel = true
		}
	}

	// Lọc bỏ dep trùng và chỉ giữ các dep còn đang active trong scheduler
	deps = dedup(deps)
	var activeDeps []uint64
	for _, dep := range deps {
		if _, ok := s.activeTxns[dep]; ok {
			activeDeps = append(activeDeps, dep)
		}
	}

	s.activeTxns[seq] = t

	if len(activeDeps) == 0 {
		// Không có phụ thuộc, sẵn sàng chạy ngay
		s.emitReady(t)
	} else {
		s.inDegree[seq] = len(activeDeps)
		for _, dep := range activeDeps {
			s.dependents[dep] = append(s.dependents[dep], seq)
		}
	}

	return nil
}

func (s *Scheduler) emitReady(t *model.Txn) {
	// Ghi nhận histogram độ rộng DAG (số txn trong ready queue)
	curWidth := len(s.readyCh) + 1
	bucket := (curWidth / 4) * 4
	s.readyWidthHist[bucket]++

	s.readyCh <- t
}

// Done thông báo một Txn đã hoàn thành apply.
func (s *Scheduler) Done(seq uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Dọn trạng thái bảng
	for _, st := range s.tables {
		delete(st.active, seq)
		if st.tableWriter == seq {
			st.tableWriter = 0
			st.forcedTableLevel = false
		}
		for k, writerSeq := range st.rows {
			if writerSeq == seq {
				delete(st.rows, k)
				s.totalRows--
			}
		}
	}

	// Giải phóng các transaction phụ thuộc trong DAG
	if depList, ok := s.dependents[seq]; ok {
		for _, depSeq := range depList {
			s.inDegree[depSeq]--
			if s.inDegree[depSeq] == 0 {
				delete(s.inDegree, depSeq)
				if readyTxn, exists := s.activeTxns[depSeq]; exists {
					s.emitReady(readyTxn)
				}
			}
		}
		delete(s.dependents, seq)
	}

	delete(s.activeTxns, seq)
}

// Close đóng scheduler khi không còn transaction mới.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isClosed = true
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

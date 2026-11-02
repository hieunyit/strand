package checkpoint

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"strand/internal/model"
)

// spec: 02 §3, §4 (PG: {"lwd","high","holes"})

// PGAppliedSet là tập Txn PostgreSQL đã apply, định danh bằng commit LSN.
//
// LSN là offset byte trong WAL, KHÔNG liên tiếp (hai Txn kế nhau cách nhau hàng trăm byte trở lên),
// nên không thể biểu diễn bằng dải số nguyên như GNO của MySQL: tập chỉ lưu các LSN tường minh.
//
//   - base: mốc đã được xác nhận — mọi Txn có commit LSN ≤ base đã apply, TRỪ các LSN trong holes.
//     Chỉ bên biết luồng Txn (coordinator/scheduler) mới xác nhận được mốc này, qua Vouch.
//   - holes: LSN ≤ base của các Txn đã biết là CHƯA apply.
//   - extra: LSN > base đã apply (tường minh). Row delta của worker ở chế độ relaxed nằm ở đây.
//
// Bộ nhớ tỷ lệ với |holes| + |extra|, không phụ thuộc giá trị LSN.
type PGAppliedSet struct {
	base  uint64
	holes map[uint64]struct{}
	extra map[uint64]struct{}
}

// NewPGAppliedSet tạo tập với mốc base: mọi Txn có LSN ≤ base coi như đã apply (0 = tập rỗng).
func NewPGAppliedSet(base uint64) *PGAppliedSet {
	return &PGAppliedSet{base: base, holes: map[uint64]struct{}{}, extra: map[uint64]struct{}{}}
}

func pgPos(l uint64) model.Pos { return model.Pos{Engine: model.EnginePG, LSN: l} }

// FormatLSN in LSN theo dạng "X/Y" của PostgreSQL.
func FormatLSN(l uint64) string { return fmt.Sprintf("%X/%X", l>>32, l&0xFFFFFFFF) }

func (s *PGAppliedSet) containsLSN(l uint64) bool {
	if l <= s.base {
		_, hole := s.holes[l]
		return !hole
	}
	_, ok := s.extra[l]
	return ok
}

func (s *PGAppliedSet) Contains(p model.Pos) bool {
	if p.Engine != model.EnginePG || p.LSN == 0 {
		return false
	}
	return s.containsLSN(p.LSN)
}

func (s *PGAppliedSet) Add(p model.Pos) {
	if p.Engine != model.EnginePG || p.LSN == 0 {
		return
	}
	if p.LSN <= s.base {
		delete(s.holes, p.LSN)
		return
	}
	s.extra[p.LSN] = struct{}{}
}

// Vouch dời mốc base lên newBase: bên gọi (biết mọi Txn đã đọc) khẳng định mọi Txn có LSN trong
// (base, newBase] đã apply, trừ pending (các Txn đã đọc mà chưa apply). Không làm gì nếu newBase ≤ base.
func (s *PGAppliedSet) Vouch(newBase uint64, pending []uint64) {
	if newBase <= s.base {
		return
	}
	for _, l := range pending {
		if l > s.base && l <= newBase {
			if _, applied := s.extra[l]; !applied {
				s.holes[l] = struct{}{}
			}
		}
	}
	for l := range s.extra {
		if l <= newBase {
			delete(s.extra, l)
		}
	}
	s.base = newBase
}

// Merge hợp hai tập theo đúng ngữ nghĩa Contains: kết quả chứa l ⇔ s chứa l hoặc other chứa l.
func (s *PGAppliedSet) Merge(other AppliedSet) {
	o, ok := other.(*PGAppliedSet)
	if !ok || o == nil {
		return
	}
	a := s.Clone().(*PGAppliedSet)
	newBase := a.base
	if o.base > newBase {
		newBase = o.base
	}
	holes := map[uint64]struct{}{}
	for _, src := range []map[uint64]struct{}{a.holes, o.holes} {
		for h := range src {
			if !a.containsLSN(h) && !o.containsLSN(h) {
				holes[h] = struct{}{}
			}
		}
	}
	// LSN nằm giữa base nhỏ và base lớn mà tập có base nhỏ chưa biết: tập base lớn đã xác nhận,
	// nên chỉ là hole nếu là hole của tập base lớn (đã xét ở trên).
	extra := map[uint64]struct{}{}
	for _, src := range []map[uint64]struct{}{a.extra, o.extra} {
		for l := range src {
			if l > newBase {
				extra[l] = struct{}{}
			}
		}
	}
	s.base, s.holes, s.extra = newBase, holes, extra
}

// Lwd: mọi Txn ≤ Lwd đã apply.
func (s *PGAppliedSet) Lwd() model.Pos {
	if len(s.holes) == 0 {
		return pgPos(s.base)
	}
	min := ^uint64(0)
	for h := range s.holes {
		if h < min {
			min = h
		}
	}
	return pgPos(min - 1)
}

// High: LSN lớn nhất đã apply (kể cả extra).
func (s *PGAppliedSet) High() model.Pos {
	h := s.base
	for l := range s.extra {
		if l > h {
			h = l
		}
	}
	return pgPos(h)
}

// Holes: các Txn đã biết là chưa apply trong (lwd, base]. Txn chưa apply giữa base và các extra
// không được biết (chỉ coordinator biết) nên không nằm ở đây.
func (s *PGAppliedSet) Holes() []model.Pos {
	if len(s.holes) == 0 {
		return nil
	}
	out := make([]model.Pos, 0, len(s.holes))
	for _, h := range sortedLSNs(s.holes) {
		out = append(out, pgPos(h))
	}
	return out
}

func (s *PGAppliedSet) Equal(other AppliedSet) bool {
	o, ok := other.(*PGAppliedSet)
	if !ok || o == nil {
		return false
	}
	return s.base == o.base && sameLSNSet(s.holes, o.holes) && sameLSNSet(s.extra, o.extra)
}

func (s *PGAppliedSet) Clone() AppliedSet {
	if s == nil {
		return nil
	}
	c := NewPGAppliedSet(s.base)
	for h := range s.holes {
		c.holes[h] = struct{}{}
	}
	for l := range s.extra {
		c.extra[l] = struct{}{}
	}
	return c
}

// subtract loại khỏi s các LSN extra đã có trong g (dùng để thu nhỏ row delta sau compaction).
// Phần ≤ base giữ nguyên (thận trọng: chỉ có thể làm delta lớn hơn, không làm mất Txn).
func (s *PGAppliedSet) subtract(g *PGAppliedSet) {
	for l := range s.extra {
		if g.containsLSN(l) {
			delete(s.extra, l)
		}
	}
}

func (s *PGAppliedSet) isEmpty() bool {
	return s.base == 0 && len(s.extra) == 0 && len(s.holes) == 0
}

// PGAppliedJSON là dạng lưu của row checkpoint PG (02 §4). Applied chứa các LSN > high đã apply
// (row delta ở relaxed trước khi coordinator xác nhận mốc mới); rỗng thì bỏ khỏi JSON.
type PGAppliedJSON struct {
	Lwd     string   `json:"lwd"`
	High    string   `json:"high"`
	Holes   []string `json:"holes"`
	Applied []string `json:"applied,omitempty"`
}

func (s *PGAppliedSet) String() string {
	if s == nil || s.isEmpty() {
		return ""
	}
	pj := PGAppliedJSON{Lwd: FormatLSN(s.Lwd().LSN), High: FormatLSN(s.base), Holes: []string{}}
	for _, h := range sortedLSNs(s.holes) {
		pj.Holes = append(pj.Holes, FormatLSN(h))
	}
	for _, l := range sortedLSNs(s.extra) {
		pj.Applied = append(pj.Applied, FormatLSN(l))
	}
	b, _ := json.Marshal(pj)
	return string(b)
}

// parsePGApplied đọc dạng JSON {"lwd","high","holes","applied"} hoặc danh sách LSN cách nhau bởi dấu phẩy.
func parsePGApplied(str string) (*PGAppliedSet, error) {
	str = strings.TrimSpace(str)
	set := NewPGAppliedSet(0)
	if str == "" {
		return set, nil
	}
	if strings.HasPrefix(str, "{") {
		var pj PGAppliedJSON
		if err := json.Unmarshal([]byte(str), &pj); err != nil {
			return nil, fmt.Errorf("parse PG applied JSON: %w", err)
		}
		if pj.High != "" {
			high, err := ParseLSN(pj.High)
			if err != nil {
				return nil, fmt.Errorf("parse PG applied high %q: %w", pj.High, err)
			}
			set.base = high
		}
		var lwd uint64
		if pj.Lwd != "" {
			v, err := ParseLSN(pj.Lwd)
			if err != nil {
				return nil, fmt.Errorf("parse PG applied lwd %q: %w", pj.Lwd, err)
			}
			lwd = v
		}
		for _, hs := range pj.Holes {
			h, err := ParseLSN(hs)
			if err != nil {
				return nil, fmt.Errorf("parse PG applied hole %q: %w", hs, err)
			}
			if h <= lwd || h > set.base {
				return nil, fmt.Errorf("PG applied hole %s outside (lwd %s, high %s]", hs, pj.Lwd, pj.High)
			}
			set.holes[h] = struct{}{}
		}
		for _, as := range pj.Applied {
			l, err := ParseLSN(as)
			if err != nil {
				return nil, fmt.Errorf("parse PG applied LSN %q: %w", as, err)
			}
			set.Add(pgPos(l))
		}
		if got := set.Lwd().LSN; pj.Lwd != "" && got != lwd {
			return nil, fmt.Errorf("PG applied lwd %s inconsistent with holes (expected %s)", pj.Lwd, FormatLSN(got))
		}
		return set, nil
	}
	for _, p := range strings.Split(str, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		l, err := ParseLSN(p)
		if err != nil {
			return nil, fmt.Errorf("parse PG applied LSN %q: %w", p, err)
		}
		set.Add(pgPos(l))
	}
	return set, nil
}

func sortedLSNs(m map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(m))
	for l := range m {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sameLSNSet(a, b map[uint64]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for l := range a {
		if _, ok := b[l]; !ok {
			return false
		}
	}
	return true
}

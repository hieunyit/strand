package checkpoint

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"strand/internal/model"
)

// spec: 02 §3

// AppliedSet biểu diễn tập các transaction đã apply (GTID set với MySQL; tập LSN kèm holes với PG).
type AppliedSet interface {
	Contains(model.Pos) bool
	Add(model.Pos)
	Merge(AppliedSet)
	Lwd() model.Pos
	High() model.Pos
	Holes() []model.Pos
	Equal(AppliedSet) bool
	Clone() AppliedSet
}

// -----------------------------------------------------------------------------
// PGAppliedSet
// -----------------------------------------------------------------------------

// PGAppliedSet cài đặt AppliedSet cho PostgreSQL dựa trên LSN và RangeSet.
type PGAppliedSet struct {
	ranges *RangeSet
}

// NewPGAppliedSet tạo mới PGAppliedSet với baseLSN quy định (mặc định 1).
func NewPGAppliedSet(baseLSN uint64) *PGAppliedSet {
	return &PGAppliedSet{
		ranges: NewRangeSet(baseLSN),
	}
}

func (s *PGAppliedSet) Clone() AppliedSet {
	if s == nil {
		return nil
	}
	return &PGAppliedSet{
		ranges: s.ranges.Clone(),
	}
}

func (s *PGAppliedSet) Contains(p model.Pos) bool {
	if p.Engine != model.EnginePG {
		return false
	}
	return s.ranges.Contains(p.LSN)
}

func (s *PGAppliedSet) Add(p model.Pos) {
	if p.Engine != model.EnginePG || p.LSN == 0 {
		return
	}
	s.ranges.Add(p.LSN)
}

func (s *PGAppliedSet) Merge(other AppliedSet) {
	if other == nil {
		return
	}
	if pgOther, ok := other.(*PGAppliedSet); ok {
		s.ranges.Merge(pgOther.ranges)
	}
}

func (s *PGAppliedSet) Lwd() model.Pos {
	return model.Pos{
		Engine: model.EnginePG,
		LSN:    s.ranges.Lwd(),
	}
}

func (s *PGAppliedSet) High() model.Pos {
	return model.Pos{
		Engine: model.EnginePG,
		LSN:    s.ranges.High(),
	}
}

func (s *PGAppliedSet) Holes() []model.Pos {
	holes := s.ranges.Holes()
	if len(holes) == 0 {
		return nil
	}
	res := make([]model.Pos, len(holes))
	for i, h := range holes {
		res[i] = model.Pos{
			Engine: model.EnginePG,
			LSN:    h,
		}
	}
	return res
}

func (s *PGAppliedSet) Equal(other AppliedSet) bool {
	if other == nil {
		return false
	}
	pgOther, ok := other.(*PGAppliedSet)
	if !ok {
		return false
	}
	return s.ranges.Equal(pgOther.ranges)
}

// -----------------------------------------------------------------------------
// MySQLAppliedSet
// -----------------------------------------------------------------------------

// MySQLAppliedSet cài đặt AppliedSet cho MySQL quản lý GTID theo UUID và GNO.
type MySQLAppliedSet struct {
	defaultUUID string
	sets        map[string]*RangeSet
}

// NewMySQLAppliedSet khởi tạo MySQLAppliedSet với UUID mặc định (nếu không truyền sẽ dùng "default-uuid").
func NewMySQLAppliedSet(defaultUUID ...string) *MySQLAppliedSet {
	def := "default-uuid"
	if len(defaultUUID) > 0 && defaultUUID[0] != "" {
		def = defaultUUID[0]
	}
	return &MySQLAppliedSet{
		defaultUUID: def,
		sets:        make(map[string]*RangeSet),
	}
}

func (s *MySQLAppliedSet) parseGTID(gtid string) (string, uint64, error) {
	gtid = strings.TrimSpace(gtid)
	if gtid == "" {
		return "", 0, fmt.Errorf("empty gtid")
	}

	parts := strings.Split(gtid, ":")
	if len(parts) == 1 {
		// Chỉ có GNO
		gno, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return "", 0, err
		}
		return s.defaultUUID, gno, nil
	}

	gnoStr := parts[len(parts)-1]
	gno, err := strconv.ParseUint(gnoStr, 10, 64)
	if err != nil {
		return "", 0, err
	}

	prefix := strings.Join(parts[:len(parts)-1], ":")
	return prefix, gno, nil
}

func (s *MySQLAppliedSet) getOrCreateRangeSet(prefix string) *RangeSet {
	rs, ok := s.sets[prefix]
	if !ok {
		rs = NewRangeSet(1) // MySQL GNO bắt đầu từ 1
		s.sets[prefix] = rs
	}
	return rs
}

func (s *MySQLAppliedSet) Clone() AppliedSet {
	if s == nil {
		return nil
	}
	res := &MySQLAppliedSet{
		defaultUUID: s.defaultUUID,
		sets:        make(map[string]*RangeSet, len(s.sets)),
	}
	for k, v := range s.sets {
		res.sets[k] = v.Clone()
	}
	return res
}

func (s *MySQLAppliedSet) Contains(p model.Pos) bool {
	if p.Engine != model.EngineMySQL {
		return false
	}
	prefix, gno, err := s.parseGTID(p.GTID)
	if err != nil {
		return false
	}
	rs, ok := s.sets[prefix]
	if !ok {
		return false
	}
	return rs.Contains(gno)
}

func (s *MySQLAppliedSet) Add(p model.Pos) {
	if p.Engine != model.EngineMySQL {
		return
	}
	prefix, gno, err := s.parseGTID(p.GTID)
	if err != nil || gno == 0 {
		return
	}
	rs := s.getOrCreateRangeSet(prefix)
	rs.Add(gno)
}

func (s *MySQLAppliedSet) Merge(other AppliedSet) {
	if other == nil {
		return
	}
	myOther, ok := other.(*MySQLAppliedSet)
	if !ok {
		return
	}
	for prefix, otherRS := range myOther.sets {
		rs := s.getOrCreateRangeSet(prefix)
		rs.Merge(otherRS)
	}
}

func (s *MySQLAppliedSet) primaryPrefix() string {
	if _, ok := s.sets[s.defaultUUID]; ok {
		return s.defaultUUID
	}
	var keys []string
	for k := range s.sets {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return s.defaultUUID
	}
	sort.Strings(keys)
	return keys[0]
}

func (s *MySQLAppliedSet) Lwd() model.Pos {
	prefix := s.primaryPrefix()
	rs, ok := s.sets[prefix]
	if !ok || rs.IsEmpty() {
		return model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:0", prefix)}
	}
	return model.Pos{
		Engine: model.EngineMySQL,
		GTID:   fmt.Sprintf("%s:%d", prefix, rs.Lwd()),
	}
}

func (s *MySQLAppliedSet) High() model.Pos {
	prefix := s.primaryPrefix()
	rs, ok := s.sets[prefix]
	if !ok || rs.IsEmpty() {
		return model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:0", prefix)}
	}
	return model.Pos{
		Engine: model.EngineMySQL,
		GTID:   fmt.Sprintf("%s:%d", prefix, rs.High()),
	}
}

func (s *MySQLAppliedSet) Holes() []model.Pos {
	var res []model.Pos

	var keys []string
	for k := range s.sets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, prefix := range keys {
		rs := s.sets[prefix]
		for _, h := range rs.Holes() {
			res = append(res, model.Pos{
				Engine: model.EngineMySQL,
				GTID:   fmt.Sprintf("%s:%d", prefix, h),
			})
		}
	}
	return res
}

func (s *MySQLAppliedSet) Equal(other AppliedSet) bool {
	if other == nil {
		return false
	}
	myOther, ok := other.(*MySQLAppliedSet)
	if !ok {
		return false
	}

	// So sánh các tập sets
	allKeys := make(map[string]struct{})
	for k := range s.sets {
		allKeys[k] = struct{}{}
	}
	for k := range myOther.sets {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		rs1 := s.sets[k]
		rs2 := myOther.sets[k]
		empty1 := rs1 == nil || rs1.IsEmpty()
		empty2 := rs2 == nil || rs2.IsEmpty()
		if empty1 && empty2 {
			continue
		}
		if empty1 != empty2 {
			return false
		}
		if !rs1.Equal(rs2) {
			return false
		}
	}
	return true
}

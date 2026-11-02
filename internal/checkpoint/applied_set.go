package checkpoint

import (
	"encoding/json"
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

// PGAppliedJSON biểu diễn JSON metadata applied của PostgreSQL theo spec/02 §4.
type PGAppliedJSON struct {
	Lwd   string   `json:"lwd"`
	High  string   `json:"high"`
	Holes []string `json:"holes"`
}

func (s *PGAppliedSet) String() string {
	if s == nil || s.ranges == nil || s.ranges.IsEmpty() {
		return ""
	}
	lwdPos := s.Lwd()
	highPos := s.High()
	holesPos := s.Holes()

	holesStr := make([]string, len(holesPos))
	for i, h := range holesPos {
		holesStr[i] = fmt.Sprintf("%X/%X", h.LSN>>32, h.LSN&0xFFFFFFFF)
	}

	payload := PGAppliedJSON{
		Lwd:   fmt.Sprintf("%X/%X", lwdPos.LSN>>32, lwdPos.LSN&0xFFFFFFFF),
		High:  fmt.Sprintf("%X/%X", highPos.LSN>>32, highPos.LSN&0xFFFFFFFF),
		Holes: holesStr,
	}
	bytes, _ := json.Marshal(payload)
	return string(bytes)
}

// ParseLSN phân tích chuỗi LSN dạng "X/Y" (hex) hoặc decimal thành uint64.
func ParseLSN(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty LSN")
	}
	parts := strings.Split(s, "/")
	if len(parts) == 2 {
		hi, err := strconv.ParseUint(parts[0], 16, 32)
		if err != nil {
			return 0, err
		}
		lo, err := strconv.ParseUint(parts[1], 16, 32)
		if err != nil {
			return 0, err
		}
		return (hi << 32) | lo, nil
	}
	return strconv.ParseUint(s, 10, 64)
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

	// UUID và tag của MySQL không phân biệt hoa thường; chuẩn hóa về chữ thường.
	prefix := strings.ToLower(strings.Join(parts[:len(parts)-1], ":"))
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
	_ = s.addSegment(p.GTID)
}

// ParseAndAddRange phân tích một phân đoạn GTID set và thêm vào tập.
// Phân đoạn hỏng bị bỏ qua; dùng AddGTIDSet khi cần biết lỗi.
func (s *MySQLAppliedSet) ParseAndAddRange(segment string) {
	_ = s.addSegment(segment)
}

// AddGTIDSet thêm một GTID set đầy đủ (các phân đoạn cách nhau bởi dấu phẩy/xuống dòng),
// theo đúng định dạng của MySQL kể cả tagged GTID: "uuid:1-5:tag:1-3,uuid2:7".
// Trả lỗi nếu có phân đoạn hỏng; khi lỗi, tập không bị thay đổi.
func (s *MySQLAppliedSet) AddGTIDSet(set string) error {
	tmp := NewMySQLAppliedSet(s.defaultUUID)
	for _, seg := range splitGTIDSet(set) {
		if err := tmp.addSegment(seg); err != nil {
			return err
		}
	}
	s.Merge(tmp)
	return nil
}

// ParseMySQLGTIDSet phân tích một GTID set, trả lỗi nếu hỏng.
func ParseMySQLGTIDSet(set string, defaultUUID ...string) (*MySQLAppliedSet, error) {
	s := NewMySQLAppliedSet(defaultUUID...)
	if err := s.AddGTIDSet(set); err != nil {
		return nil, err
	}
	return s, nil
}

func parseInterval(tok string) (uint64, uint64, bool) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return 0, 0, false
	}
	a, b, isRange := strings.Cut(tok, "-")
	start, err := strconv.ParseUint(strings.TrimSpace(a), 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end := start
	if isRange {
		if end, err = strconv.ParseUint(strings.TrimSpace(b), 10, 64); err != nil {
			return 0, 0, false
		}
	}
	return start, end, true
}

// looksNumeric: token có dạng interval "n" hoặc "a-b" (chỉ chữ số và một dấu '-').
// UUID như "3e11fa47-..." có chữ cái nên không khớp; tag MySQL không được bắt đầu bằng chữ số.
func looksNumeric(tok string) bool {
	tok = strings.TrimSpace(tok)
	if tok == "" || tok[0] < '0' || tok[0] > '9' {
		return false
	}
	dashes := 0
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		switch {
		case c >= '0' && c <= '9':
		case c == '-':
			dashes++
		case c == ' ':
		default:
			return false
		}
	}
	return dashes <= 1
}

// addSegment phân tích "uuid[:interval|:tag]..." trong đó interval sau một tag thuộc về uuid:tag.
func (s *MySQLAppliedSet) addSegment(segment string) error {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return nil
	}
	parts := strings.Split(segment, ":")

	type ival struct {
		prefix     string
		start, end uint64
	}
	var ivals []ival

	var uuid string
	i := 0
	if looksNumeric(parts[0]) {
		uuid = s.defaultUUID // chỉ có GNO/interval, dùng UUID mặc định
	} else {
		uuid = strings.ToLower(strings.TrimSpace(parts[0]))
		if uuid == "" {
			return fmt.Errorf("invalid GTID segment %q: empty source id", segment)
		}
		i = 1
	}
	if i >= len(parts) {
		return fmt.Errorf("invalid GTID segment %q: no intervals", segment)
	}

	prefix := uuid
	pendingTag := false
	for ; i < len(parts); i++ {
		tok := strings.TrimSpace(parts[i])
		if tok == "" {
			return fmt.Errorf("invalid GTID segment %q: empty element", segment)
		}
		if looksNumeric(tok) {
			start, end, ok := parseInterval(tok)
			if !ok || start == 0 || start > end {
				return fmt.Errorf("invalid GTID interval %q in %q", tok, segment)
			}
			ivals = append(ivals, ival{prefix, start, end})
			pendingTag = false
			continue
		}
		if pendingTag {
			return fmt.Errorf("invalid GTID segment %q: tag %q has no intervals", segment, parts[i-1])
		}
		prefix = uuid + ":" + strings.ToLower(tok)
		pendingTag = true
	}
	if pendingTag {
		return fmt.Errorf("invalid GTID segment %q: trailing tag without intervals", segment)
	}
	for _, iv := range ivals {
		s.getOrCreateRangeSet(iv.prefix).AddRange(iv.start, iv.end)
	}
	return nil
}

// Subtract loại khỏi s mọi GTID có trong other.
func (s *MySQLAppliedSet) Subtract(other *MySQLAppliedSet) {
	if other == nil {
		return
	}
	for prefix, rs := range s.sets {
		if ors, ok := other.sets[prefix]; ok {
			rs.Subtract(ors)
		}
	}
}

// IsSubsetOf kiểm tra mọi GTID của s đều có trong other.
func (s *MySQLAppliedSet) IsSubsetOf(other *MySQLAppliedSet) bool {
	for prefix, rs := range s.sets {
		if rs.IsEmpty() {
			continue
		}
		if other == nil {
			return false
		}
		ors, ok := other.sets[prefix]
		if !ok || !rs.IsSubsetOf(ors) {
			return false
		}
	}
	return true
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

func (s *MySQLAppliedSet) String() string {
	if s == nil || len(s.sets) == 0 {
		return ""
	}
	var prefixes []string
	for k := range s.sets {
		prefixes = append(prefixes, k)
	}
	sort.Strings(prefixes)

	var parts []string
	for _, prefix := range prefixes {
		rs := s.sets[prefix]
		if rs == nil || rs.IsEmpty() {
			continue
		}
		var ivStrs []string
		for _, it := range rs.Intervals() {
			if it.Start == it.End {
				ivStrs = append(ivStrs, fmt.Sprintf("%d", it.Start))
			} else {
				ivStrs = append(ivStrs, fmt.Sprintf("%d-%d", it.Start, it.End))
			}
		}
		if len(ivStrs) > 0 {
			parts = append(parts, fmt.Sprintf("%s:%s", prefix, strings.Join(ivStrs, ":")))
		}
	}
	return strings.Join(parts, ",")
}

package checkpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"strand/internal/model"
)

// Chế độ vị trí binlog (source MySQL KHÔNG có GTID: 5.5, hoặc 5.6/5.7 gtid_mode=OFF).
//
// Một Txn được định danh bằng vị trí KẾT THÚC của nó: (file binlog, end_log_pos của event XID/COMMIT
// hoặc của câu DDL). Vị trí đó được ánh xạ thành một số đơn điệu "khóa":
//
//	khóa = seq(file) << 32 | offset
//
// với seq(file) là hậu tố số của tên file ("mysql-bin.000123" → 123) và offset < 2^32 (file binlog
// luôn < 4 GiB vì max_binlog_size ≤ 1 GiB). Hai Txn trong cùng một luồng binlog có khóa tăng ngặt
// theo thứ tự commit, nên tập đã apply dùng được cùng mô hình với PGAppliedSet (mốc base + extra +
// holes), nhưng đọc lại theo VỊ TRÍ chứ không theo slot.
//
// Các bất biến (CP-1..CP-5) được lập luận ở MySQLPosAppliedSet và ở engine.go (lsnTracker).

// MaxBinlogOffset: offset lớn nhất biểu diễn được trong khóa (2^32 - 1).
const MaxBinlogOffset = math.MaxUint32

// minBinlogOffset: file binlog bắt đầu bằng 4 byte magic nên mọi vị trí hợp lệ ≥ 4.
const minBinlogOffset = 4

// BinlogPosFormatVersion là phiên bản của dạng JSON lưu tập đã apply ở chế độ vị trí binlog.
const BinlogPosFormatVersion = 1

// BinlogPosMode là giá trị trường "mode" của JSON.
const BinlogPosMode = "binlog_pos"

// FormatBinlogName dựng tên file từ tên gốc và số thứ tự: MySQL luôn đệm tối thiểu 6 chữ số.
func FormatBinlogName(base string, seq uint32) string {
	return fmt.Sprintf("%s.%06d", base, seq)
}

// SplitBinlogName tách "mysql-bin.000123" thành ("mysql-bin", 123). Tên không đúng dạng chuẩn
// (thiếu hậu tố số, hậu tố không phải dạng FormatBinlogName tạo ra, seq tràn uint32) bị từ chối:
// không đoán, vì khóa sai là mất hoặc apply lại dữ liệu.
func SplitBinlogName(name string) (base string, seq uint32, err error) {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", 0, fmt.Errorf("binlog file name %q has no numeric suffix (want <base>.<number>)", name)
	}
	n, perr := strconv.ParseUint(name[i+1:], 10, 32)
	if perr != nil {
		return "", 0, fmt.Errorf("binlog file name %q: suffix %q is not a 32-bit number", name, name[i+1:])
	}
	base, seq = name[:i], uint32(n)
	if FormatBinlogName(base, seq) != name {
		return "", 0, fmt.Errorf("binlog file name %q is not in canonical form %q", name, FormatBinlogName(base, seq))
	}
	return base, seq, nil
}

// BinlogKey ghép (seq, offset) thành khóa đơn điệu. Offset phải nằm trong [4, 2^32).
func BinlogKey(seq uint32, offset uint64) (uint64, error) {
	if offset < minBinlogOffset || offset > MaxBinlogOffset {
		return 0, fmt.Errorf("binlog offset %d outside [%d, 2^32): binlog files must be < 4 GiB (max_binlog_size <= 1 GiB)", offset, minBinlogOffset)
	}
	return uint64(seq)<<32 | offset, nil
}

// SplitBinlogKey là phép ngược của BinlogKey.
func SplitBinlogKey(key uint64) (seq uint32, offset uint64) {
	return uint32(key >> 32), key & 0xFFFFFFFF
}

// KeyOfPos đổi Pos chế độ vị trí binlog thành khóa; lỗi nếu Pos không phải vị trí binlog, hoặc tên
// gốc của file khác fileBase (log-bin basename bị đổi: không so sánh được với tập đã apply).
func KeyOfPos(fileBase string, p model.Pos) (uint64, error) {
	if !p.IsBinlogPos() {
		return 0, fmt.Errorf("position %s is not a binlog file position", p)
	}
	base, seq, err := SplitBinlogName(p.File)
	if err != nil {
		return 0, err
	}
	if base != fileBase {
		return 0, fmt.Errorf("binlog file %q has base name %q but the checkpoint was made for %q (log-bin basename changed): re-seed required", p.File, base, fileBase)
	}
	return BinlogKey(seq, p.BinlogPos)
}

// PosOfKey dựng Pos từ khóa và tên gốc.
func PosOfKey(fileBase string, key uint64) model.Pos {
	seq, off := SplitBinlogKey(key)
	return model.Pos{Engine: model.EngineMySQL, File: FormatBinlogName(fileBase, seq), BinlogPos: off}
}

// FormatBinlogKey in khóa dạng "SEQ:OFFSET" ("000123:4567"), dạng lưu trong checkpoint.
func FormatBinlogKey(key uint64) string {
	seq, off := SplitBinlogKey(key)
	return fmt.Sprintf("%06d:%d", seq, off)
}

// fmtKeyOpt/parseKeyOpt: khóa 0 ("chưa có mốc", ví dụ tập của worker chỉ có extra) được lưu là chuỗi rỗng.
func fmtKeyOpt(k uint64) string {
	if k == 0 {
		return ""
	}
	return FormatBinlogKey(k)
}

func parseKeyOpt(s string) (uint64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	return ParseBinlogKey(s)
}

// ParseBinlogKey đọc "SEQ:OFFSET" (SEQ: số thập phân, OFFSET trong [4, 2^32)).
func ParseBinlogKey(s string) (uint64, error) {
	a, b, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok {
		return 0, fmt.Errorf("binlog position %q: want SEQ:OFFSET", s)
	}
	seq, err := strconv.ParseUint(a, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("binlog position %q: bad file sequence: %w", s, err)
	}
	off, err := strconv.ParseUint(b, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("binlog position %q: bad offset: %w", s, err)
	}
	return BinlogKey(uint32(seq), off)
}

// ParseBinlogStartAt đọc tham số --start-at dạng "mysql-bin.000123:4567" thành (tên gốc, khóa).
func ParseBinlogStartAt(s string) (fileBase string, key uint64, err error) {
	s = strings.TrimSpace(s)
	i := strings.LastIndexByte(s, ':')
	if i <= 0 {
		return "", 0, fmt.Errorf("want <binlog file>:<position> (for example mysql-bin.000123:4567), got %q", s)
	}
	name := s[:i]
	off, perr := strconv.ParseUint(s[i+1:], 10, 64)
	if perr != nil {
		return "", 0, fmt.Errorf("start position %q: bad offset: %w", s, perr)
	}
	base, seq, err := SplitBinlogName(name)
	if err != nil {
		return "", 0, err
	}
	key, err = BinlogKey(seq, off)
	if err != nil {
		return "", 0, err
	}
	return base, key, nil
}

// BinlogSpan là một Txn đã đọc: [Start, End] theo khóa (Start = vị trí bắt đầu event đầu tiên,
// End = vị trí kết thúc). Cùng file: Txn không bao giờ vắt qua hai file binlog.
type BinlogSpan struct {
	Start, End uint64
}

// MySQLPosAppliedSet là tập Txn MySQL (không GTID) đã apply, định danh bằng khóa vị trí kết thúc.
// Cùng ngữ nghĩa với PGAppliedSet, thêm vị trí BẮT ĐẦU của mỗi hole để biết đọc lại từ đâu:
//
//   - base: mốc đã xác nhận — mọi Txn có khóa kết thúc ≤ base đã apply, TRỪ các hole. base luôn là
//     một ranh giới event (đầu event đầu tiên của Txn chưa apply sớm nhất, hoặc kết thúc Txn cuối
//     cùng đã nhận, hoặc vị trí --start-at), nên đọc lại tại base là hợp lệ.
//   - holes: end → start của các Txn ≤ base biết là CHƯA apply.
//   - extra: khóa kết thúc > base đã apply (tường minh; row delta của worker ở relaxed).
//
// Lập luận bất biến:
//   - CP-2 (không apply hai lần): router bỏ Txn có Contains(vị trí kết thúc) = true. Đọc lại từ
//     Lwd() (≤ mọi Txn chưa apply) sẽ gặp lại các Txn đã apply nằm sau Lwd; chúng ở trong extra
//     hoặc ≤ base-không-phải-hole nên bị bỏ.
//   - CP-1 (không mất): một khóa chỉ nằm trong tập khi (a) Add — bên gọi Add cùng transaction với dữ
//     liệu, hoặc (b) Vouch — bên gọi biết luồng Txn khẳng định mọi Txn kết thúc ≤ mốc đã commit
//     (trừ pending). Hai thứ đó là toàn bộ đường vào của tập.
//   - CP-3: chỉ (file, end_log_pos) được ghi; seq nội bộ không bao giờ.
//   - CP-4: bộ nhớ/JSON tỷ lệ với |holes| + |extra|, không với giá trị khóa.
type MySQLPosAppliedSet struct {
	fileBase string
	source   string // định danh server nguồn (server_id[/server_uuid]); chỉ ở row toàn cục
	base     uint64
	holes    map[uint64]uint64 // end → start
	extra    map[uint64]struct{}
}

// NewMySQLPosAppliedSet tạo tập với tên gốc file và mốc base (0 = tập rỗng).
func NewMySQLPosAppliedSet(fileBase string, base uint64) *MySQLPosAppliedSet {
	return &MySQLPosAppliedSet{fileBase: fileBase, base: base, holes: map[uint64]uint64{}, extra: map[uint64]struct{}{}}
}

// FileBase: tên gốc của file binlog ("mysql-bin"); rỗng khi tập chưa gắn với luồng nào.
func (s *MySQLPosAppliedSet) FileBase() string { return s.fileBase }

// Source/SetSource: định danh server nguồn mà vị trí thuộc về (xem pipeline: kiểm khi resume).
func (s *MySQLPosAppliedSet) Source() string       { return s.source }
func (s *MySQLPosAppliedSet) SetSource(src string) { s.source = src }

// Base là mốc đã xác nhận (khóa).
func (s *MySQLPosAppliedSet) Base() uint64 { return s.base }

func (s *MySQLPosAppliedSet) containsKey(k uint64) bool {
	if k <= s.base {
		_, hole := s.holes[k]
		return !hole
	}
	_, ok := s.extra[k]
	return ok
}

// ContainsKey: khóa kết thúc k đã apply.
func (s *MySQLPosAppliedSet) ContainsKey(k uint64) bool { return s.containsKey(k) }

func (s *MySQLPosAppliedSet) Contains(p model.Pos) bool {
	k, err := KeyOfPos(s.fileBase, p)
	if err != nil {
		// Không phải vị trí binlog, hoặc khác tên gốc: không khẳng định đã apply. Router kiểm tên gốc
		// trước (checkBinlogPos) nên Txn tới được đây luôn cùng tên gốc.
		return false
	}
	return s.containsKey(k)
}

// Add ghi nhận Txn kết thúc tại p đã apply. Vị trí sai tên gốc là lỗi lập trình (router đã kiểm):
// bỏ qua im lặng sẽ làm Txn bị apply lại sau restart (CP-2), nên panic để dừng TRƯỚC khi commit.
func (s *MySQLPosAppliedSet) Add(p model.Pos) {
	if s.fileBase == "" && s.isEmpty() && p.IsBinlogPos() {
		if b, _, err := SplitBinlogName(p.File); err == nil {
			s.fileBase = b
		}
	}
	k, err := KeyOfPos(s.fileBase, p)
	if err != nil {
		panic(fmt.Sprintf("checkpoint: MySQLPosAppliedSet.Add(%s): %v", p, err))
	}
	s.AddKey(k)
}

// AddKey: như Add, theo khóa.
func (s *MySQLPosAppliedSet) AddKey(k uint64) {
	if k <= s.base {
		delete(s.holes, k)
		return
	}
	s.extra[k] = struct{}{}
}

// Vouch dời mốc base lên newBase: bên gọi (biết mọi Txn đã đọc) khẳng định mọi Txn có khóa kết thúc
// trong (base, newBase] đã apply, trừ pending (Txn đã đọc mà chưa apply — thành hole, kèm vị trí bắt
// đầu để đọc lại). newBase phải là ranh giới Txn: không Txn pending nào được vắt qua newBase
// (Start < newBase < End), vì khi đó đọc lại tại base sẽ cắt giữa Txn — lỗi, không đổi gì.
// Không làm gì nếu newBase ≤ base.
func (s *MySQLPosAppliedSet) Vouch(newBase uint64, pending []BinlogSpan) error {
	if newBase <= s.base {
		return nil
	}
	for _, p := range pending {
		if p.Start >= p.End {
			return fmt.Errorf("vouch: pending txn has start %s >= end %s", FormatBinlogKey(p.Start), FormatBinlogKey(p.End))
		}
		if p.End > s.base && p.End > newBase && p.Start < newBase {
			return fmt.Errorf("vouch to %s would cut pending txn [%s, %s]", FormatBinlogKey(newBase), FormatBinlogKey(p.Start), FormatBinlogKey(p.End))
		}
	}
	for _, p := range pending {
		if p.End > s.base && p.End <= newBase {
			if _, applied := s.extra[p.End]; !applied {
				s.holes[p.End] = p.Start
			}
		}
	}
	for k := range s.extra {
		if k <= newBase {
			delete(s.extra, k)
		}
	}
	s.base = newBase
	return nil
}

// Merge hợp hai tập theo đúng ngữ nghĩa Contains: kết quả chứa k ⇔ s chứa k hoặc other chứa k.
// Khác tên gốc (cả hai khác rỗng): không gộp — dùng MergeChecked để nhận lỗi.
func (s *MySQLPosAppliedSet) Merge(other AppliedSet) {
	o, ok := other.(*MySQLPosAppliedSet)
	if !ok || o == nil {
		return
	}
	if s.fileBase == "" {
		s.fileBase = o.fileBase
	} else if o.fileBase != "" && o.fileBase != s.fileBase {
		return
	}
	if s.source == "" {
		s.source = o.source
	}
	a := s.Clone().(*MySQLPosAppliedSet)
	newBase := a.base
	if o.base > newBase {
		newBase = o.base
	}
	holes := map[uint64]uint64{}
	for _, src := range []map[uint64]uint64{a.holes, o.holes} {
		for h, start := range src {
			if !a.containsKey(h) && !o.containsKey(h) {
				holes[h] = start
			}
		}
	}
	extra := map[uint64]struct{}{}
	for _, src := range []map[uint64]struct{}{a.extra, o.extra} {
		for k := range src {
			if k > newBase {
				extra[k] = struct{}{}
			}
		}
	}
	s.base, s.holes, s.extra = newBase, holes, extra
}

// Lwd: mọi Txn kết thúc ≤ Lwd đã apply, và Lwd là ranh giới event nên đọc lại tại đó là hợp lệ
// (CP-3). Không hole: base. Có hole: vị trí BẮT ĐẦU sớm nhất của các hole (mọi Txn trước đó đã apply).
// Tập rỗng: Pos zero.
func (s *MySQLPosAppliedSet) Lwd() model.Pos {
	if s.isEmpty() || s.fileBase == "" {
		return model.Pos{}
	}
	return PosOfKey(s.fileBase, s.LwdKey())
}

// LwdKey như Lwd, dạng khóa.
func (s *MySQLPosAppliedSet) LwdKey() uint64 {
	if len(s.holes) == 0 {
		return s.base
	}
	min := ^uint64(0)
	for _, st := range s.holes {
		if st < min {
			min = st
		}
	}
	return min
}

// High: khóa lớn nhất đã apply (kể cả extra).
func (s *MySQLPosAppliedSet) High() model.Pos {
	if s.isEmpty() || s.fileBase == "" {
		return model.Pos{}
	}
	return PosOfKey(s.fileBase, s.HighKey())
}

// HighKey như High, dạng khóa.
func (s *MySQLPosAppliedSet) HighKey() uint64 {
	h := s.base
	for k := range s.extra {
		if k > h {
			h = k
		}
	}
	return h
}

// Holes: vị trí kết thúc của các Txn ≤ base biết là chưa apply, tăng dần.
func (s *MySQLPosAppliedSet) Holes() []model.Pos {
	if len(s.holes) == 0 {
		return nil
	}
	ends := make([]uint64, 0, len(s.holes))
	for e := range s.holes {
		ends = append(ends, e)
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i] < ends[j] })
	out := make([]model.Pos, len(ends))
	for i, e := range ends {
		out[i] = PosOfKey(s.fileBase, e)
	}
	return out
}

// NumHoles/NumExtra phục vụ kiểm CP-4.
func (s *MySQLPosAppliedSet) NumHoles() int { return len(s.holes) }
func (s *MySQLPosAppliedSet) NumExtra() int { return len(s.extra) }

func (s *MySQLPosAppliedSet) Equal(other AppliedSet) bool {
	o, ok := other.(*MySQLPosAppliedSet)
	if !ok || o == nil {
		return false
	}
	if s.fileBase != o.fileBase || s.base != o.base || len(s.holes) != len(o.holes) || len(s.extra) != len(o.extra) {
		return false
	}
	for e, st := range s.holes {
		if ost, ok := o.holes[e]; !ok || ost != st {
			return false
		}
	}
	for k := range s.extra {
		if _, ok := o.extra[k]; !ok {
			return false
		}
	}
	return true
}

func (s *MySQLPosAppliedSet) Clone() AppliedSet {
	if s == nil {
		return nil
	}
	c := NewMySQLPosAppliedSet(s.fileBase, s.base)
	c.source = s.source
	for e, st := range s.holes {
		c.holes[e] = st
	}
	for k := range s.extra {
		c.extra[k] = struct{}{}
	}
	return c
}

// subtract loại khỏi s các khóa extra đã có trong g (thu nhỏ row delta sau compaction). Phần ≤ base
// giữ nguyên (thận trọng: chỉ làm delta lớn hơn, không làm mất Txn).
func (s *MySQLPosAppliedSet) subtract(g *MySQLPosAppliedSet) {
	if g.fileBase != s.fileBase && s.fileBase != "" && g.fileBase != "" {
		return
	}
	for k := range s.extra {
		if g.containsKey(k) {
			delete(s.extra, k)
		}
	}
}

func (s *MySQLPosAppliedSet) isEmpty() bool {
	return s.base == 0 && len(s.extra) == 0 && len(s.holes) == 0
}

// IsEmpty: chưa có vị trí nào.
func (s *MySQLPosAppliedSet) IsEmpty() bool { return s == nil || s.isEmpty() }

type posHoleJSON struct {
	End   string `json:"end"`
	Start string `json:"start"`
}

// posAppliedJSON là dạng lưu của row checkpoint ở chế độ vị trí binlog. "lwd" và "high" chỉ để người
// đọc và để kiểm nhất quán khi parse; trạng thái thật là base + holes + applied.
type posAppliedJSON struct {
	Mode     string        `json:"mode"`
	V        int           `json:"v"`
	FileBase string        `json:"file_base"`
	Source   string        `json:"source,omitempty"`
	Base     string        `json:"base"`
	Lwd      string        `json:"lwd"`
	High     string        `json:"high"`
	Holes    []posHoleJSON `json:"holes"`
	Applied  []string      `json:"applied,omitempty"`
}

func (s *MySQLPosAppliedSet) String() string {
	if s == nil || s.isEmpty() {
		return ""
	}
	pj := posAppliedJSON{
		Mode: BinlogPosMode, V: BinlogPosFormatVersion, FileBase: s.fileBase, Source: s.source,
		Base: fmtKeyOpt(s.base), Lwd: fmtKeyOpt(s.LwdKey()), High: fmtKeyOpt(s.HighKey()),
		Holes: []posHoleJSON{},
	}
	ends := make([]uint64, 0, len(s.holes))
	for e := range s.holes {
		ends = append(ends, e)
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i] < ends[j] })
	for _, e := range ends {
		pj.Holes = append(pj.Holes, posHoleJSON{End: FormatBinlogKey(e), Start: FormatBinlogKey(s.holes[e])})
	}
	extras := make([]uint64, 0, len(s.extra))
	for k := range s.extra {
		extras = append(extras, k)
	}
	sort.Slice(extras, func(i, j int) bool { return extras[i] < extras[j] })
	for _, k := range extras {
		pj.Applied = append(pj.Applied, FormatBinlogKey(k))
	}
	b, _ := json.Marshal(pj)
	return string(b)
}

// IsBinlogPosApplied: chuỗi applied là dạng JSON chế độ vị trí binlog (không phải GTID set).
func IsBinlogPosApplied(s string) bool {
	return strings.HasPrefix(strings.TrimSpace(s), "{")
}

// ParseBinlogPosApplied đọc dạng JSON của MySQLPosAppliedSet. Chuỗi hỏng, phiên bản/chế độ lạ, hay
// không nhất quán (lwd/high không khớp, hole ngoài (·, base], extra ≤ base, Txn vắt qua hai file)
// đều trả lỗi: đọc sai im lặng là mất hoặc apply lại dữ liệu.
func ParseBinlogPosApplied(str string) (*MySQLPosAppliedSet, error) {
	str = strings.TrimSpace(str)
	if str == "" {
		return NewMySQLPosAppliedSet("", 0), nil
	}
	var pj posAppliedJSON
	dec := json.NewDecoder(bytes.NewReader([]byte(str)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&pj); err != nil {
		return nil, fmt.Errorf("parse binlog position applied JSON: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("parse binlog position applied JSON: trailing data")
	}
	if pj.Mode != BinlogPosMode {
		return nil, fmt.Errorf("applied set has mode %q, want %q", pj.Mode, BinlogPosMode)
	}
	if pj.V != BinlogPosFormatVersion {
		return nil, fmt.Errorf("applied set format version %d is not supported (this build reads %d)", pj.V, BinlogPosFormatVersion)
	}
	if pj.FileBase == "" {
		return nil, fmt.Errorf("applied set has no file_base")
	}
	base, err := parseKeyOpt(pj.Base)
	if err != nil {
		return nil, fmt.Errorf("applied set base: %w", err)
	}
	set := NewMySQLPosAppliedSet(pj.FileBase, base)
	set.source = pj.Source
	for _, h := range pj.Holes {
		e, err := ParseBinlogKey(h.End)
		if err != nil {
			return nil, fmt.Errorf("applied set hole end: %w", err)
		}
		st, err := ParseBinlogKey(h.Start)
		if err != nil {
			return nil, fmt.Errorf("applied set hole start: %w", err)
		}
		if e > base {
			return nil, fmt.Errorf("applied set hole %s is above base %s", h.End, pj.Base)
		}
		if st >= e || st>>32 != e>>32 {
			return nil, fmt.Errorf("applied set hole [%s, %s] is not a txn inside one binlog file", h.Start, h.End)
		}
		set.holes[e] = st
	}
	for _, as := range pj.Applied {
		k, err := ParseBinlogKey(as)
		if err != nil {
			return nil, fmt.Errorf("applied set extra: %w", err)
		}
		if k <= base {
			return nil, fmt.Errorf("applied set extra %s is not above base %s", as, pj.Base)
		}
		set.extra[k] = struct{}{}
	}
	if got := fmtKeyOpt(set.LwdKey()); pj.Lwd != got {
		return nil, fmt.Errorf("applied set lwd %s inconsistent with holes (expected %s)", pj.Lwd, got)
	}
	if got := fmtKeyOpt(set.HighKey()); pj.High != got {
		return nil, fmt.Errorf("applied set high %s inconsistent with contents (expected %s)", pj.High, got)
	}
	return set, nil
}

// MergeChecked gộp src vào dst và trả tập kết quả, từ chối các tổ hợp mà Merge sẽ bỏ qua IM LẶNG
// (khác loại tập, khác tên gốc file binlog): bỏ qua im lặng một row delta là mất Txn (CP-1).
// Ngoại lệ hợp lệ duy nhất: tập GTID RỖNG (row toàn cục chưa ghi, parse "" ra GTID set rỗng) đứng cạnh
// tập vị trí binlog thì nhường chỗ cho nó.
func MergeChecked(dst, src AppliedSet) (AppliedSet, error) {
	if dst == nil {
		return src, nil
	}
	if src == nil {
		return dst, nil
	}
	dp, dIsPos := dst.(*MySQLPosAppliedSet)
	sp, sIsPos := src.(*MySQLPosAppliedSet)
	switch {
	case dIsPos && sIsPos:
		if dp.fileBase != "" && sp.fileBase != "" && dp.fileBase != sp.fileBase {
			return nil, fmt.Errorf("cannot merge applied sets of different binlog base names %q and %q", dp.fileBase, sp.fileBase)
		}
		dp.Merge(sp)
		return dp, nil
	case dIsPos != sIsPos:
		other := src
		pos := dp
		if sIsPos {
			other, pos = dst, sp
		}
		if g, ok := other.(*MySQLAppliedSet); ok && g.String() == "" {
			if sIsPos {
				return pos.Clone(), nil
			}
			return pos, nil
		}
		return nil, fmt.Errorf("cannot merge a binlog-position applied set with a %T: the pipeline was created in a different position mode", other)
	}
	if fmt.Sprintf("%T", dst) != fmt.Sprintf("%T", src) {
		return nil, fmt.Errorf("cannot merge applied sets of different types %T and %T", dst, src)
	}
	dst.Merge(src)
	return dst, nil
}

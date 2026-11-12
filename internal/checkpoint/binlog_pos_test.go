package checkpoint

import (
	"math/rand"
	"strings"
	"testing"

	"strand/internal/model"
)

func bp(seq uint32, off uint64) uint64 {
	k, err := BinlogKey(seq, off)
	if err != nil {
		panic(err)
	}
	return k
}

func bpos(seq uint32, off uint64) model.Pos {
	return model.Pos{Engine: model.EngineMySQL, File: FormatBinlogName("mysql-bin", seq), BinlogPos: off}
}

func TestBinlogNameAndKey(t *testing.T) {
	base, seq, err := SplitBinlogName("mysql-bin.000123")
	if err != nil || base != "mysql-bin" || seq != 123 {
		t.Fatalf("split: %q %d %v", base, seq, err)
	}
	if base, seq, err = SplitBinlogName("my.sql-bin.1234567"); err != nil || base != "my.sql-bin" || seq != 1234567 {
		t.Fatalf("7-digit seq: %q %d %v", base, seq, err)
	}
	for _, bad := range []string{"mysql-bin", "mysql-bin.", ".000001", "mysql-bin.00x001", "mysql-bin.0000123", "mysql-bin.99999999999", "mysql-bin.12"} {
		if _, _, err := SplitBinlogName(bad); err == nil {
			t.Errorf("SplitBinlogName(%q) accepted", bad)
		}
	}
	// Khóa đơn điệu theo (seq, offset), kể cả khi offset của file trước lớn hơn offset của file sau.
	if !(bp(5, 4_000_000_000) < bp(6, 4)) || !(bp(6, 4) < bp(6, 5)) {
		t.Fatal("keys are not monotonic")
	}
	if _, err := BinlogKey(1, 1<<32); err == nil {
		t.Error("offset 2^32 must be rejected")
	}
	if _, err := BinlogKey(1, 3); err == nil {
		t.Error("offset < 4 must be rejected")
	}
	if _, err := BinlogKey(1, MaxBinlogOffset); err != nil {
		t.Errorf("offset 2^32-1 must be accepted: %v", err)
	}
	k := bp(77, 12345)
	if s, o := SplitBinlogKey(k); s != 77 || o != 12345 {
		t.Fatalf("split key: %d %d", s, o)
	}
	if FormatBinlogKey(k) != "000077:12345" {
		t.Fatalf("format: %s", FormatBinlogKey(k))
	}
	if pk, err := ParseBinlogKey("000077:12345"); err != nil || pk != k {
		t.Fatalf("parse key: %d %v", pk, err)
	}
	for _, bad := range []string{"", "77", "77:", ":4", "x:4", "77:y", "77:3", "4294967296:10", "77:4294967296", "-1:10"} {
		if _, err := ParseBinlogKey(bad); err == nil {
			t.Errorf("ParseBinlogKey(%q) accepted", bad)
		}
	}
	b, key, err := ParseBinlogStartAt("mysql-bin.000123:4567")
	if err != nil || b != "mysql-bin" || key != bp(123, 4567) {
		t.Fatalf("start-at: %q %d %v", b, key, err)
	}
	for _, bad := range []string{"", "mysql-bin.000123", "mysql-bin.000123:", "mysql-bin:4567", "mysql-bin.000123:abc", "mysql-bin.000123:2", "uuid:1-5"} {
		if _, _, err := ParseBinlogStartAt(bad); err == nil {
			t.Errorf("ParseBinlogStartAt(%q) accepted", bad)
		}
	}
}

func TestKeyOfPosBaseMismatch(t *testing.T) {
	if _, err := KeyOfPos("other-bin", bpos(1, 100)); err == nil || !strings.Contains(err.Error(), "basename") {
		t.Fatalf("want base-name error, got %v", err)
	}
	if _, err := KeyOfPos("mysql-bin", model.Pos{Engine: model.EngineMySQL, GTID: "u:1"}); err == nil {
		t.Fatal("GTID pos must not convert to a binlog key")
	}
	if _, err := KeyOfPos("mysql-bin", model.Pos{Engine: model.EnginePG, LSN: 5}); err == nil {
		t.Fatal("PG pos must not convert to a binlog key")
	}
}

func TestPosSetBasics(t *testing.T) {
	s := NewMySQLPosAppliedSet("mysql-bin", bp(1, 500))
	if !s.Contains(bpos(1, 400)) || !s.Contains(bpos(1, 500)) || s.Contains(bpos(1, 501)) || s.Contains(bpos(2, 4)) {
		t.Fatal("base containment wrong")
	}
	s.Add(bpos(1, 900))
	s.Add(bpos(2, 300))
	if !s.Contains(bpos(1, 900)) || !s.Contains(bpos(2, 300)) || s.Contains(bpos(1, 700)) {
		t.Fatal("extra containment wrong")
	}
	if s.High() != bpos(2, 300) || s.Lwd() != bpos(1, 500) {
		t.Fatalf("high/lwd: %v %v", s.High(), s.Lwd())
	}
	// Không phải vị trí binlog / khác tên gốc: không bao giờ "đã apply".
	if s.Contains(model.Pos{Engine: model.EngineMySQL, GTID: "u:1"}) || s.Contains(model.Pos{Engine: model.EnginePG, LSN: 3}) ||
		s.Contains(model.Pos{Engine: model.EngineMySQL, File: "other.000001", BinlogPos: 10}) {
		t.Fatal("foreign position reported as applied")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Add with a foreign base name must panic (silent drop would double-apply after restart)")
		}
	}()
	s.Add(model.Pos{Engine: model.EngineMySQL, File: "other.000001", BinlogPos: 10})
}

func TestPosSetVouchHolesAndResume(t *testing.T) {
	s := NewMySQLPosAppliedSet("mysql-bin", bp(1, 100))
	s.Add(bpos(1, 400)) // extra
	// Txn kết thúc 300 (bắt đầu 200) chưa apply; 400 đã apply; 500 (bắt đầu 450) chưa apply.
	err := s.Vouch(bp(1, 450), []BinlogSpan{{Start: bp(1, 200), End: bp(1, 300)}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Contains(bpos(1, 300)) || !s.Contains(bpos(1, 400)) {
		t.Fatal("vouch result wrong")
	}
	if s.NumExtra() != 0 || s.NumHoles() != 1 || s.Base() != bp(1, 450) {
		t.Fatalf("after vouch: extra=%d holes=%d base=%d", s.NumExtra(), s.NumHoles(), s.Base())
	}
	// Lwd = bắt đầu của hole: đọc lại từ đó (CP-3), không phải end-1.
	if s.Lwd() != bpos(1, 200) {
		t.Fatalf("Lwd = %v, want start of the hole", s.Lwd())
	}
	if h := s.Holes(); len(h) != 1 || h[0] != bpos(1, 300) {
		t.Fatalf("holes: %v", h)
	}
	// Apply hole: hole biến mất, Lwd tiến tới base.
	s.Add(bpos(1, 300))
	if !s.Contains(bpos(1, 300)) || s.NumHoles() != 0 || s.Lwd() != bpos(1, 450) {
		t.Fatalf("hole not closed: %s", s)
	}
	// Vouch không lùi.
	if err := s.Vouch(bp(1, 10), nil); err != nil || s.Base() != bp(1, 450) {
		t.Fatal("vouch went backwards")
	}
	// Vouch cắt giữa Txn pending: lỗi và không đổi gì.
	before := s.Clone()
	if err := s.Vouch(bp(1, 700), []BinlogSpan{{Start: bp(1, 600), End: bp(1, 800)}}); err == nil {
		t.Fatal("vouch inside a pending txn must fail")
	}
	if !s.Equal(before) {
		t.Fatal("failed vouch mutated the set")
	}
	if err := s.Vouch(bp(1, 700), []BinlogSpan{{Start: bp(1, 800), End: bp(1, 800)}}); err == nil {
		t.Fatal("degenerate pending span accepted")
	}
	// Vouch ngay tại start của pending Txn (ranh giới) hợp lệ, Txn ở trên mốc không thành hole.
	if err := s.Vouch(bp(1, 600), []BinlogSpan{{Start: bp(1, 600), End: bp(1, 800)}}); err != nil || s.NumHoles() != 0 {
		t.Fatalf("boundary vouch: %v holes=%d", err, s.NumHoles())
	}
}

func TestPosSetMergeSemantics(t *testing.T) {
	a := NewMySQLPosAppliedSet("mysql-bin", bp(1, 1000))
	a.holes[bp(1, 500)] = bp(1, 400)
	a.Add(bpos(1, 1500))
	a.Add(bpos(2, 100))
	o := NewMySQLPosAppliedSet("mysql-bin", 0)
	o.Add(bpos(1, 500))  // áp lại hole của a
	o.Add(bpos(1, 1500)) // trùng
	o.Add(bpos(2, 900))
	m := a.Clone().(*MySQLPosAppliedSet)
	m.Merge(o)
	for _, p := range []model.Pos{bpos(1, 500), bpos(1, 1000), bpos(1, 1500), bpos(2, 100), bpos(2, 900), bpos(1, 10)} {
		if !m.Contains(p) {
			t.Errorf("merge lost %v: %s", p, m)
		}
	}
	if m.NumHoles() != 0 {
		t.Errorf("hole should be closed by the other side: %s", m)
	}
	// Hole của tập base lớn tồn tại nếu bên kia (base nhỏ) không có.
	o2 := NewMySQLPosAppliedSet("mysql-bin", bp(1, 200))
	m2 := a.Clone().(*MySQLPosAppliedSet)
	m2.Merge(o2)
	if m2.Contains(bpos(1, 500)) || m2.Lwd() != bpos(1, 400) {
		t.Errorf("hole must survive: %s", m2)
	}
	// Merge giao hoán và lũy đẳng theo Contains trên nhiều tập ngẫu nhiên.
	rnd := rand.New(rand.NewSource(7))
	randSet := func() *MySQLPosAppliedSet {
		s := NewMySQLPosAppliedSet("mysql-bin", uint64(rnd.Intn(40)+1)<<4|4)
		for i := 0; i < rnd.Intn(6); i++ {
			k := uint64(rnd.Intn(40)+1)<<4 | 4
			if k <= s.base {
				if k > 4 {
					s.holes[k] = k - 4
				}
			}
		}
		for i := 0; i < rnd.Intn(8); i++ {
			s.AddKey(uint64(rnd.Intn(60)+1)<<4 | 4)
		}
		return s
	}
	for i := 0; i < 500; i++ {
		x, y := randSet(), randSet()
		xy, yx := x.Clone().(*MySQLPosAppliedSet), y.Clone().(*MySQLPosAppliedSet)
		xy.Merge(y)
		yx.Merge(x)
		for k := uint64(0); k < 70; k++ {
			key := k<<4 | 4
			want := x.ContainsKey(key) || y.ContainsKey(key)
			if xy.ContainsKey(key) != want || yx.ContainsKey(key) != want {
				t.Fatalf("iter %d key %d: merge != union\nx=%s\ny=%s\nxy=%s", i, k, x, y, xy)
			}
		}
		again := xy.Clone().(*MySQLPosAppliedSet)
		again.Merge(y)
		if !again.Equal(xy) {
			t.Fatalf("merge not idempotent")
		}
	}
	// khác tên gốc: Merge bỏ qua, MergeChecked báo lỗi.
	other := NewMySQLPosAppliedSet("other-bin", bp(1, 100))
	if _, err := MergeChecked(a.Clone(), other); err == nil {
		t.Fatal("MergeChecked must reject different base names")
	}
}

func TestPosSetSubtract(t *testing.T) {
	g := NewMySQLPosAppliedSet("mysql-bin", bp(1, 1000))
	w := NewMySQLPosAppliedSet("mysql-bin", 0)
	w.Add(bpos(1, 900))  // đã trong global
	w.Add(bpos(1, 1200)) // chưa
	SubtractApplied(w, g)
	if w.Contains(bpos(1, 900)) && w.NumExtra() != 1 {
		t.Fatal("subtract failed")
	}
	if w.NumExtra() != 1 || !w.Contains(bpos(1, 1200)) {
		t.Fatalf("subtract result: %s", w)
	}
}

func TestPosSetRoundTripAndParseErrors(t *testing.T) {
	s := NewMySQLPosAppliedSet("mysql-bin", bp(3, 9000))
	s.SetSource("server_id=1;uuid=abc")
	s.holes[bp(3, 8000)] = bp(3, 7000)
	s.Add(bpos(3, 9500))
	s.Add(bpos(4, 120))
	str := s.String()
	for _, want := range []string{`"mode":"binlog_pos"`, `"v":1`, `"file_base":"mysql-bin"`, `"lwd":"000003:7000"`, `"high":"000004:120"`} {
		if !strings.Contains(str, want) {
			t.Errorf("JSON lacks %s: %s", want, str)
		}
	}
	back, err := ParseAppliedSet(model.EngineMySQL, str)
	if err != nil {
		t.Fatal(err)
	}
	bs := back.(*MySQLPosAppliedSet)
	if !bs.Equal(s) || bs.Source() != s.Source() || bs.String() != str {
		t.Fatalf("round trip differs:\n%s\n%s", str, bs)
	}
	// Tập rỗng: chuỗi rỗng; parse "" của MySQL vẫn là GTID set rỗng.
	if NewMySQLPosAppliedSet("mysql-bin", 0).String() != "" {
		t.Fatal("empty pos set must serialize to empty string")
	}
	bad := map[string]string{
		"not json":      `{"mode":`,
		"wrong mode":    strings.Replace(str, `"binlog_pos"`, `"gtid"`, 1),
		"future v":      strings.Replace(str, `"v":1`, `"v":2`, 1),
		"unknown field": strings.Replace(str, `"mode"`, `"extra":1,"mode"`, 1),
		"lwd mismatch":  strings.Replace(str, `"lwd":"000003:7000"`, `"lwd":"000003:9000"`, 1),
		"high mismatch": strings.Replace(str, `"high":"000004:120"`, `"high":"000004:121"`, 1),
		"hole > base":   strings.Replace(str, `"end":"000003:8000"`, `"end":"000003:9900"`, 1),
		"extra <= base": strings.Replace(str, `"000003:9500"`, `"000003:9000"`, 1),
		"cross-file":    strings.Replace(str, `"start":"000003:7000"`, `"start":"000002:7000"`, 1),
		"no file_base":  strings.Replace(str, `"file_base":"mysql-bin"`, `"file_base":""`, 1),
		"bad offset":    strings.Replace(str, `"base":"000003:9000"`, `"base":"000003:2"`, 1),
		"trailing":      str + `{}`,
	}
	for name, in := range bad {
		if _, err := ParseAppliedSet(model.EngineMySQL, in); err == nil {
			t.Errorf("%s: accepted %s", name, in)
		}
	}
	// GTID set vẫn parse như cũ, không bị nhận nhầm.
	g, err := ParseAppliedSet(model.EngineMySQL, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := g.(*MySQLAppliedSet); !ok {
		t.Fatalf("GTID set parsed as %T", g)
	}
}

func TestMergeCheckedModes(t *testing.T) {
	pos := NewMySQLPosAppliedSet("mysql-bin", bp(1, 100))
	emptyGTID := NewMySQLAppliedSet()
	// row toàn cục chưa ghi ("" → GTID set rỗng) + delta vị trí: delta thắng.
	r, err := MergeChecked(emptyGTID, pos)
	if err != nil || !r.(*MySQLPosAppliedSet).Equal(pos) {
		t.Fatalf("empty GTID + pos: %v %v", r, err)
	}
	r, err = MergeChecked(pos.Clone(), emptyGTID)
	if err != nil || !r.(*MySQLPosAppliedSet).Equal(pos) {
		t.Fatalf("pos + empty GTID: %v %v", r, err)
	}
	// GTID set KHÔNG rỗng + vị trí: lỗi, không im lặng bỏ delta.
	g := NewMySQLAppliedSet()
	if err := g.AddGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"); err != nil {
		t.Fatal(err)
	}
	if _, err := MergeChecked(g, pos); err == nil {
		t.Fatal("non-empty GTID set merged with a position set")
	}
	if _, err := MergeChecked(pos.Clone(), g); err == nil {
		t.Fatal("position set merged with a non-empty GTID set")
	}
	// PG + vị trí: lỗi.
	if _, err := MergeChecked(NewPGAppliedSet(5), pos); err == nil {
		t.Fatal("PG set merged with a position set")
	}
	// Cùng loại: bình thường.
	if r, err := MergeChecked(NewPGAppliedSet(5), NewPGAppliedSet(9)); err != nil || r.(*PGAppliedSet).High().LSN != 9 {
		t.Fatalf("PG merge: %v %v", r, err)
	}
}

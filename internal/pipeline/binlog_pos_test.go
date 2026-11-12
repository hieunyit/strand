package pipeline

import (
	"strings"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

func pk(seq uint32, off uint64) uint64 {
	k, err := checkpoint.BinlogKey(seq, off)
	if err != nil {
		panic(err)
	}
	return k
}

// pointPos: mốc là ranh giới event — bắt đầu Txn chưa xác nhận sớm nhất, hoặc kết thúc Txn cuối đã nhận.
func TestLSNTrackerPointPos(t *testing.T) {
	var tr lsnTracker
	if p := tr.pointPos(0); p != 0 {
		t.Fatalf("empty tracker point = %d", p)
	}
	tr.routedPos(pk(1, 300), pk(1, 100), 1)
	tr.routedPos(pk(1, 400), pk(1, 350), 0) // đã apply: chỉ nâng seen
	tr.routedPos(pk(1, 700), pk(1, 500), 2)
	tr.routedPos(pk(2, 300), pk(2, 4), 3)
	if p := tr.pointPos(0); p != pk(1, 100) {
		t.Fatalf("nothing done: %s", checkpoint.FormatBinlogKey(p))
	}
	if p := tr.pointPos(1); p != pk(1, 500) {
		t.Fatalf("seq 1 done: %s", checkpoint.FormatBinlogKey(p))
	}
	if p := tr.pointPos(2); p != pk(2, 4) {
		t.Fatalf("seq 2 done (txn 3 starts at the start of the next file): %s", checkpoint.FormatBinlogKey(p))
	}
	if p := tr.pointPos(3); p != pk(2, 300) {
		t.Fatalf("all done: %s", checkpoint.FormatBinlogKey(p))
	}
	// Seq được Done trước khi router kịp ghi nhận Txn: mốc không vượt Txn đã nhận cuối.
	if p := tr.pointPos(9); p != pk(2, 300) {
		t.Fatalf("unknown seq done: %s", checkpoint.FormatBinlogKey(p))
	}
	// Không bao giờ vouch cắt giữa một Txn: vouch qua pointPos với pending = nil luôn hợp lệ.
	s := checkpoint.NewMySQLPosAppliedSet("mysql-bin", pk(1, 4))
	tr2 := lsnTracker{}
	tr2.routedPos(pk(1, 300), pk(1, 100), 1)
	tr2.routedPos(pk(1, 700), pk(1, 500), 2)
	s.Add(checkpoint.PosOfKey("mysql-bin", pk(1, 700))) // Txn 2 xong trước Txn 1 (relaxed)
	if err := s.Vouch(tr2.pointPos(0), nil); err != nil {
		t.Fatal(err)
	}
	if s.Contains(checkpoint.PosOfKey("mysql-bin", pk(1, 300))) || !s.Contains(checkpoint.PosOfKey("mysql-bin", pk(1, 700))) {
		t.Fatalf("vouch made the unfinished txn look applied: %s", s)
	}
	if s.Lwd() != checkpoint.PosOfKey("mysql-bin", pk(1, 100)) {
		t.Fatalf("resume point = %v, want the start of the unfinished txn", s.Lwd())
	}
}

func posTxn(file string, start, end uint64) *model.Txn {
	return &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, File: file, BinlogPos: end}, BinlogFile: file, BinlogPos: start}
}

// Router kiểm vị trí trước khi dùng: tên gốc, bắt đầu/kết thúc hợp lệ, luồng đơn điệu và không chồng lấn.
func TestCheckBinlogTxn(t *testing.T) {
	p := &Pipeline{binlogBase: "mysql-bin", lastRouteEnd: pk(3, 100)}
	if s, e, err := p.checkBinlogTxn(posTxn("mysql-bin.000003", 100, 300)); err != nil || s != pk(3, 100) || e != pk(3, 300) {
		t.Fatalf("first txn: %v", err)
	}
	if p.curStart != pk(3, 100) || p.curEnd != pk(3, 300) {
		t.Fatal("cur positions not recorded")
	}
	// Event không thuộc Txn giữa hai Txn (start > end trước): hợp lệ.
	if _, _, err := p.checkBinlogTxn(posTxn("mysql-bin.000003", 350, 500)); err != nil {
		t.Fatal(err)
	}
	// Sang file mới.
	if _, _, err := p.checkBinlogTxn(posTxn("mysql-bin.000004", 4, 90)); err != nil {
		t.Fatal(err)
	}
	for name, tx := range map[string]*model.Txn{
		"overlap":        posTxn("mysql-bin.000004", 50, 200),
		"backwards file": posTxn("mysql-bin.000003", 600, 700),
		"other base":     posTxn("other-bin.000004", 100, 200),
		"start>=end":     posTxn("mysql-bin.000004", 300, 300),
		"no start file":  {SourcePos: model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000004", BinlogPos: 400}},
		"gtid txn":       {SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:1"}},
	} {
		if _, _, err := p.checkBinlogTxn(tx); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, _, err := p.checkBinlogTxn(posTxn("mysql-bin.000003", 600, 700)); err == nil || !strings.Contains(err.Error(), "monotonic") && !strings.Contains(err.Error(), "before the end") {
		t.Errorf("non-monotonic stream not described: %v", err)
	}
}

// Txn MySQL rỗng (không GTID) không được coi là Txn PG rỗng và bỏ qua: nó phải vào tập đã apply.
func TestSkipEmptyPGTxnKeepsBinlogPosTxns(t *testing.T) {
	for _, strict := range []bool{false, true} {
		if skipEmptyPGTxn(strict, posTxn("mysql-bin.000001", 4, 100)) {
			t.Errorf("strict=%v: empty binlog-position txn would be dropped (position never recorded)", strict)
		}
	}
}

// strictGlobal ở chế độ vị trí: mốc là ranh giới, không vượt Txn chưa vào batch.
func TestStrictGlobalBinlogPos(t *testing.T) {
	set := checkpoint.NewMySQLPosAppliedSet("mysql-bin", pk(1, 4))
	p := &Pipeline{binlogBase: "mysql-bin", strictApplied: set, lsns: &lsnTracker{}}
	// router đã nhận Txn 1,2,3; Txn 3 chưa vào batch
	p.lsns.routedPos(pk(1, 300), pk(1, 100), 1)
	p.lsns.routedPos(pk(1, 500), pk(1, 350), 2)
	p.lsns.routedPos(pk(1, 800), pk(1, 600), 3)
	b := []*model.Txn{posTxn("mysql-bin.000001", 100, 300), posTxn("mysql-bin.000001", 350, 500)}
	b[0].SetSeq(1)
	b[1].SetSeq(2)
	str := p.strictGlobal(b)
	got, err := checkpoint.ParseAppliedSet(model.EngineMySQL, str)
	if err != nil {
		t.Fatal(err)
	}
	ps := got.(*checkpoint.MySQLPosAppliedSet)
	// mốc = bắt đầu của Txn 3 (ranh giới), Txn 3 chưa apply; resume tại bắt đầu của nó.
	if ps.Base() != pk(1, 600) || ps.Contains(checkpoint.PosOfKey("mysql-bin", pk(1, 800))) || !ps.Contains(checkpoint.PosOfKey("mysql-bin", pk(1, 500))) {
		t.Fatalf("strict global: %s", str)
	}
	if ps.Lwd() != checkpoint.PosOfKey("mysql-bin", pk(1, 600)) {
		t.Fatalf("resume = %v", ps.Lwd())
	}
	// Router chưa kịp ghi nhận Txn cuối vào tracker: mốc ≥ kết thúc Txn cuối batch.
	p2 := &Pipeline{binlogBase: "mysql-bin", strictApplied: checkpoint.NewMySQLPosAppliedSet("mysql-bin", pk(1, 4)), lsns: &lsnTracker{}}
	b2 := []*model.Txn{posTxn("mysql-bin.000001", 100, 300)}
	b2[0].SetSeq(1)
	got2, _ := checkpoint.ParseAppliedSet(model.EngineMySQL, p2.strictGlobal(b2))
	if got2.(*checkpoint.MySQLPosAppliedSet).Base() != pk(1, 300) {
		t.Fatalf("strict global without tracker data: %s", got2)
	}
}

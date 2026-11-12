package spool

import (
	"context"
	"testing"

	"strand/internal/model"
)

// Pos chế độ vị trí binlog phải đi qua encode/decode của spool (kể cả vị trí bắt đầu Txn), và Txn
// GTID/PG vẫn dùng định dạng V2 cũ.
func TestTxnCodecBinlogPosRoundTrip(t *testing.T) {
	in := &model.Txn{
		SourcePos:  model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000123", BinlogPos: 4567},
		BinlogFile: "mysql-bin.000123", BinlogPos: 4300,
		SourceTS: 99,
		Rows:     []model.RowChange{{Table: model.TableID{Schema: "a", Table: "t"}, Op: model.OpInsert, Key: []model.Value{int64(1)}}},
	}
	b, err := encodeTxn(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(b[:4]) != txnMagicV3 {
		t.Fatalf("magic = %q", b[:4])
	}
	out, err := decodeTxn(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.SourcePos != in.SourcePos || out.BinlogFile != in.BinlogFile || out.BinlogPos != in.BinlogPos || out.SourceTS != 99 || len(out.Rows) != 1 {
		t.Fatalf("round trip: %+v", out)
	}
	for _, old := range []*model.Txn{
		{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:1"}},
		{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 77}},
	} {
		b, err := encodeTxn(old)
		if err != nil {
			t.Fatal(err)
		}
		if string(b[:4]) != txnMagicV2 {
			t.Fatalf("legacy txn encoded as %q", b[:4])
		}
		o, err := decodeTxn(b)
		if err != nil || o.SourcePos != old.SourcePos {
			t.Fatalf("legacy round trip: %+v %v", o, err)
		}
	}
}

func TestPosKeyBinlogPos(t *testing.T) {
	a := posKey(model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000001", BinlogPos: 10})
	b := posKey(model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000001", BinlogPos: 11})
	g := posKey(model.Pos{Engine: model.EngineMySQL, GTID: "u:1"})
	if string(a) == string(b) || string(a) == string(g) {
		t.Fatalf("position keys collide: %s %s %s", a, b, g)
	}
	if !hasPos(model.Pos{Engine: model.EngineMySQL, File: "f.000001", BinlogPos: 4}) || hasPos(model.Pos{}) {
		t.Fatal("hasPos")
	}
}

// Spool lưu và đọc lại Txn theo vị trí binlog (Iter from, Truncate theo tập đã apply).
func TestSpoolBinlogPosAppendIter(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	mk := func(off uint64) *model.Txn {
		return &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000001", BinlogPos: off}}
	}
	for _, off := range []uint64{100, 200, 300} {
		if err := s.Append(ctx, mk(off)); err != nil {
			t.Fatal(err)
		}
	}
	it, err := s.Iter(ctx, mk(200).SourcePos)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got, err := it.Next(ctx)
	if err != nil || got.SourcePos.BinlogPos != 200 {
		t.Fatalf("iter from 200: %+v %v", got, err)
	}
}

package pipeline

import (
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

func TestStopGateBinlogPosition(t *testing.T) {
	p := &Pipeline{srcEngine: model.EngineMySQL, posMode: true, opts: Options{StopAt: "mysql-bin.000002:500"}}
	key := func(seq uint32, off uint64) uint64 {
		k, err := checkpoint.BinlogKey(seq, off)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	txn := func(file string, end uint64) *model.Txn {
		return &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, File: file, BinlogPos: end}}
	}
	g, reached, err := p.newStopGate(checkpoint.NewMySQLPosAppliedSet("mysql-bin", key(2, 100)))
	if err != nil || g == nil || reached {
		t.Fatalf("gate=%v reached=%v err=%v", g, reached, err)
	}
	for _, c := range []struct {
		t               *model.Txn
		beyond, reached bool
	}{
		{txn("mysql-bin.000002", 400), false, false},
		{txn("mysql-bin.000002", 500), false, true},
		{txn("mysql-bin.000002", 501), true, false},
		{txn("mysql-bin.000003", 4), true, false},
		{txn("other-bin.000009", 9999), false, false}, // tên gốc khác: không so được, không chặn
	} {
		if got := g.beyond(c.t); got != c.beyond {
			t.Errorf("beyond(%s:%d) = %v", c.t.SourcePos.File, c.t.SourcePos.BinlogPos, got)
		}
		if got := g.reachedAfter(c.t); got != c.reached {
			t.Errorf("reachedAfter(%s:%d) = %v", c.t.SourcePos.File, c.t.SourcePos.BinlogPos, got)
		}
	}
	// Checkpoint đã qua điểm dừng: dừng ngay.
	if _, reached, _ := p.newStopGate(checkpoint.NewMySQLPosAppliedSet("mysql-bin", key(2, 600))); !reached {
		t.Fatal("a checkpoint past the stop position must stop at once")
	}
}

func TestStopGateRejectsBadInput(t *testing.T) {
	for _, c := range []struct {
		p    *Pipeline
		want int
	}{
		{&Pipeline{srcEngine: model.EnginePG, opts: Options{StopAt: "mysql-bin.000001:4"}}, exitcode.InvalidSyntax},
		{&Pipeline{srcEngine: model.EngineMySQL, posMode: true, opts: Options{StopAt: "0/1A"}}, exitcode.InvalidSyntax},
		{&Pipeline{srcEngine: model.EnginePG, opts: Options{StopAt: "0/1A", StopAtTime: time.Now()}}, exitcode.InvalidSyntax},
	} {
		_, _, err := c.p.newStopGate(checkpoint.NewPGAppliedSet(1))
		if exitcode.GetExitCode(err) != c.want {
			t.Errorf("%+v: err %v", c.p.opts, err)
		}
	}
	// Không có điểm dừng.
	if g, _, err := (&Pipeline{srcEngine: model.EnginePG}).newStopGate(checkpoint.NewPGAppliedSet(1)); g != nil || err != nil {
		t.Fatalf("no stop position: gate %v err %v", g, err)
	}
}

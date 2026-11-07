package pipeline

// Post-seed verify cho mọi cặp engine (việc 7): điều kiện bắt đầu, cách verify bảng PK tổng quát, và
// bảng khóa chính xác (khác thứ tự hai phía) không được báo sạch ở verify delta.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestPostSeedReadyWaitsForCatchUpApplyAndSnapshots(t *testing.T) {
	for _, tc := range []struct {
		caughtUp      bool
		lwd, admitted uint64
		bootstrapping bool
		want          bool
	}{
		{true, 10, 10, false, true},
		{false, 10, 10, false, false}, // reader chưa theo kịp source
		{true, 9, 10, false, false},   // Txn đã nhận chưa apply
		{true, 10, 10, true, false},   // seed PG một phần: bảng ngoài seed còn đang snapshot logic
	} {
		if got := postSeedReady(tc.caughtUp, tc.lwd, tc.admitted, tc.bootstrapping); got != tc.want {
			t.Errorf("postSeedReady(%+v) = %v, want %v", tc, got, tc.want)
		}
	}
}

func TestGenVerifyModeByEnginePair(t *testing.T) {
	if m := genVerifyMode(true, false); m != "live" {
		t.Fatalf("MySQL→MySQL: %s", m)
	}
	if m := genVerifyMode(false, false); m != "canonical" {
		t.Fatalf("PG→PG / MySQL→PG same order: %s", m)
	}
	if m := genVerifyMode(false, true); m != "skip" {
		t.Fatalf("MySQL→PG exact-key table: %s", m)
	}
}

// targetKeyCodec dùng codec đích đã kiểm lúc snapshot (kể cả cờ ExactKeys) — không hỏi DB.
func TestTargetKeyCodecReportsExactKeys(t *testing.T) {
	dc := &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "k", Kind: snapshot.PKBytes, SQLType: "text"}}}
	p := &Pipeline{srcEngine: model.EngineMySQL, dstEngine: model.EnginePG}
	m := &tableMeta{TargetPKCodec: dc, ExactKeys: true}
	got, exact, err := p.targetKeyCodec(context.Background(), model.TableID{Schema: "a", Table: "b"}, m)
	if err != nil || got != dc || !exact {
		t.Fatalf("got %v exact=%v err=%v", got, exact, err)
	}
}

func TestDeltaVerifyResultIncompleteForSkippedTables(t *testing.T) {
	a := model.TableID{Schema: "app", Table: "a"}
	x := model.TableID{Schema: "app", Table: "x"}
	tt := touchedTake{keys: map[model.TableID][]int64{a: {1}}, over: map[model.TableID]bool{},
		gen: map[model.TableID][]snapshot.Bound{x: {snapshot.IntBound(1)}}}
	_, err := deltaVerifyResult(tt, x, x)
	var xe *exitcode.Error
	if err == nil || !errors.As(err, &xe) || xe.Code != exitcode.ErrVerifyIncomplete {
		t.Fatalf("skipped table reported clean: %v", err)
	}
	if !strings.Contains(err.Error(), "1 tables cannot be verified by key") || !strings.Contains(err.Error(), "app.x") {
		t.Fatalf("message: %v", err)
	}
	if msg, err := deltaVerifyResult(tt); err != nil || !strings.Contains(msg, "0 mismatches") {
		t.Fatalf("no skipped: %q %v", msg, err)
	}
}

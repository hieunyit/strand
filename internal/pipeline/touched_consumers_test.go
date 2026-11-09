package pipeline

// Regression test review N12: post-seed verify và verify delta mỗi bên có tập khóa đã chạm riêng;
// verify lỗi gộp lại khóa đã lấy; bảng vượt giới hạn ghi khóa làm kết quả delta incomplete (exit 5).
// Unit test thuần, không cần DB.

import (
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestN12TouchedKeysPerConsumer(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "t"}
	tk := newTouchedKeys(100)
	tk.enable(touchPostSeed, true)
	tk.enable(touchDelta, true)
	tk.add(id, []model.Value{int64(1)})
	tk.add(id, []model.Value{int64(2)})

	// verify delta chạy giữa cửa sổ post-seed: không được lấy mất khóa của post-seed.
	if d := tk.take(touchDelta); len(d.keys[id]) != 2 {
		t.Fatalf("delta took %v, want 2 keys", d.keys)
	}
	tk.add(id, []model.Value{int64(3)})
	ps := tk.take(touchPostSeed)
	if got := ps.keys[id]; len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("post-seed took %v, want [1 2 3] (keys stolen by delta?)", got)
	}
	if d := tk.take(touchDelta); len(d.keys[id]) != 1 || d.keys[id][0] != 3 {
		t.Fatalf("delta second take %v, want [3]", d.keys)
	}

	// Tắt post-seed không ảnh hưởng delta.
	tk.enable(touchPostSeed, false)
	tk.add(id, []model.Value{int64(4)})
	if ps := tk.take(touchPostSeed); len(ps.keys) != 0 {
		t.Fatalf("disabled consumer recorded %v", ps.keys)
	}
	if d := tk.take(touchDelta); len(d.keys[id]) != 1 {
		t.Fatalf("delta after post-seed off: %v", d.keys)
	}
}

func TestN12TouchedKeysRestoreAfterFailedVerify(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "t"}
	g := model.TableID{Schema: "app", Table: "g"}
	tk := newTouchedKeys(3)
	tk.enable(touchDelta, true)
	for i := 1; i <= 4; i++ {
		tk.add(id, []model.Value{int64(i)})
	}
	tt := tk.take(touchDelta)
	tt.gen = map[model.TableID][]snapshot.Bound{g: {{K: "k1"}}}
	if !tt.over[id] || len(tt.keys[id]) != 3 {
		t.Fatalf("take: keys=%v over=%v, want 3 keys + overflow", tt.keys, tt.over)
	}
	tk.add(id, []model.Value{int64(9)})
	tk.restore(touchDelta, tt) // verify lỗi: gộp lại
	again := tk.take(touchDelta)
	if len(again.keys[id]) != 3 || !again.over[id] {
		t.Fatalf("after restore: keys=%v over=%v, want limit-capped 3 keys + overflow kept", again.keys, again.over)
	}
	if len(again.gen[g]) != 1 {
		t.Fatalf("after restore: gen=%v, want k1", again.gen)
	}
}

func TestN12DeltaVerifyResultIncompleteOnOverflow(t *testing.T) {
	a := model.TableID{Schema: "app", Table: "a"}
	b := model.TableID{Schema: "app", Table: "b"}
	msg, err := deltaVerifyResult(touchedTake{keys: map[model.TableID][]int64{a: {1, 2}}, over: map[model.TableID]bool{}})
	if err != nil || !strings.Contains(msg, "2 changed keys in 1 tables, 0 mismatches") {
		t.Fatalf("clean: %q %v", msg, err)
	}
	msg, err = deltaVerifyResult(touchedTake{keys: map[model.TableID][]int64{a: {1}, b: {5}}, over: map[model.TableID]bool{b: true}})
	var xe *exitcode.Error
	if err == nil || !errors.As(err, &xe) || xe.Code != exitcode.ErrVerifyIncomplete || xe.ExitCode != exitcode.DataError {
		t.Fatalf("overflow reported clean: msg=%q err=%v", msg, err)
	}
	if !strings.Contains(err.Error(), "incomplete: 1 tables have incomplete changed-key tracking (limit or encoding failure): app.b") {
		t.Fatalf("message: %v", err)
	}
}

package pipeline

// Regression test review N05: addKey không được gọi codec (WEIGHT_STRING trên source) khi delta
// tracking tắt. Unit test thuần, codec có Weigh giả.

import (
	"sync/atomic"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestN05AddKeySkipsCodecWhenTrackingOff(t *testing.T) {
	var calls atomic.Int64
	codec := &snapshot.PKCodec{
		Cols: []snapshot.PKColumn{{Name: "k", Kind: snapshot.PKWeight, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		Weigh: func(_ snapshot.PKColumn, v model.Value) ([]byte, error) {
			calls.Add(1)
			return []byte("w"), nil
		},
	}
	tbl := model.TableID{Schema: "app", Table: "t"}
	tk := newTouchedKeys(100)

	tk.addKey(tbl, []model.Value{"a"}, codec)
	if n := calls.Load(); n != 0 {
		t.Fatalf("tracking off: codec weigher called %d times, want 0", n)
	}
	if g := tk.take(touchDelta).gen; len(g[tbl]) != 0 {
		t.Fatalf("tracking off: recorded %v", g)
	}

	// Đối chứng: bật tracking thì khóa được mã hóa và ghi lại.
	tk.enable(touchDelta, true)
	tk.addKey(tbl, []model.Value{"a"}, codec)
	if calls.Load() == 0 {
		t.Fatalf("tracking on: codec weigher not called")
	}
	if g := tk.take(touchDelta).gen; len(g[tbl]) != 1 {
		t.Fatalf("tracking on: recorded %v, want 1 key", g)
	}
}

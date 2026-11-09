package pipeline

import (
	"errors"
	"strand/internal/model"
	"strand/internal/snapshot"
	"testing"
)

// Verify không được báo sạch khi nguồn tính weight thất bại; mỗi consumer giữ cờ incomplete riêng.
func TestReviewTouchedEncodeFailureIsIncomplete(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "t"}
	tk := newTouchedKeys(10)
	tk.enable(touchDelta, true)
	tk.enable(touchPostSeed, true)
	c := &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "id", Kind: snapshot.PKWeight}}, Weigh: func(snapshot.PKColumn, model.Value) ([]byte, error) { return nil, errors.New("source unavailable") }}
	tk.addKey(id, []model.Value{"a"}, c)
	for _, consumer := range []string{touchDelta, touchPostSeed} {
		tt := tk.take(consumer)
		if !tt.over[id] {
			t.Fatalf("%s falsely complete after encoding failure", consumer)
		}
		if _, err := deltaVerifyResult(tt); err == nil {
			t.Fatal("verify reported clean")
		}
	}
}

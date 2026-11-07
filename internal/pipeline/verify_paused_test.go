package pipeline

import (
	"context"
	"errors"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// review item 9: `verify --mode delta` khi pipeline đang pause phải báo pipeline_paused ngay (router dừng
// nên chờ apply chỉ hết hạn), và không lấy mất tập khóa đã đổi của lần verify sau.
func TestVerifyDeltaFailsFastWhenPaused(t *testing.T) {
	id := model.TableID{Schema: "db", Table: "t"}
	p := &Pipeline{id: "p1", touched: newTouchedKeys(100)}
	p.touched.enable(touchDelta, true)
	p.touched.add(id, []model.Value{int64(1)})
	p.paused.Store(true)

	_, err := p.VerifyDelta(context.Background())
	var xe *exitcode.Error
	if !errors.As(err, &xe) || xe.Code != exitcode.ErrPipelinePaused || xe.ExitCode != exitcode.Fenced {
		t.Fatalf("paused: err = %v, want pipeline_paused (exit %d)", err, exitcode.Fenced)
	}
	if got := p.touched.take(touchDelta).keys; len(got[id]) != 1 {
		t.Fatalf("paused verify consumed the changed keys: %v", got)
	}
}

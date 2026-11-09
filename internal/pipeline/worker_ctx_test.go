package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// F11 / 03 §4: dừng theo yêu cầu (hủy ctx, pipeline không lỗi) KHÔNG được hủy context của worker —
// batch đang ghi dở phải xong trong lúc drain. Pipeline lỗi thì worker bị hủy ngay.
func TestWorkerContextSurvivesStopButNotFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := workerContext(ctx, func() error { return nil })
	cancel() // SIGTERM
	select {
	case <-wctx.Done():
		t.Fatal("worker context cancelled by a requested stop: in-flight batches would be dropped before drain")
	case <-time.After(50 * time.Millisecond):
	}
	wcancel() // sau drain
	if wctx.Err() == nil {
		t.Fatal("worker context not cancelled by its own cancel")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	failErr := errors.New("boom")
	wctx2, wcancel2 := workerContext(ctx2, func() error { return failErr })
	defer wcancel2()
	cancel2() // p.fail: failErr được gán trước khi hủy ctx
	select {
	case <-wctx2.Done():
	case <-time.After(time.Second):
		t.Fatal("worker context not cancelled when the pipeline failed")
	}
}

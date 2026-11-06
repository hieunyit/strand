package pipeline

// Gate 2 (04 §5, hot row → unverifiable_hot trên đường PG): verify --mode delta không còn bị từ chối ở
// pipeline PG→PG / MySQL→PG — khóa đã chạm được so bằng chuỗi chính tắc (canonicalVerifier). Unit test
// thuần, không cần DB: pipeline không có bảng nào nên không có truy vấn.

import (
	"strings"
	"testing"

	"strand/internal/model"
)

func TestG2DeltaVerifyAcceptsPGPipelines(t *testing.T) {
	for _, pair := range [][2]model.Engine{{model.EnginePG, model.EnginePG}, {model.EngineMySQL, model.EnginePG}} {
		p := &Pipeline{srcEngine: pair[0], dstEngine: pair[1], touched: newTouchedKeys(100), tables: map[model.TableID]*tableMeta{}}
		if p.liveMySQL() {
			t.Fatalf("%s→%s must not use the MySQL-only LiveVerifier", pair[0], pair[1])
		}
		// Chưa bật verify.delta: lỗi nói cần bật, không phải "chỉ MySQL→MySQL".
		if _, err := p.VerifyDelta(t.Context()); err == nil || !strings.Contains(err.Error(), "verify.delta: on") {
			t.Fatalf("%s→%s, delta off: err = %v, want the verify.delta: on hint", pair[0], pair[1], err)
		}
		p.touched.enable(touchDelta, true)
		msg, err := p.VerifyDelta(t.Context())
		if err != nil || !strings.Contains(msg, "0 mismatches") {
			t.Fatalf("%s→%s, delta on: msg = %q, err = %v", pair[0], pair[1], msg, err)
		}
	}
	if !(&Pipeline{srcEngine: model.EngineMySQL, dstEngine: model.EngineMySQL}).liveMySQL() {
		t.Fatal("MySQL→MySQL must keep using LiveVerifier")
	}
}

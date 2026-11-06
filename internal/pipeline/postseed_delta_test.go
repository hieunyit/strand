package pipeline

import (
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/model"
)

// review R21: post-seed verify không được tắt ghi khóa khi verify.delta: on; khi delta tắt thì vẫn
// tắt như trước.
func TestPostSeedKeepsDeltaTracking(t *testing.T) {
	id := model.TableID{Schema: "db", Table: "t"}
	for _, tc := range []struct {
		delta  string
		wantOn bool
	}{{"on", true}, {"ON", true}, {"", false}, {"off", false}} {
		cfg := &config.Config{}
		cfg.Verify.Delta = tc.delta
		p := &Pipeline{cfg: cfg, touched: newTouchedKeys(100)}
		p.touched.enable(touchPostSeed, true)
		if strings.EqualFold(tc.delta, "on") {
			p.touched.enable(touchDelta, true) // như Run khi verify.delta: on
		}
		p.touched.add(id, []model.Value{int64(1)})
		keys, _, _ := p.takePostSeedKeys()
		if len(keys[id]) != 1 {
			t.Fatalf("delta=%q: post-seed took %v, want key 1", tc.delta, keys)
		}
		// Khóa CDC chạm sau post-seed: verify delta phải thấy nếu delta on.
		p.touched.add(id, []model.Value{int64(2)})
		later := p.touched.take(touchDelta).keys
		if got := len(later[id]) > 0 && later[id][len(later[id])-1] == 2; got != tc.wantOn {
			t.Fatalf("delta=%q: tracking after post-seed = %v, want %v", tc.delta, got, tc.wantOn)
		}
	}
}

package verify

// Toán lấy mẫu của verify --mode sample theo tuple PK (sample_keys.go) — thuần, không cần DB.

import (
	"math"
	"math/rand"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestKeySampleCountBoundedByRate(t *testing.T) {
	for _, tc := range []struct {
		est  int64
		rate float64
		rows int64
		want int
	}{
		{0, 0.01, 1000, 1},          // bảng rỗng / chưa ANALYZE: vẫn một range
		{-1, 0.01, 1000, 1},         // reltuples = -1 (PG14+, chưa ANALYZE)
		{1_000_000, 0.01, 1000, 11}, // ≈ rate × row / rows (+1)
		{50_000, 0.01, 1000, 1},
		{1_000_000, 0.01, 0, 11}, // rows ≤ 0: mặc định 1000
		{1 << 62, 1, 1, 1_000_001},
	} {
		if got := keySampleCount(tc.est, tc.rate, tc.rows); got != tc.want {
			t.Errorf("keySampleCount(%d, %v, %d) = %d, want %d", tc.est, tc.rate, tc.rows, got, tc.want)
		}
		// Số row đọc ≈ n × rows không vượt rate × est quá một range.
		if tc.est > 0 && tc.rows > 0 && tc.est < 1<<40 {
			n := keySampleCount(tc.est, tc.rate, tc.rows)
			if float64(int64(n-1)*tc.rows) > float64(tc.est)*tc.rate {
				t.Errorf("est %d rate %v: %d ranges × %d rows exceed rate", tc.est, tc.rate, n, tc.rows)
			}
		}
	}
}

func TestTableSamplePercent(t *testing.T) {
	for _, tc := range []struct{ rate, want float64 }{
		{0.01, 1}, {0.5, 50}, {1, 100}, {3, 100}, {0, 0.0001}, {-1, 0.0001}, {math.NaN(), 100},
	} {
		if got := tableSamplePercent(tc.rate); got != tc.want {
			t.Errorf("tableSamplePercent(%v) = %v, want %v", tc.rate, got, tc.want)
		}
	}
}

func TestRandIntInStaysInRangeAtExtremes(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, r := range [][2]int64{{math.MinInt64, math.MaxInt64}, {-5, 5}, {7, 7}, {math.MinInt64, 0}, {-1, math.MaxInt64}, {9, 3}} {
		for i := 0; i < 200; i++ {
			v := randInt64In(rng, r[0], r[1])
			if r[1] >= r[0] && (v < r[0] || v > r[1]) {
				t.Fatalf("randInt64In%v = %d", r, v)
			}
			if r[1] < r[0] && v != r[0] {
				t.Fatalf("randInt64In%v (empty) = %d, want lo", r, v)
			}
		}
	}
	for _, r := range [][2]uint64{{0, math.MaxUint64}, {10, 20}, {5, 5}, {1 << 63, math.MaxUint64}} {
		for i := 0; i < 200; i++ {
			if v := randUint64In(rng, r[0], r[1]); v < r[0] || v > r[1] {
				t.Fatalf("randUint64In%v = %d", r, v)
			}
		}
	}
}

func TestRandomLeadOnlyForIntegerLeadColumn(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	intC := &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "a", Kind: snapshot.PKInt}, {Name: "b", Kind: snapshot.PKBytes}}}
	mn := snapshot.Bound{V: []model.Value{int64(-3), "x"}}
	mx := snapshot.Bound{V: []model.Value{int64(40), "y"}}
	for i := 0; i < 100; i++ {
		v, ok := randomLead(rng, intC, mn, mx)
		if !ok || v.(int64) < -3 || v.(int64) > 40 {
			t.Fatalf("int lead: %v %v", v, ok)
		}
	}
	uC := &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "a", Kind: snapshot.PKUint}, {Name: "b", Kind: snapshot.PKInt}}}
	v, ok := randomLead(rng, uC, snapshot.Bound{V: []model.Value{uint64(5), int64(0)}}, snapshot.Bound{V: []model.Value{uint64(9), int64(0)}})
	if !ok || v.(uint64) < 5 || v.(uint64) > 9 {
		t.Fatalf("uint lead: %v %v", v, ok)
	}
	sC := &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "s", Kind: snapshot.PKWeight}, {Name: "a", Kind: snapshot.PKInt}}}
	if _, ok := randomLead(rng, sC, snapshot.Bound{V: []model.Value{"a", int64(1)}}, snapshot.Bound{V: []model.Value{"z", int64(1)}}); ok {
		t.Fatal("string lead column must not be sampled by value")
	}
}

func TestPlanKeyStartsSortsDedupsCapsAndFallsBack(t *testing.T) {
	b := snapshot.IntBound
	got := planKeyStarts([]snapshot.Bound{b(9), b(3), b(9), b(7), b(1)}, 3, b(0))
	want := []int64{1, 3, 7}
	if len(got) != len(want) {
		t.Fatalf("got %d starts, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].K != b(w).K {
			t.Fatalf("start %d = %v, want %d", i, got[i].V, w)
		}
	}
	// TABLESAMPLE không trả row nào (bảng nhỏ): bắt đầu ở khóa nhỏ nhất.
	got = planKeyStarts(nil, 4, b(42))
	if len(got) != 1 || got[0].K != b(42).K {
		t.Fatalf("empty sample: %v, want [42]", got)
	}
}

func TestRangesFromStartsSkipsOverlap(t *testing.T) {
	b := snapshot.IntBound
	end := func(lo snapshot.Bound) (snapshot.Bound, error) { return b(lo.V[0].(int64) + 9), nil }
	got, err := rangesFromStarts([]snapshot.Bound{b(0), b(5), b(9), b(10), b(30)}, end)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]int64{{0, 9}, {10, 19}, {30, 39}}
	if len(got) != len(want) {
		t.Fatalf("got %d ranges, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Lo.K != b(w[0]).K || got[i].Hi.K != b(w[1]).K || got[i].All {
			t.Fatalf("range %d = [%v, %v], want %v", i, got[i].Lo.V, got[i].Hi.V, w)
		}
	}
}

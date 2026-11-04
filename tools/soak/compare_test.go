package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var testCols = []string{"id", "ver", "body"}

// fakeTable là bảng giả trong bộ nhớ, map pk -> giá trị các cột (không gồm id).
type fakeTable map[int64][2]any

// fakeFetcher mô phỏng keyset pagination (WHERE id > after ORDER BY id LIMIT n) và đếm số lần gọi,
// đồng thời ghi lại kích thước chunk lớn nhất để kiểm tra bộ nhớ O(chunk).
type fakeFetcher struct {
	tbl      fakeTable
	cols     []string
	calls    atomic.Int64
	maxChunk atomic.Int64
	failAt   int64 // >0: trả lỗi ở lần gọi thứ failAt
}

func (f *fakeFetcher) fetch(ctx context.Context, after int64, first bool, limit int) (cmpChunk, error) {
	n := f.calls.Add(1)
	if f.failAt > 0 && n == f.failAt {
		return cmpChunk{}, errors.New("boom")
	}
	if err := ctx.Err(); err != nil {
		return cmpChunk{}, err
	}
	keys := make([]int64, 0, len(f.tbl))
	for k := range f.tbl {
		if first || k > after {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if len(keys) > limit {
		keys = keys[:limit]
	}
	cols := f.cols
	if cols == nil {
		cols = testCols
	}
	out := cmpChunk{cols: cols}
	for _, k := range keys {
		v := f.tbl[k]
		out.rows = append(out.rows, cmpRow{pk: k, vals: []any{fmt.Sprint(k), v[0], v[1]}})
	}
	if int64(len(out.rows)) > f.maxChunk.Load() {
		f.maxChunk.Store(int64(len(out.rows)))
	}
	return out, nil
}

func genTable(n int) fakeTable {
	t := make(fakeTable, n)
	for i := 1; i <= n; i++ {
		t[int64(i*3)] = [2]any{fmt.Sprint(1000 + i), "x"}
	}
	return t
}

func cloneTable(t fakeTable) fakeTable {
	c := make(fakeTable, len(t))
	for k, v := range t {
		c[k] = v
	}
	return c
}

func runCompare(t *testing.T, src, dst fakeTable, chunk int) CompareResult {
	t.Helper()
	res := CompareResult{Equal: true}
	sf := &fakeFetcher{tbl: src}
	df := &fakeFetcher{tbl: dst}
	if err := compareTableStreaming(context.Background(), "app.events", sf.fetch, df.fetch, chunk, DefaultMaxViolations, &res); err != nil {
		t.Fatalf("compare: %v", err)
	}
	if c := int(sf.maxChunk.Load()); c > chunk {
		t.Fatalf("source chunk %d > limit %d", c, chunk)
	}
	if c := int(df.maxChunk.Load()); c > chunk {
		t.Fatalf("target chunk %d > limit %d", c, chunk)
	}
	return res
}

func TestCompareTableStreaming(t *testing.T) {
	base := genTable(50)

	cases := []struct {
		name      string
		src, dst  func() fakeTable
		wantTotal int64
		wantSub   []string
	}{
		{
			name: "Equal",
			src:  func() fakeTable { return cloneTable(base) },
			dst:  func() fakeTable { return cloneTable(base) },
		},
		{
			name: "EmptyBoth",
			src:  func() fakeTable { return fakeTable{} },
			dst:  func() fakeTable { return fakeTable{} },
		},
		{
			name:      "EmptyTarget",
			src:       func() fakeTable { return cloneTable(base) },
			dst:       func() fakeTable { return fakeTable{} },
			wantTotal: 50,
			wantSub:   []string{"pk=3 tồn tại ở source nhưng thiếu ở target"},
		},
		{
			name:      "EmptySource",
			src:       func() fakeTable { return fakeTable{} },
			dst:       func() fakeTable { return cloneTable(base) },
			wantTotal: 50,
			wantSub:   []string{"pk=3 tồn tại ở target nhưng không có ở source"},
		},
		{
			name: "MissingAtTarget",
			src:  func() fakeTable { return cloneTable(base) },
			dst: func() fakeTable {
				d := cloneTable(base)
				delete(d, 3)   // đầu bảng
				delete(d, 63)  // giữa
				delete(d, 150) // cuối bảng
				return d
			},
			wantTotal: 3,
			wantSub: []string{
				"pk=3 tồn tại ở source nhưng thiếu ở target",
				"pk=63 tồn tại ở source nhưng thiếu ở target",
				"pk=150 tồn tại ở source nhưng thiếu ở target",
			},
		},
		{
			name: "ExtraAtTarget",
			src:  func() fakeTable { return cloneTable(base) },
			dst: func() fakeTable {
				d := cloneTable(base)
				d[1] = [2]any{"1", "x"}    // trước dòng đầu
				d[64] = [2]any{"64", "x"}  // giữa hai khóa
				d[9999] = [2]any{"9", "x"} // sau dòng cuối
				return d
			},
			wantTotal: 3,
			wantSub: []string{
				"pk=1 tồn tại ở target nhưng không có ở source",
				"pk=64 tồn tại ở target nhưng không có ở source",
				"pk=9999 tồn tại ở target nhưng không có ở source",
			},
		},
		{
			name: "ValueDiff",
			src:  func() fakeTable { return cloneTable(base) },
			dst: func() fakeTable {
				d := cloneTable(base)
				d[30] = [2]any{"1010", "y"} // body khác
				d[33] = [2]any{"999", "x"}  // ver khác
				d[36] = [2]any{"1012", nil} // NULL khác "x"
				return d
			},
			wantTotal: 3,
			wantSub: []string{
				`pk=30 lệch hash`, `body: source="x" target="y"`,
				`pk=33 lệch hash`, `ver: source="1011" target="999"`,
				`pk=36 lệch hash`, `body: source="x" target=NULL`,
			},
		},
		{
			name: "Mixed",
			src: func() fakeTable {
				s := cloneTable(base)
				s[4] = [2]any{"4", "x"}
				return s
			},
			dst: func() fakeTable {
				d := cloneTable(base)
				d[5] = [2]any{"5", "x"}
				d[150] = [2]any{"1050", "z"}
				return d
			},
			wantTotal: 3,
			wantSub: []string{
				"pk=4 tồn tại ở source nhưng thiếu ở target",
				"pk=5 tồn tại ở target nhưng không có ở source",
				"pk=150 lệch hash",
			},
		},
	}

	// Nhiều kích thước chunk để biên chunk rơi vào các vị trí khác nhau (1 dòng, lẻ, đúng bằng bảng, lớn hơn bảng).
	chunks := []int{1, 2, 3, 7, 10, 49, 50, 51, 10000}
	for _, tc := range cases {
		for _, chunk := range chunks {
			t.Run(fmt.Sprintf("%s/chunk=%d", tc.name, chunk), func(t *testing.T) {
				res := runCompare(t, tc.src(), tc.dst(), chunk)
				if res.ViolationsTotal != tc.wantTotal {
					t.Fatalf("violations total = %d, want %d: %v", res.ViolationsTotal, tc.wantTotal, res.Violations)
				}
				if res.Equal != (tc.wantTotal == 0) {
					t.Fatalf("equal = %v, want %v", res.Equal, tc.wantTotal == 0)
				}
				all := strings.Join(res.Violations, "\n")
				for _, sub := range tc.wantSub {
					if !strings.Contains(all, sub) {
						t.Fatalf("missing %q in violations:\n%s", sub, all)
					}
				}
				if len(res.Violations) > DefaultMaxViolations {
					t.Fatalf("kept %d violations > %d", len(res.Violations), DefaultMaxViolations)
				}
			})
		}
	}
}

func TestCompareTableStreamingRowCounts(t *testing.T) {
	src := genTable(100)
	dst := cloneTable(src)
	delete(dst, 30)
	dst[31] = [2]any{"1", "x"}
	res := runCompare(t, src, dst, 7)
	if res.SourceRows != 100 || res.TargetRows != 100 {
		t.Fatalf("rows src=%d dst=%d, want 100/100", res.SourceRows, res.TargetRows)
	}
}

func TestCompareTableStreamingCapsViolations(t *testing.T) {
	src := genTable(500)
	res := runCompare(t, src, fakeTable{}, 13)
	if res.ViolationsTotal != 500 {
		t.Fatalf("total = %d, want 500", res.ViolationsTotal)
	}
	if len(res.Violations) != DefaultMaxViolations {
		t.Fatalf("kept %d, want %d", len(res.Violations), DefaultMaxViolations)
	}
	// Vi phạm giữ lại là 20 vi phạm đầu theo thứ tự PK.
	if !strings.Contains(res.Violations[0], "pk=3 ") || !strings.Contains(res.Violations[19], "pk=60 ") {
		t.Fatalf("unexpected order: first=%q last=%q", res.Violations[0], res.Violations[19])
	}
}

func TestCompareTableStreamingColumnOrderAndSet(t *testing.T) {
	src := genTable(5)
	// Target trả cột theo thứ tự khác: so sánh theo tên cột nên vẫn bằng nhau.
	sf := &fakeFetcher{tbl: src}
	df := &reorderFetcher{inner: &fakeFetcher{tbl: cloneTable(src)}}
	res := CompareResult{Equal: true}
	if err := compareTableStreaming(context.Background(), "app.events", sf.fetch, df.fetch, 2, 20, &res); err != nil {
		t.Fatal(err)
	}
	if !res.Equal {
		t.Fatalf("reordered columns should compare equal: %v", res.Violations)
	}

	// Target thiếu cột body: mọi dòng lệch.
	df2 := &fakeFetcher{tbl: cloneTable(src), cols: []string{"id", "ver", "other"}}
	res = CompareResult{Equal: true}
	if err := compareTableStreaming(context.Background(), "app.events", sf.fetch, df2.fetch, 2, 20, &res); err != nil {
		t.Fatal(err)
	}
	if res.Equal || res.ViolationsTotal != 5 || !strings.Contains(res.Violations[0], "body: thiếu cột ở target") {
		t.Fatalf("expected column-set violations, got %v", res.Violations)
	}
}

type reorderFetcher struct{ inner *fakeFetcher }

func (r *reorderFetcher) fetch(ctx context.Context, after int64, first bool, limit int) (cmpChunk, error) {
	c, err := r.inner.fetch(ctx, after, first, limit)
	if err != nil {
		return c, err
	}
	c.cols = []string{"body", "id", "ver"}
	for i := range c.rows {
		v := c.rows[i].vals
		c.rows[i].vals = []any{v[2], v[0], v[1]}
	}
	return c, nil
}

func TestCompareTableStreamingFetchErrorStopsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	src := genTable(1000)
	sf := &fakeFetcher{tbl: src}
	df := &fakeFetcher{tbl: cloneTable(src), failAt: 3}
	res := CompareResult{Equal: true}
	err := compareTableStreaming(context.Background(), "app.events", sf.fetch, df.fetch, 10, 20, &res)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected fetch error, got %v", err)
	}
	// Goroutine prefetch của phía source phải dừng (không rò rỉ) sau khi compare thoát sớm.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines leaked: before=%d after=%d", before, n)
	}
	// Không đọc hết bảng: chỉ prefetch tối đa vài chunk.
	if c := sf.calls.Load(); c > 6 {
		t.Fatalf("source fetched %d chunks after target error; prefetch should be bounded", c)
	}
}

func TestCompareTableStreamingNonMonotonicPK(t *testing.T) {
	bad := func(ctx context.Context, after int64, first bool, limit int) (cmpChunk, error) {
		if !first {
			return cmpChunk{cols: testCols}, nil
		}
		return cmpChunk{cols: testCols, rows: []cmpRow{
			{pk: 5, vals: []any{"5", "1", "x"}},
			{pk: 4, vals: []any{"4", "1", "x"}},
		}}, nil
	}
	res := CompareResult{Equal: true}
	err := compareTableStreaming(context.Background(), "app.events", bad, bad, 10, 20, &res)
	if err == nil || !strings.Contains(err.Error(), "keyset pagination") {
		t.Fatalf("expected keyset ordering error, got %v", err)
	}
}

func TestRSSGuard(t *testing.T) {
	var cur int64 = 100 << 20
	var tripped []string
	g := newRSSGuard(512<<20, func() (int64, error) { return cur, nil }, func(r string) { tripped = append(tripped, r) })
	if got := g.check(); got != cur || g.Reason() != "" || len(tripped) != 0 {
		t.Fatalf("below limit should not trip: got=%d reason=%q", got, g.Reason())
	}
	cur = 600 << 20
	g.check()
	cur = 700 << 20
	g.check()
	if len(tripped) != 1 {
		t.Fatalf("onTrip must fire exactly once, fired %d", len(tripped))
	}
	if !strings.Contains(g.Reason(), "exceeds --driver-rss-limit-mb 512 MiB") {
		t.Fatalf("unexpected reason %q", g.Reason())
	}
	cur = 50 << 20
	g.check()
	if g.max.Load() != 700<<20 {
		t.Fatalf("max = %d, want %d", g.max.Load(), 700<<20)
	}

	// limit = 0 tắt guard.
	off := newRSSGuard(0, func() (int64, error) { return 1 << 40, nil }, func(string) { t.Fatal("must not trip") })
	off.check()
}

func TestReadSelfRSS(t *testing.T) {
	rss, err := readSelfRSSBytes()
	if err != nil {
		t.Fatalf("read self RSS: %v", err)
	}
	if rss <= 0 {
		t.Fatalf("rss = %d", rss)
	}
}

func TestComputeRSSGrowthIgnoresSamplesWithoutStrand(t *testing.T) {
	t0 := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	// Mẫu thực tế từ đợt soak 15 phút: RSS phẳng ~31 MB, hai mẫu 0 khi strand đang restart.
	rss := []float64{31.9, 31.5, 31.5, 36.7, 34.8, 0, 32.0, 30.9, 33.2, 0, 31.0, 30.9, 31.6, 31.3, 32.1}
	var samples []Sample
	for i, v := range rss {
		samples = append(samples, Sample{
			Timestamp:      t0.Add(time.Duration(i+1) * time.Minute),
			ElapsedSeconds: float64((i + 1) * 60),
			RSSMB:          v,
		})
	}
	got := computeRSSGrowthMBPerHour(samples)
	if got > DefaultMaxRSSGrowthMBPerHour || got < -DefaultMaxRSSGrowthMBPerHour {
		t.Fatalf("rss_growth = %.3f MB/h; zero samples (strand not running) must be ignored", got)
	}
}

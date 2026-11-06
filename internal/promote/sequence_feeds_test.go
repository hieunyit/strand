package promote_test

import (
	"context"
	"fmt"
	"testing"

	"strand/internal/promote"
)

// mysqlLikeSource: source MySQL đã fence được, không có sequence (HasSequences = false).
type mysqlLikeSource struct{ *mockSourceDriver }

func (mysqlLikeSource) HasSequences() bool { return false }

// Hồi quy: MySQLSourceDriver.ReadSequenceLastValue dựng "SELECT COALESCE(MAX(x), 0) FROM x" (SQL sai)
// — nay trả lỗi rõ ràng mà không chạm DB, và driver khai báo không có sequence.
func TestMySQLSourceHasNoSequences(t *testing.T) {
	d := promote.NewMySQLSourceDriver(nil)
	if _, err := d.ReadSequenceLastValue(context.Background(), "order_no_seq"); err == nil {
		t.Fatalf("MySQL source must not return a value for a PG sequence")
	}
	var src promote.SourceDriver = d
	if sl, ok := src.(promote.SequencelessSource); !ok || sl.HasSequences() {
		t.Fatalf("MySQL source driver must report HasSequences() = false")
	}
}

// MySQL→PG: sequence độc lập ở target lấy max của các cột nó cấp (DEFAULT nextval) + margin, không đọc
// source và không dừng exit 10 — kể cả khi source còn truy cập được (trước đây: SQL sai → exit 10).
func TestMySQLSourceIndependentSequenceFromTargetFeeds(t *testing.T) {
	ctx := context.Background()
	src := mysqlLikeSource{newMockSourceDriver(true)}
	target := newMockTargetDriver()
	target.maxColValues["orders.order_no"] = 5000
	target.maxColValues["archive.order_no"] = 7000
	seqs := []promote.SequenceInfo{
		{Name: "order_no_seq", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1,
			Feeds: []promote.ColumnRef{{Table: "orders", Column: "order_no"}, {Table: "archive", Column: "order_no"}}},
		{Name: "free_seq", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1},
	}
	cfg := promote.Config{PipelineID: "my2pg", PeakRate: 10, SyncIntervalSec: 1}
	p := promote.NewPromoter(cfg, promote.NewMemoryStateStore(), src, target, seqs, nil)
	progress, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("promote MySQL→PG with independent sequences must succeed, got %v", err)
	}
	margin := promote.CalculateMargin(1, 1, 10, 1)
	if got, want := target.setvalValues["order_no_seq"], int64(7000)+margin; got != want {
		t.Fatalf("setval order_no_seq = %d, want max(feeds) + margin = %d", got, want)
	}
	if got := target.setvalValues["free_seq"]; got != margin {
		t.Fatalf("setval free_seq = %d, want %d (no feeds: never behind, setval keeps GREATEST)", got, margin)
	}
	if n := src.readCalls["order_no_seq"] + src.readCalls["free_seq"]; n != 0 {
		t.Fatalf("MySQL source must not be asked for sequence values (%d reads)", n)
	}
	if progress.State != promote.StatePromoted {
		t.Fatalf("state = %s, want promoted", progress.State)
	}
}

// Gợi ý khi phải hỏi người vận hành không được thấp hơn max(cột target mà sequence cấp) + margin: lần
// đồng bộ cuối có thể là 0 (không có đồng bộ định kỳ, ví dụ source MySQL) hoặc đã cũ.
func TestIndependentSequenceSuggestionCoversTargetFeeds(t *testing.T) {
	ctx := context.Background()
	target := newMockTargetDriver()
	target.maxColValues["orders.order_no"] = 9000
	seqs := []promote.SequenceInfo{{Name: "order_no_seq", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1,
		LastSyncedValue: 100, Feeds: []promote.ColumnRef{{Table: "orders", Column: "order_no"}}}}
	cfg := promote.Config{PipelineID: "sugg", PeakRate: 10, SyncIntervalSec: 1}
	margin := promote.CalculateMargin(1, 1, 10, 1)

	// Không fence được (source không truy cập được): dừng exit 10 kèm gợi ý.
	p := promote.NewPromoter(cfg, promote.NewMemoryStateStore(), newMockSourceDriver(false), target, seqs, nil)
	progress, err := p.Run(ctx)
	if err == nil {
		t.Fatalf("unfenced source must stop for an operator decision")
	}
	if got, want := progress.SuggestedSequences["order_no_seq"], int64(9000)+margin; got != want {
		t.Fatalf("suggestion = %d, want max(feeds) + margin = %d (last sync + margin = %d is below used values)", got, want, 100+margin)
	}

	// Fence xong rồi source mất ở bước 4: gợi ý cũng vậy.
	src := newMockSourceDriver(true)
	target.applyHook = func() error { src.mu.Lock(); src.reachable = false; src.mu.Unlock(); return nil }
	p = promote.NewPromoter(cfg, promote.NewMemoryStateStore(), src, target, seqs, nil)
	progress, err = p.Run(ctx)
	if err == nil {
		t.Fatalf("unreadable independent sequence must stop for an operator decision")
	}
	if got, want := progress.SuggestedSequences["order_no_seq"], int64(9000)+margin; got != want {
		t.Fatalf("step-4 suggestion = %d, want %d", got, want)
	}

	// Source MySQL không fence được: không có gì để người vận hành chỉ định cho sequence (bước 4 tự suy ra).
	my := mysqlLikeSource{newMockSourceDriver(false)}
	p = promote.NewPromoter(cfg, promote.NewMemoryStateStore(), my, newMockTargetDriver(), seqs, nil)
	progress, _ = p.Run(ctx)
	if len(progress.SuggestedSequences) != 0 {
		t.Fatalf("MySQL source: no sequence suggestions expected, got %v", progress.SuggestedSequences)
	}
}

// Margin theo CACHE của source (lấy max với target); tên gợi ý in theo thứ tự ổn định.
func TestMergeSourceCacheAndSortedNames(t *testing.T) {
	target := []promote.SequenceInfo{{Name: "a", CacheSize: 1}, {Name: "b", CacheSize: 50}, {Name: "c", CacheSize: 1}}
	source := []promote.SequenceInfo{{Name: "a", CacheSize: 1000}, {Name: "b", CacheSize: 10}}
	got := promote.MergeSourceCache(target, source)
	if got[0].CacheSize != 1000 || got[1].CacheSize != 50 || got[2].CacheSize != 1 {
		t.Fatalf("merged cache sizes: %+v", got)
	}
	if target[0].CacheSize != 1 {
		t.Fatalf("input slice must not be modified")
	}
	names := promote.SortedSequenceNames(map[string]int64{"z": 1, "a": 2, "m": 3, "b.x": 4})
	if want := []string{"a", "b.x", "m", "z"}; fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("sorted names = %v, want %v", names, want)
	}
}

// Resume: CACHE đã lưu bởi đồng bộ định kỳ (đọc từ source) không bị CACHE target nhỏ hơn đè mất.
func TestResumeKeepsLargerSavedCache(t *testing.T) {
	ctx := context.Background()
	store := promote.NewMemoryStateStore()
	if err := store.SaveProgress(ctx, &promote.Progress{PipelineID: "cache", State: promote.StateValidating,
		Sequences:          []promote.SequenceInfo{{Name: "s", Type: promote.SequenceTypeIndependent, CacheSize: 1000, IncrementBy: 1, LastSyncedValue: 10}},
		SuggestedSequences: map[string]int64{}}); err != nil {
		t.Fatal(err)
	}
	seqs := []promote.SequenceInfo{{Name: "s", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1}}
	cfg := promote.Config{PipelineID: "cache", PeakRate: 10, SyncIntervalSec: 1}
	progress, err := promote.NewPromoter(cfg, store, newMockSourceDriver(false), newMockTargetDriver(), seqs, nil).Run(ctx)
	if err == nil {
		t.Fatalf("unfenced source must stop for an operator decision")
	}
	if got, want := progress.SuggestedSequences["s"], 10+promote.CalculateMargin(1000, 1, 10, 1); got != want {
		t.Fatalf("suggestion = %d, want %d (margin from the source CACHE 1000)", got, want)
	}
}

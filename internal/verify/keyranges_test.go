package verify

import (
	"math"
	"reflect"
	"testing"
)

// Post-seed verify: khóa CDC chạm phải được gộp thành ít range (không phải 1 range/khóa),
// mọi khóa vẫn nằm trong một range, range không rộng quá keyRangeMaxSpan.
func TestKeyRangesCoalescesAndCoversEveryKey(t *testing.T) {
	keys := []int64{5, 3, 4, 3, 100, 69, 5000} // 5→69 cách 64 (gộp), 100→5000 tách
	got := KeyRanges(keys)
	want := [][2]int64{{3, 100}, {5000, 5000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	var dense []int64
	for k := int64(1); k <= 30000; k++ {
		dense = append(dense, k)
	}
	r := KeyRanges(dense)
	if len(r) != 30 {
		t.Fatalf("30000 contiguous keys -> %d ranges, want 30", len(r))
	}
	for _, x := range r {
		if x[1]-x[0] >= keyRangeMaxSpan {
			t.Fatalf("range %v wider than %d", x, keyRangeMaxSpan)
		}
	}
	for _, k := range keys {
		in := false
		for _, x := range got {
			if k >= x[0] && k <= x[1] {
				in = true
			}
		}
		if !in {
			t.Fatalf("key %d not covered", k)
		}
	}
	if KeyRanges(nil) != nil {
		t.Fatal("nil keys must give nil")
	}
}

// Review 4.1: k-prev tràn int64 khi khóa ở hai đầu int64 → hai khóa xa nhau bị gộp thành
// range [MinInt64, MaxInt64] (đọc lại cả bảng).
func TestKeyRangesInt64Extremes(t *testing.T) {
	got := KeyRanges([]int64{math.MaxInt64, math.MinInt64, 0})
	want := [][2]int64{{math.MinInt64, math.MinInt64}, {0, 0}, {math.MaxInt64, math.MaxInt64}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	got = KeyRanges([]int64{math.MaxInt64 - 2, math.MaxInt64})
	if want := [][2]int64{{math.MaxInt64 - 2, math.MaxInt64}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

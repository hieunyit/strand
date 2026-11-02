package verify

import (
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

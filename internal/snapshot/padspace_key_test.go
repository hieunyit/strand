package snapshot

import (
	"bytes"
	"math/rand"
	"testing"
)

// padCompare là phép so PAD SPACE tham chiếu: mảng đơn vị ngắn hơn được đệm bằng sp.
func padCompare(a, b [][]byte, sp []byte) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		x, y := sp, sp
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if c := bytes.Compare(x, y); c != 0 {
			return c
		}
	}
	return 0
}

// review N04: khóa PadSpaceWeightKey so byte phải trùng phép so PAD SPACE (bằng nhau và thứ tự) với mọi
// cặp mảng weight — gồm space cuối, đoạn space giữa chuỗi, đơn vị nhỏ hơn / lớn hơn weight của space.
func TestN04_PadSpaceWeightKeyMatchesPadCompare(t *testing.T) {
	for _, width := range []int{1, 2} {
		sp := []byte{0x02, 0x09}[:width]
		if width == 1 {
			sp = []byte{0x20}
		}
		units := [][]byte{sp}
		for _, d := range []int{-0x10, -1, 1, 0x10, 0x40} {
			u := append([]byte(nil), sp...)
			u[width-1] = byte(int(u[width-1]) + d)
			units = append(units, u)
		}
		r := rand.New(rand.NewSource(int64(width)))
		gen := func() [][]byte {
			n := r.Intn(6)
			out := make([][]byte, n)
			for i := range out {
				out[i] = units[r.Intn(len(units))]
			}
			return out
		}
		flat := func(us [][]byte) []byte { return bytes.Join(us, nil) }
		for i := 0; i < 20000; i++ {
			a, b := gen(), gen()
			ka, err := PadSpaceWeightKey(flat(a), sp)
			if err != nil {
				t.Fatal(err)
			}
			kb, err := PadSpaceWeightKey(flat(b), sp)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := bytes.Compare(ka, kb), padCompare(a, b, sp); got != want {
				t.Fatalf("width %d: %x vs %x: key order %d, PAD SPACE order %d", width, flat(a), flat(b), got, want)
			}
		}
	}
	if _, err := PadSpaceWeightKey([]byte{1, 2, 3}, []byte{0, 0x20}); err == nil {
		t.Fatal("odd-length weight accepted for a 2-byte unit")
	}
}

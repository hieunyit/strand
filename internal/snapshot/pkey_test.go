package snapshot

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"strand/internal/model"
)

func mustKey(t *testing.T, kind PKKind, v model.Value) Key {
	t.Helper()
	k, err := (&PKCodec{Cols: []PKColumn{{Name: "k", Kind: kind}}}).Encode([]model.Value{v})
	if err != nil {
		t.Fatalf("encode %v %#v: %v", kind, v, err)
	}
	return k
}

func TestGPK_DecimalEncodingIsNumericOrder(t *testing.T) {
	for _, eq := range [][2]string{{"1", "1.000"}, {"01.50", "1.5"}, {"0", "-0.000"}, {"-2.10", "-2.1"}, {".5", "0.50"}} {
		if mustKey(t, PKDecimal, eq[0]) != mustKey(t, PKDecimal, eq[1]) {
			t.Errorf("%s and %s must encode equally", eq[0], eq[1])
		}
	}
	r := rand.New(rand.NewSource(1))
	gen := func() string {
		s := fmt.Sprintf("%d.%0*d", r.Int63n(1e6)*int64(r.Intn(2))*int64(r.Intn(1000)), 1+r.Intn(8), r.Int63n(1e8))
		if r.Intn(2) == 0 {
			s = "-" + s
		}
		return s
	}
	for i := 0; i < 20000; i++ {
		a, b := gen(), gen()
		ra, _ := new(big.Rat).SetString(a)
		rb, _ := new(big.Rat).SetString(b)
		ka, kb := mustKey(t, PKDecimal, a), mustKey(t, PKDecimal, b)
		if c := ra.Cmp(rb); (c < 0) != (ka < kb) || (c == 0) != (ka == kb) {
			t.Fatalf("%s vs %s: numeric cmp %d, key order disagrees", a, b, c)
		}
	}
}

func TestGPK_PadSpaceAndBytes(t *testing.T) {
	if mustKey(t, PKPadBytes, "ab") != mustKey(t, PKPadBytes, []byte("ab   ")) {
		t.Error("PAD SPACE: 'ab' and 'ab   ' are the same key")
	}
	if _, err := (&PKCodec{Cols: []PKColumn{{Name: "k", Kind: PKPadBytes}}}).Encode([]model.Value{"a\tb"}); err == nil {
		t.Error("PAD SPACE value with a control byte must be rejected")
	}
	// NO PAD / bytea: tiền tố đứng trước, byte 0x00 giữ thứ tự.
	order := []string{"", "\x00", "\x00\x00", "\x00\x01", "a", "a\x00", "a ", "ab", "\xff"}
	for i := 1; i < len(order); i++ {
		if !(mustKey(t, PKBytes, order[i-1]) < mustKey(t, PKBytes, order[i])) {
			t.Fatalf("bytes order broken at %q < %q", order[i-1], order[i])
		}
	}
	if mustKey(t, PKTrimBytes, "x  ") != mustKey(t, PKTrimBytes, "x") {
		t.Error("bpchar trailing spaces are insignificant")
	}
	if mustKey(t, PKUUID, "00000000-0000-0000-0000-0000000000ff") >= mustKey(t, PKUUID, "00000000-0000-0000-0000-000000000100") {
		t.Error("uuid order")
	}
}

func TestGPK_IntegersAndTimes(t *testing.T) {
	if mustKey(t, PKInt, int64(-1)) >= mustKey(t, PKInt, []byte("0")) || mustKey(t, PKInt, int32(5)) != mustKey(t, PKInt, "5") {
		t.Error("int encoding")
	}
	if mustKey(t, PKUint, uint64(1<<63)) <= mustKey(t, PKUint, []byte("9223372036854775807")) {
		t.Error("unsigned above MaxInt64")
	}
	if mustKey(t, PKDateTime, "2026-01-01 00:00:00") != mustKey(t, PKDateTime, "2026-01-01 00:00:00.000000") ||
		mustKey(t, PKDateTime, "2026-01-01T00:00:00.5Z") != mustKey(t, PKDateTime, "2026-01-01 00:00:00.500") {
		t.Error("datetime formats of the same value must encode equally")
	}
	if mustKey(t, PKDateTime, "0000-00-00 00:00:00") >= mustKey(t, PKDateTime, "0001-01-01") {
		t.Error("zero date sorts first")
	}
	if mustKey(t, PKTime, "-00:00:01") >= mustKey(t, PKTime, "00:00:00") || mustKey(t, PKTime, "838:59:59") <= mustKey(t, PKTime, "99:00:00") {
		t.Error("time order")
	}
	if EncodeInt64(7) != mustKey(t, PKInt, int64(7)) {
		t.Error("EncodeInt64 must match the int codec")
	}
}

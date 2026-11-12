package mysql

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"net/netip"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/model"
)

// Kỳ vọng lấy từ SELECT CAST(... AS INET6) của MariaDB 10.6/11.4 thật (cũng đã đối chiếu 95k địa chỉ).
func TestMariaDBInet6TextMatchesServer(t *testing.T) {
	for in, want := range map[string]string{
		"::": "::", "::1": "::1", "::1.2.3.4": "::1.2.3.4", "::ffff:1.2.3.4": "::ffff:1.2.3.4",
		"0:0:0:0:0:ffff:0:0": "::ffff:0.0.0.0", "1:0:0:2:0:0:0:3": "1:0:0:2::3", "1:0:2:0:3:0:4:0": "1::2:0:3:0:4:0",
		"1::": "1::", "0:0:0:0:0:0:1:2": "::0.1.0.2", "::ffff:0:1": "::ffff:0.0.0.1", "FE80::A": "fe80::a",
		"::0.0.0.2": "::2", "0:0:0:0:0:1:0:0": "::1:0:0", "1:2:3:4:5:6:0:8": "1:2:3:4:5:6::8",
	} {
		a := netip.MustParseAddr(in).As16()
		if got := mariaInet6Text(a[:]); got != want {
			t.Errorf("%s: got %q want %q", in, got, want)
		}
	}
}

// Field_string cắt 0x00 cuối: giá trị thô ngắn hơn phải được đệm lại.
func TestMariaDBFixedTypesPadTrailingZeros(t *testing.T) {
	info := &TableInfo{Columns: []ColumnInfo{{Name: "a", DataType: "inet6"}, {Name: "b", DataType: "inet4"}, {Name: "c", DataType: "uuid"}}}
	rows := [][]any{{"\x20\x01\x0d\xb8", "\x0a", "\x12\x34\x56\x78\x9a\xbc\x11\xef\x80"}, {nil, "", "\x00"}}
	if err := convertMariaDBTypes(&replication.TableMapEvent{}, info, rows); err != nil {
		t.Fatal(err)
	}
	want := [][]any{{"2001:db8::", "10.0.0.0", "12345678-9abc-11ef-8000-000000000000"}, {nil, "0.0.0.0", "00000000-0000-0000-0000-000000000000"}}
	for i := range want {
		for j := range want[i] {
			if rows[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %v want %v", i, j, rows[i][j], want[i][j])
			}
		}
	}
}

func TestDecompressMariaDBValue(t *testing.T) {
	orig := bytes.Repeat([]byte("strand-compress "), 100)
	var raw, wrapped bytes.Buffer
	fw, _ := flate.NewWriter(&raw, 6)
	fw.Write(orig)
	fw.Close()
	zw := zlib.NewWriter(&wrapped)
	zw.Write(orig)
	zw.Close()
	hdr := func(h byte, n int) []byte { return []byte{h, byte(n >> 8), byte(n)} }
	cases := map[string][]byte{
		"raw deflate": append(hdr(0x80|0x08|2, len(orig)), raw.Bytes()...),
		"zlib":        append(hdr(0x80|2, len(orig)), wrapped.Bytes()...),
		"stored":      append([]byte{0x00}, orig...),
	}
	for name, in := range cases {
		out, err := decompressMariaDBValue(in)
		if err != nil || !bytes.Equal(out, orig) {
			t.Errorf("%s: err=%v equal=%v", name, err, bytes.Equal(out, orig))
		}
	}
	if out, err := decompressMariaDBValue(nil); err != nil || len(out) != 0 {
		t.Errorf("empty: %v %v", out, err)
	}
	// Độ dài khai báo sai hoặc header lạ phải lỗi, không ghi sai.
	if _, err := decompressMariaDBValue(append(hdr(0x80|0x08|2, len(orig)+1), raw.Bytes()...)); err == nil {
		t.Error("length mismatch accepted")
	}
	if _, err := decompressMariaDBValue([]byte{0x42, 1}); err == nil {
		t.Error("unknown header accepted")
	}
}

func TestSplitSystemVersioned(t *testing.T) {
	const max, ts = sysVerRowEndMax, "2026-01-01 00:00:00.000000"
	cur := func(id int) []any { return []any{id, "v", "2026-01-01 00:00:00.000000", max} }
	his := func(id int) []any { return []any{id, "v", "2025-01-01 00:00:00.000000", ts} }
	segs, err := splitSystemVersioned(model.OpInsert, [][]any{cur(1), his(2)}, 2) // INSERT: chỉ hàng hiện hành
	if err != nil || len(segs) != 1 || len(segs[0].rows) != 1 || len(segs[0].rows[0]) != 2 {
		t.Fatalf("insert: %+v %v", segs, err)
	}
	// UPDATE hiện hành→hiện hành; UPDATE đóng hàng (→ DELETE); INSERT lịch sử (bỏ).
	segs, err = splitSystemVersioned(model.OpUpdate, [][]any{cur(1), cur(1), cur(3), his(3)}, 2)
	if err != nil || len(segs) != 2 || segs[0].op != model.OpUpdate || segs[1].op != model.OpDelete || len(segs[1].rows) != 1 {
		t.Fatalf("update: %+v %v", segs, err)
	}
	if _, err := splitSystemVersioned(model.OpInsert, [][]any{{1, "v"}}, 2); err == nil {
		t.Error("short row accepted")
	}
}

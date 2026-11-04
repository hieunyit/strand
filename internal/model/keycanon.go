package model

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Khóa trong writeset phải "bằng nhau" ít nhất mọi khi DB coi chúng bằng nhau (invariant: CO-1).
// Collation không phân biệt hoa/thường, dấu, độ rộng hay khoảng trắng cuối (MySQL *_ci/*_ai, PAD SPACE,
// PG nondeterministic) khiến 'a' và 'A' va cùng một UNIQUE/PK ở DB, trong khi so sánh byte coi là khác:
// scheduler thiếu cạnh và hai Txn xung đột chạy song song (review R01: T2 INSERT 'A' chạy trước T1
// UPDATE 'a'→'b', ON DUPLICATE KEY UPDATE đè row cũ, mất một row).
//
// Ta không biết collation của từng cột ở tầng này, nên gập BẢO THỦ: mọi chuỗi/[]byte được đưa về một
// "bộ xương" thô hơn mọi collation thường gặp — gập thừa chỉ thêm cạnh (mất chút song song), không bao
// giờ thiếu cạnh. Bộ xương: NFKD, bỏ dấu (Mn/Me) và ký tự định dạng (Cf), chữ thường theo SimpleFold,
// tách chữ ghép (ß→s, æ→ae, ...), gộp các ký tự lặp liền nhau (để 'ß'='ss'='s' như UCA và general_ci),
// bỏ khoảng trắng cuối.

// canonValue trả về dạng chuẩn của một giá trị khóa để hash.
func canonValue(v Value) string {
	// Không gắn tiền tố kiểu: cùng một giá trị có thể đến dưới dạng số ở đường này và chuỗi ở đường
	// khác (DECIMAL, FK con int32 vs PK cha int64) — chúng phải hash bằng nhau.
	switch x := v.(type) {
	case string:
		return foldString(x)
	case []byte:
		return foldString(string(x))
	default:
		return fmt.Sprint(x)
	}
}

// canonKey nối các thành phần khóa với length prefix (không mơ hồ khi có dấu phân cách trong dữ liệu).
func canonKey(vals []Value) string {
	var b strings.Builder
	for _, v := range vals {
		c := canonValue(v)
		fmt.Fprintf(&b, "%d:", len(c))
		b.WriteString(c)
	}
	return b.String()
}

var ligatures = map[rune]string{
	'ß': "s", 'ẞ': "s", 'æ': "ae", 'Æ': "ae", 'œ': "oe", 'Œ': "oe", 'ø': "o", 'Ø': "o",
	'đ': "d", 'Đ': "d", 'ð': "d", 'Ð': "d", 'ł': "l", 'Ł': "l", 'þ': "th", 'Þ': "th",
	'ı': "i", 'ĸ': "k", 'ŀ': "l", 'Ŀ': "l", 'ŉ': "n", 'ſ': "s", 'ĳ': "ij", 'Ĳ': "ij",
}

// foldString là bộ xương bảo thủ của s (xem chú thích đầu file).
func foldString(s string) string {
	if !utf8.ValidString(s) {
		return s // byte nhị phân không hợp lệ UTF-8: so khớp đúng byte (collation binary)
	}
	d := norm.NFKD.String(s)
	var out []rune
	push := func(r rune) {
		if n := len(out); n > 0 && out[n-1] == r {
			return // gộp lặp liền nhau
		}
		out = append(out, r)
	}
	for _, r := range d {
		if unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf) {
			continue
		}
		if l, ok := ligatures[r]; ok {
			for _, x := range l {
				push(minFold(x))
			}
			continue
		}
		push(minFold(r))
	}
	for len(out) > 0 && unicode.IsSpace(out[len(out)-1]) {
		out = out[:len(out)-1]
	}
	return string(out)
}

// minFold trả về phần tử nhỏ nhất trong lớp SimpleFold của r (k, K, và ký hiệu Kelvin cùng một lớp).
func minFold(r rune) rune {
	m := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if f < m {
			m = f
		}
	}
	return m
}

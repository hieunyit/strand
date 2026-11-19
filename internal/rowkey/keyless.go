package rowkey

import (
	"fmt"
	"strings"
)

// KeylessLabel: nhãn khóa của bảng chạy ở chế độ keyless (so khớp theo toàn bộ hàng).
const KeylessLabel = "full row (keyless)"

// KeylessColumnProblem trả lý do một cột không dùng được trong bảng keyless ("" nếu dùng được). dataType là
// DATA_TYPE của MySQL/MariaDB (pg=false) hoặc typname của PostgreSQL (pg=true), chữ thường.
//
// UPDATE/DELETE của bảng keyless khớp row theo MỌI cột (`<=>` / IS NOT DISTINCT FROM), nên kiểu cột phải có phép
// so bằng chính xác, đối xứng giữa source và target:
//   - FLOAT/DOUBLE/REAL: giá trị đọc từ binlog/WAL và giá trị đã lưu ở target có thể khác vài bit (làm tròn khi
//     chuyển đổi, MySQL→PG) nên so bằng có thể không khớp; từ chối trừ khi allowFloat.
//   - JSON (MySQL: so theo ngữ nghĩa JSON, chuẩn hóa thứ tự khóa; PG json không có phép so bằng) và hình học
//     (MySQL geometry; PG point/line/box/circle... so bằng theo diện tích hoặc không xác định): từ chối.
func KeylessColumnProblem(dataType string, pg, allowFloat bool) string {
	t := strings.ToLower(strings.TrimSpace(dataType))
	if pg {
		// Catalog array names use an underscore; textual callers may use [].
		t = strings.TrimPrefix(t, "_")
		for strings.HasSuffix(t, "[]") {
			t = strings.TrimSuffix(t, "[]")
		}
	}
	switch t {
	case "float", "double", "real", "float4", "float8", "double precision":
		if !allowFloat {
			return "floating-point column (equality may not match after conversion; set tables.keyless_allow_float: true to accept)"
		}
	case "json", "xml":
		return t + " column (no reliable equality)"
	}
	if pg {
		switch t {
		case "citext":
			return "citext column (case-insensitive equality does not preserve the full row image)"
		case "point", "line", "lseg", "box", "path", "polygon", "circle":
			return t + " column (no reliable equality)"
		}
		return ""
	}
	switch t {
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection", "geomcollection":
		return t + " column (no reliable equality)"
	}
	return ""
}

// KeylessProblem: lý do bảng (cột → kiểu) không chạy được ở chế độ keyless ("" nếu được).
func KeylessProblem(cols []string, types map[string]string, pg, allowFloat bool) string {
	var bad []string
	for _, c := range cols {
		if why := KeylessColumnProblem(types[c], pg, allowFloat); why != "" {
			bad = append(bad, fmt.Sprintf("%s: %s", c, why))
		}
	}
	return strings.Join(bad, "; ")
}

package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

// Kiểu INET6 / INET4 / UUID của MariaDB (10.5+/10.10+/10.7+) không có ở MySQL; bảng đích dùng VARCHAR/CHAR
// chứa đúng dạng chữ mà SELECT của MariaDB trả. Binlog mang giá trị thô là STRING(16)/STRING(4)/STRING(16)
// (charset binary): Field_string cắt các byte 0x00 ở cuối (giống BINARY(n)), nên phải đệm 0x00 lại trước khi
// định dạng. UUID lưu theo thứ tự byte tự nhiên (kiểm trên 10.11 và 11.4, kể cả v1/v3/v4): chỉ phép so sánh
// mới đổi thứ tự đoạn.

// mariaFixedLen: độ dài thô của kiểu (rỗng nếu không phải kiểu này).
var mariaFixedLen = map[string]int{"inet6": 16, "inet4": 4, "uuid": 16}

func padZero(raw []byte, n int) ([]byte, error) {
	if len(raw) > n {
		return nil, fmt.Errorf("value has %d bytes, want at most %d", len(raw), n)
	}
	if len(raw) == n {
		return raw, nil
	}
	out := make([]byte, n)
	copy(out, raw)
	return out, nil
}

func mariaInet4Text(b []byte) string {
	return strconv.Itoa(int(b[0])) + "." + strconv.Itoa(int(b[1])) + "." + strconv.Itoa(int(b[2])) + "." + strconv.Itoa(int(b[3]))
}

func mariaUUIDText(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hexd[c>>4], hexd[c&15])
	}
	return string(out)
}

// mariaInet6Text định dạng 16 byte như SELECT của MariaDB.
func mariaInet6Text(b []byte) string {
	var w [8]uint16
	for i := range w {
		w[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
	}
	// Dãy nhóm 0 dài nhất (hòa: dãy đầu); MariaDB rút gọn cả dãy chỉ có 1 nhóm 0.
	bestBase, bestLen := -1, 0
	for i := 0; i < 8; {
		if w[i] != 0 {
			i++
			continue
		}
		j := i
		for j < 8 && w[j] == 0 {
			j++
		}
		if j-i > bestLen {
			bestBase, bestLen = i, j-i
		}
		i = j
	}
	var sb strings.Builder
	for i := 0; i < 8; i++ {
		if bestBase >= 0 && i >= bestBase && i < bestBase+bestLen {
			if i == bestBase {
				sb.WriteByte(':')
			}
			continue
		}
		if i != 0 {
			sb.WriteByte(':')
		}
		if i == 6 && bestBase == 0 && (bestLen == 6 || bestLen == 5 && w[5] == 0xffff) {
			sb.WriteString(mariaInet4Text(b[12:]))
			return sb.String()
		}
		sb.WriteString(strconv.FormatUint(uint64(w[i]), 16))
	}
	if bestBase >= 0 && bestBase+bestLen == 8 {
		sb.WriteByte(':')
	}
	return sb.String()
}

// convertMariaDBTypes đổi (tại chỗ) giá trị thô của cột INET6/INET4/UUID thành chuỗi như SELECT trả. info: cấu
// trúc cột của source (information_schema); DataType "inet6"/"inet4"/"uuid".
func convertMariaDBTypes(tm *replication.TableMapEvent, info *TableInfo, rows [][]any) error {
	if info == nil {
		return nil
	}
	for i, c := range info.Columns {
		n, ok := mariaFixedLen[c.DataType]
		if !ok {
			continue
		}
		for _, row := range rows {
			if i >= len(row) || row[i] == nil {
				continue
			}
			var raw []byte
			switch v := row[i].(type) {
			case string:
				raw = []byte(v)
			case []byte:
				raw = v
			default:
				return fmt.Errorf("%s.%s column %s (%s): unexpected binlog value %T", tm.Schema, tm.Table, c.Name, c.DataType, v)
			}
			b, err := padZero(raw, n)
			if err != nil {
				return fmt.Errorf("%s.%s column %s (%s): %w", tm.Schema, tm.Table, c.Name, c.DataType, err)
			}
			switch c.DataType {
			case "inet6":
				row[i] = mariaInet6Text(b)
			case "inet4":
				row[i] = mariaInet4Text(b)
			default:
				row[i] = mariaUUIDText(b)
			}
		}
	}
	return nil
}

// MariaDBInet6Text: xuất cho tool/test.
func MariaDBInet6Text(b []byte) string { return mariaInet6Text(b) }

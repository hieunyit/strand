package mysql

import (
	"fmt"
	"strconv"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// MySQL < 5.6.4 (và bảng tạo trên 5.5 rồi nâng cấp lên 5.6+ mà chưa ALTER) dùng định dạng nhị phân CŨ cho
// TIME / DATETIME / TIMESTAMP (MYSQL_TYPE_TIME/DATETIME/TIMESTAMP, không có phần giây lẻ). go-mysql giải mã
// DATETIME/TIMESTAMP cũ đúng (zero date thành "0000-00-00 00:00:00", TIMESTAMP quy về UTC), nhưng TIME cũ là
// số nguyên 24 bit CÓ DẤU (sint3korr) còn go-mysql đọc như không dấu: TIME âm (vd. '-01:00:00') ra chuỗi sai
// kiểu "1676:72:16". fixLegacyTime khôi phục giá trị đúng từ chuỗi sai (phép biến đổi khả nghịch vì phút, giây < 100).
func fixLegacyTime(tm *replication.TableMapEvent, rows [][]any) {
	if tm == nil {
		return
	}
	for idx, typ := range tm.ColumnType {
		if typ != gomysql.MYSQL_TYPE_TIME {
			continue
		}
		for _, row := range rows {
			if idx >= len(row) {
				continue
			}
			if s, ok := row[idx].(string); ok {
				row[idx] = fixLegacyTimeString(s)
			}
		}
	}
}

// fixLegacyTimeString đổi "HHHH:MM:SS" do go-mysql dựng từ số không dấu về giá trị có dấu đúng.
func fixLegacyTimeString(s string) string {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return s
	}
	h, e1 := strconv.Atoi(parts[0])
	m, e2 := strconv.Atoi(parts[1])
	sec, e3 := strconv.Atoi(parts[2])
	if e1 != nil || e2 != nil || e3 != nil || h < 0 {
		return s
	}
	u := h*10000 + m*100 + sec
	if u < 1<<23 {
		return s // dương: go-mysql đã đúng
	}
	v := (1 << 24) - u // trị tuyệt đối của số âm 24 bit
	return fmt.Sprintf("-%02d:%02d:%02d", v/10000, (v%10000)/100, v%100)
}

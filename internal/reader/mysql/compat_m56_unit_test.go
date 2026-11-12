package mysql

import (
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// TIME cũ (MySQL < 5.6.4): số nguyên 24 bit có dấu; go-mysql đọc không dấu nên TIME âm bị sai.
func TestCompatM56_FixLegacyTimeString(t *testing.T) {
	cases := map[string]string{
		"01:02:03":    "01:02:03", // dương giữ nguyên
		"00:00:00":    "00:00:00",
		"838:59:59":   "838:59:59",  // max dương (8385959 < 2^23)
		"1676:70:13":  "-01:02:03",  // -10203 = 2^24-10203 = 16767013
		"839:12:57":   "-838:59:59", // chặn dưới: 2^24-8385959 = 8391257
		"abc":         "abc",
		"01:02":       "01:02",
		"1685:77:216": "1685:77:216", // không phải giá trị 24 bit: để nguyên
	}
	for in, want := range cases {
		if got := fixLegacyTimeString(in); got != want {
			t.Errorf("fixLegacyTimeString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCompatM56_NormalizeRowsLegacyTimeOnlyTouchesOldTimeColumns(t *testing.T) {
	tm := &replication.TableMapEvent{
		ColumnType: []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_TIME, gomysql.MYSQL_TYPE_TIME2, gomysql.MYSQL_TYPE_VARCHAR},
	}
	rows := [][]any{{int32(1), "1676:70:13", "-01:02:03", "1676:70:13"}}
	if err := NormalizeRows(tm, rows, nil); err != nil {
		t.Fatal(err)
	}
	if rows[0][1] != "-01:02:03" {
		t.Errorf("old TIME: got %v", rows[0][1])
	}
	if rows[0][2] != "-01:02:03" {
		t.Errorf("TIME2 must be untouched: got %v", rows[0][2])
	}
	if rows[0][3] != "1676:70:13" {
		t.Errorf("VARCHAR must be untouched: got %v", rows[0][3])
	}
}

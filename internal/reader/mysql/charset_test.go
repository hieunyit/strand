package mysql_test

import (
	"database/sql"
	"fmt"
	"testing"

	readermysql "strand/internal/reader/mysql"
)

// Việc C / charset: bảng giải mã latin1 của reader trùng với CONVERT(… USING latin1) của MySQL cho mọi byte.
func TestLatin1DecodeMatchesMySQL(t *testing.T) {
	db, _, _, cleanup := startMySQLServer(t, 7, "--innodb-use-native-aio=OFF")
	defer cleanup()
	for b := 0; b < 256; b++ {
		var s sql.NullString
		if err := db.QueryRow(fmt.Sprintf("SELECT CONVERT(CONVERT(X'%02x' USING latin1) USING utf8mb4)", b)).Scan(&s); err != nil {
			t.Fatal(err)
		}
		if got := readermysql.DecodeMySQLLatin1ForTest([]byte{byte(b)}); got != s.String {
			t.Errorf("byte %02x: reader %q, MySQL %q", b, got, s.String)
		}
	}
}

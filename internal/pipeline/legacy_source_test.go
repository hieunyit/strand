package pipeline

import (
	"strings"
	"testing"
)

// Phiên ghi target phải giữ STRICT, bỏ NO_ZERO_DATE/NO_ZERO_IN_DATE (zero date từ nguồn 5.5/5.6/5.7).
func TestCompatM56_TargetSQLMode(t *testing.T) {
	m := mysqlTargetSessionSQLMode
	if !strings.Contains(m, "STRICT_TRANS_TABLES") {
		t.Error("must stay strict")
	}
	for _, bad := range []string{"NO_ZERO_DATE", "NO_ZERO_IN_DATE", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES"} {
		if strings.Contains(m, bad) {
			t.Errorf("sql_mode must not contain %s", bad)
		}
	}
	dsn, err := targetDSN("u:p@tcp(h:3306)/", "p1", "i1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "sql_mode=") {
		t.Errorf("targetDSN lacks sql_mode: %s", dsn)
	}
}

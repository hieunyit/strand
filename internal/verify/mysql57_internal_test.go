package verify

import (
	"strings"
	"testing"

	"strand/internal/model"
)

// CAST(x AS DOUBLE) chỉ có từ MySQL 8.0.17: biểu thức canonical chạy được trên nguồn 5.7 không được dùng nó.
func TestCanonicalFloatExprHasNoCastAsDouble(t *testing.T) {
	for _, dt := range []string{"float", "double", "real"} {
		e := canonicalValueExpr(model.EngineMySQL, ColumnInfo{Name: "f", DataType: dt})
		if strings.Contains(strings.ToUpper(e), "AS DOUBLE") {
			t.Errorf("%s: %s", dt, e)
		}
		if !strings.Contains(e, "(`f` + 0e0)") {
			t.Errorf("%s: want (`f` + 0e0) in %s", dt, e)
		}
	}
}

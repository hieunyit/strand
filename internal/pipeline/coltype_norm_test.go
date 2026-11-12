package pipeline

import "testing"

// Nguồn MySQL 5.5–5.7 báo độ rộng hiển thị số nguyên, 8.0.19+ thì không: cùng kiểu phải so bằng nhau
// (e2e TestM57_E2E_AlterTableWhileStreaming dừng mãi ở schema_change_pending vì "bigint(20)" ≠ "bigint").
func TestNormalizeColumnTypeIgnoresIntDisplayWidth(t *testing.T) {
	for in, want := range map[string]string{
		"bigint(20)":               "bigint",
		"bigint(20) unsigned":      "bigint unsigned",
		"int(11)":                  "int",
		"mediumint(8) unsigned":    "mediumint unsigned",
		"smallint(6)":              "smallint",
		"tinyint(4)":               "tinyint",
		"tinyint(1)":               "tinyint(1)",
		"int(5) unsigned zerofill": "int(5) unsigned zerofill",
		"year(4)":                  "year",
		"varchar(20)":              "varchar(20)",
		"decimal(10,2)":            "decimal(10,2)",
		"bigint":                   "bigint",
	} {
		if got := normalizeColumnType(in); got != want {
			t.Errorf("%q → %q, want %q", in, got, want)
		}
	}
}

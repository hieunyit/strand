package rowkey

import (
	"fmt"
	"strings"

	"strand/internal/model"
)

// OnlineRowThreshold: bảng ước lượng nhiều hơn số hàng này thì thêm hướng dẫn cho bảng lớn.
const OnlineRowThreshold = 1_000_000

// SuggestInput là đầu vào của Suggest.
type SuggestInput struct {
	Flavor  string // "mysql", "mariadb", "postgres"
	Major   int
	Minor   int
	Patch   int
	Table   model.TableID
	EstRows int64
	// PGIndex: unique index đủ điều kiện (nguồn PG) có thể làm REPLICA IDENTITY; rỗng nếu không có.
	PGIndex string
	// Notes: lý do các unique index hiện có không dùng được (Explain).
	Notes string
	// KeylessWhy: vì sao chế độ keyless (tables.keyless=full_row) không áp được cho bảng này (nếu đã bật).
	KeylessWhy string
}

func (in SuggestInput) atLeast(maj, min, pat int) bool {
	if in.Major != maj {
		return in.Major > maj
	}
	if in.Minor != min {
		return in.Minor > min
	}
	return in.Patch >= pat
}

func qMy(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func qPG(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// SuggestMySQLStmt trả câu ALTER thêm khóa thay thế và có phải cột INVISIBLE không.
func SuggestMySQLStmt(in SuggestInput) (stmt string, invisible bool) {
	tbl := qMy(in.Table.Schema) + "." + qMy(in.Table.Table)
	switch {
	case in.Flavor == "mariadb" && in.atLeast(10, 3, 3):
		return "ALTER TABLE " + tbl + " ADD COLUMN `_strand_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, ADD PRIMARY KEY (`_strand_id`);", true
	case in.Flavor == "mysql" && in.atLeast(8, 0, 23):
		return "ALTER TABLE " + tbl + " ADD COLUMN `my_row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, ADD PRIMARY KEY (`my_row_id`);", true
	}
	return "ALTER TABLE " + tbl + " ADD COLUMN `_strand_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`_strand_id`);", false
}

// Suggest trả hướng dẫn nhiều dòng (không có dấu xuống dòng cuối) cho một bảng không có khóa dùng được.
func Suggest(in SuggestInput) string {
	var b strings.Builder
	line := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	line("table %s has no usable row key (PRIMARY KEY, or a UNIQUE index whose columns are all NOT NULL)", in.Table)
	if in.Notes != "" {
		line("  existing unique indexes that cannot be used: %s", in.Notes)
	}
	if in.KeylessWhy != "" {
		line("  keyless mode (tables.keyless: full_row) cannot be used for this table: %s", in.KeylessWhy)
	}
	if in.Flavor == "postgres" {
		suggestPG(&b, in)
	} else {
		suggestMySQL(&b, in)
	}
	return strings.TrimRight(b.String(), "\n")
}

func suggestPG(b *strings.Builder, in SuggestInput) {
	tbl := qPG(in.Table.Schema) + "." + qPG(in.Table.Table)
	line := func(format string, a ...any) { fmt.Fprintf(b, format+"\n", a...) }
	if in.PGIndex != "" {
		line("  an eligible unique index exists; make it the replica identity (metadata-only change, brief ACCESS EXCLUSIVE lock):")
		line("    ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", tbl, qPG(in.PGIndex))
		return
	}
	line("  add a surrogate key (a new column: SELECT * and INSERT without a column list change; the ALTER rewrites the table under ACCESS EXCLUSIVE):")
	line("    ALTER TABLE %s ADD COLUMN _strand_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;", tbl)
	if in.EstRows > OnlineRowThreshold {
		line("  large table (~%d rows) without a long lock: add a nullable bigint column, backfill it in batches, then", in.EstRows)
		line("    CREATE UNIQUE INDEX CONCURRENTLY ... ON %s (_strand_id); ALTER COLUMN _strand_id SET NOT NULL;", tbl)
		line("    ALTER TABLE %s REPLICA IDENTITY USING INDEX ...;  (or ADD PRIMARY KEY USING INDEX ...)", tbl)
	}
	line("  or, if a set of NOT NULL columns is unique, create a unique index on it and use REPLICA IDENTITY USING INDEX")
}

func suggestMySQL(b *strings.Builder, in SuggestInput) {
	line := func(format string, a ...any) { fmt.Fprintf(b, format+"\n", a...) }
	stmt, invisible := SuggestMySQLStmt(in)
	if invisible {
		line("  add an invisible surrogate key (SELECT * and INSERT without a column list are not affected):")
	} else {
		line("  add a surrogate key. WARNING: this server has no INVISIBLE columns (needs MariaDB 10.3.3+ or MySQL 8.0.23+), so the new column is visible:")
		line("  SELECT * and INSERT without a column list will see an extra column")
	}
	line("    %s", stmt)
	if in.Flavor == "mysql" && in.atLeast(8, 0, 30) {
		line("  (MySQL 8.0.30+: SET sql_generate_invisible_primary_key=ON makes NEW keyless tables get an invisible `my_row_id` primary key automatically; it does not change existing tables)")
	}
	line("  locking: adding an AUTO_INCREMENT column rebuilds the table and blocks writes while it runs (ALGORITHM=COPY or INPLACE with LOCK=SHARED); reads continue")
	if in.EstRows > OnlineRowThreshold {
		line("  large table (~%d rows): neither pt-online-schema-change nor gh-ost can add a surrogate key to a table that has no PRIMARY KEY or UNIQUE key at all", in.EstRows)
		line("  (both need a key shared by the old and new table to copy and track rows). Options: run the ALTER above in a maintenance window")
		line("  (time ~ table size), or run it on a replica first and fail over, or — if some NOT NULL columns are unique — add that unique index online")
		line("  (CREATE UNIQUE INDEX ... ALGORITHM=INPLACE, LOCK=NONE), which Strand uses directly as the row key")
	}
}

package promote

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// AutoIncCounter là bộ đếm AUTO_INCREMENT của một bảng nguồn MySQL (giá trị id KẾ TIẾP source sẽ cấp).
// Counter có thể cao hơn MAX(id)+1 khi các dòng cao nhất đã bị xóa: target phải bắt đầu từ ít nhất giá
// trị này, nếu không sẽ cấp lại id mà source đã giao cho bên ngoài (gửi cho khách, khóa ngoại ở hệ thống khác).
type AutoIncCounter struct {
	SrcTable string // "db.table" ở source
	DstTable string // "schema.table" ở target (sau tables.rename)
	Next     int64
}

// AutoIncrementReader là interface tùy chọn cho SourceDriver: đọc counter AUTO_INCREMENT sau fence.
type AutoIncrementReader interface {
	ReadAutoIncrements(ctx context.Context) ([]AutoIncCounter, error)
}

// AutoIncrementApplier là interface tùy chọn cho TargetDriver (MySQL): đặt AUTO_INCREMENT theo counter nguồn.
// Target PG không dùng: counter trở thành sàn của setval của sequence/identity gắn cột (promoter).
type AutoIncrementApplier interface {
	ApplyAutoIncrement(ctx context.Context, counters []AutoIncCounter) error
}

// ComputeAutoIncrement: giá trị AUTO_INCREMENT mới của target = max(counter nguồn, MAX(id)+1 ở target,
// counter hiện tại của target). Không bao giờ hạ counter của target.
func ComputeAutoIncrement(srcNext, targetMax, targetCurrent int64) int64 {
	v := srcNext
	if targetMax >= v { // targetMax+1 > v; tránh tràn int64
		v = targetMax + 1
	}
	if targetCurrent > v {
		v = targetCurrent
	}
	return v
}

// autoIncFloor trả giá trị setval tối thiểu (last_value) cho sequence gắn cột của target PG: giá trị kế tiếp
// phải ≥ counter nguồn nên last_value ≥ counter-1. table là SequenceInfo.Table ("tên" nếu schema public).
func autoIncFloor(counters []AutoIncCounter, table string) (int64, bool) {
	for _, c := range counters {
		d := strings.ToLower(c.DstTable)
		t := strings.ToLower(table)
		if d == t || d == "public."+t {
			return c.Next - 1, true
		}
	}
	return 0, false
}

// systemSchemas: schema không thuộc pipeline.
var systemSchemas = []string{"mysql", "information_schema", "performance_schema", "sys"}

// mapTable áp tables.rename ("db.table" → "schema.table"); không có thì giữ nguyên.
func mapTable(rename map[string]string, src string) string {
	if to, ok := rename[src]; ok {
		return to
	}
	return src
}

// BuildAutoIncCounters: hàm thuần dựng danh sách counter từ các dòng (db, table, counter) và rename.
func BuildAutoIncCounters(rows [][3]string, rename map[string]string) []AutoIncCounter {
	var out []AutoIncCounter
	for _, r := range rows {
		var n int64
		if _, err := fmt.Sscan(r[2], &n); err != nil || n <= 0 {
			continue
		}
		src := r[0] + "." + r[1]
		out = append(out, AutoIncCounter{SrcTable: src, DstTable: mapTable(rename, src), Next: n})
	}
	return out
}

// WithRename gán tables.rename để ánh xạ tên bảng nguồn sang target.
func (d *MySQLSourceDriver) WithRename(r map[string]string) *MySQLSourceDriver {
	d.rename = r
	return d
}

// ReadAutoIncrements đọc information_schema.TABLES.AUTO_INCREMENT của mọi bảng có counter (MySQL 5.5+).
// Gọi SAU fence: không còn ghi nên counter là cuối cùng. MySQL 8.0+ cache thống kê: đặt
// information_schema_stats_expiry = 0 cho session đọc (5.x không có biến này → bỏ qua lỗi).
func (d *MySQLSourceDriver) ReadAutoIncrements(ctx context.Context) ([]AutoIncCounter, error) {
	if !d.reachable || d.db == nil {
		return nil, fmt.Errorf("source db unreachable")
	}
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0")
	rows, err := conn.QueryContext(ctx, "SELECT TABLE_SCHEMA, TABLE_NAME, AUTO_INCREMENT FROM information_schema.TABLES "+
		"WHERE AUTO_INCREMENT IS NOT NULL AND TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')")
	if err != nil {
		return nil, fmt.Errorf("read source AUTO_INCREMENT counters: %w", err)
	}
	defer rows.Close()
	var raw [][3]string
	for rows.Next() {
		var s, t, n string
		if err := rows.Scan(&s, &t, &n); err != nil {
			return nil, err
		}
		raw = append(raw, [3]string{s, t, n})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return BuildAutoIncCounters(raw, d.rename), nil
}

// ApplyAutoIncrement (target MySQL): với mỗi bảng có mặt ở target và có cột AUTO_INCREMENT, đặt
// AUTO_INCREMENT = ComputeAutoIncrement(counter nguồn, MAX(cột)+1, counter hiện tại). Bảng nguồn không có
// ở target (không thuộc pipeline) bị bỏ qua.
func (d *MySQLTargetDriver) ApplyAutoIncrement(ctx context.Context, counters []AutoIncCounter) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0")
	for _, c := range counters {
		schema, table, ok := strings.Cut(c.DstTable, ".")
		if !ok {
			continue
		}
		var col string
		var cur sql.NullInt64
		err := conn.QueryRowContext(ctx, "SELECT c.COLUMN_NAME, t.AUTO_INCREMENT FROM information_schema.COLUMNS c "+
			"JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME "+
			"WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.EXTRA LIKE '%auto_increment%' LIMIT 1", schema, table).Scan(&col, &cur)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect target %s: %w", c.DstTable, err)
		}
		var maxID sql.NullInt64
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`.`%s`", col, schema, table)).Scan(&maxID); err != nil {
			return fmt.Errorf("read max(%s) of target %s: %w", col, c.DstTable, err)
		}
		next := ComputeAutoIncrement(c.Next, maxID.Int64, cur.Int64)
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = %d", schema, table, next)); err != nil {
			return fmt.Errorf("set AUTO_INCREMENT of target %s to %d: %w", c.DstTable, next, err)
		}
	}
	return nil
}

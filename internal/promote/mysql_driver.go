package promote

import (
	"context"
	"database/sql"
	"fmt"
)

// MySQLSourceDriver thực thi các thao tác trên MySQL source.
type MySQLSourceDriver struct {
	db        *sql.DB
	reachable bool
}

// NewMySQLSourceDriver khởi tạo MySQLSourceDriver.
func NewMySQLSourceDriver(db *sql.DB) *MySQLSourceDriver {
	return &MySQLSourceDriver{db: db, reachable: true}
}

// SetReachable cho phép giả lập ngắt kết nối source khi test.
func (d *MySQLSourceDriver) SetReachable(r bool) {
	d.reachable = r
}

// Fence chặn ghi trên MySQL source bằng super_read_only = ON.
func (d *MySQLSourceDriver) Fence(ctx context.Context) error {
	if !d.reachable || d.db == nil {
		return fmt.Errorf("source db unreachable")
	}
	_, err := d.db.ExecContext(ctx, "SET GLOBAL super_read_only = ON;")
	return err
}

// IsReachable kiểm tra kết nối tới source.
func (d *MySQLSourceDriver) IsReachable(ctx context.Context) bool {
	if !d.reachable || d.db == nil {
		return false
	}
	return d.db.PingContext(ctx) == nil
}

// ReadSequenceLastValue đọc giá trị sequence / auto_increment từ source.
func (d *MySQLSourceDriver) ReadSequenceLastValue(ctx context.Context, seqName string) (int64, error) {
	if !d.reachable || d.db == nil {
		return 0, fmt.Errorf("source db unreachable")
	}
	// seqName có định dạng "table.column" hoặc "seq_name"
	var val int64
	q := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s;", seqName, seqName)
	err := d.db.QueryRowContext(ctx, q).Scan(&val)
	return val, err
}

// MySQLTargetDriver thực thi các thao tác trên MySQL target.
type MySQLTargetDriver struct {
	db *sql.DB
}

// NewMySQLTargetDriver khởi tạo MySQLTargetDriver.
func NewMySQLTargetDriver(db *sql.DB) *MySQLTargetDriver {
	return &MySQLTargetDriver{db: db}
}

// HoldReadOnly thiết lập target ở chế độ super_read_only để ứng dụng không ghi được.
func (d *MySQLTargetDriver) HoldReadOnly(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, "SET GLOBAL super_read_only = ON;")
	return err
}

// IsReadOnly kiểm tra xem target có đang ở chế độ read-only hay không.
func (d *MySQLTargetDriver) IsReadOnly(ctx context.Context) (bool, error) {
	var val int
	err := d.db.QueryRowContext(ctx, "SELECT @@GLOBAL.super_read_only;").Scan(&val)
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

// ApplyRemaining xả hết các log còn lại vào target.
func (d *MySQLTargetDriver) ApplyRemaining(ctx context.Context) error {
	// Trong thực tế, gọi pipeline writer để flush nốt buffer
	return nil
}

// FindColumnMax tìm giá trị MAX của cột trong bảng target.
func (d *MySQLTargetDriver) FindColumnMax(ctx context.Context, table, column string) (int64, error) {
	var maxVal sql.NullInt64
	q := fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`;", column, table)
	err := d.db.QueryRowContext(ctx, q).Scan(&maxVal)
	if err != nil {
		return 0, err
	}
	if !maxVal.Valid {
		return 0, nil
	}
	return maxVal.Int64, nil
}

// Setval cập nhật AUTO_INCREMENT cho bảng.
func (d *MySQLTargetDriver) Setval(ctx context.Context, seq SequenceInfo, val int64) error {
	q := fmt.Sprintf("ALTER TABLE `%s` AUTO_INCREMENT = %d;", seq.Table, val)
	_, err := d.db.ExecContext(ctx, q)
	return err
}

// OpenWrites mở lại quyền ghi trên target bằng super_read_only = OFF.
func (d *MySQLTargetDriver) OpenWrites(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, "SET GLOBAL super_read_only = OFF;")
	return err
}

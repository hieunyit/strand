package promote

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
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

	// Kiểm tra bước 3 (apply nốt log): source đã fence, pipeline trên target.
	source          *sql.DB
	pipelineID      string
	catchUpTimeout  time.Duration
	catchUpInterval time.Duration
	logger          *slog.Logger
}

// WithCatchUpCheck cấu hình cách bước 3 xác minh target đã apply hết log của source.
func (d *MySQLTargetDriver) WithCatchUpCheck(source *sql.DB, pipelineID string, timeout time.Duration, logger *slog.Logger) *MySQLTargetDriver {
	d.source = source
	d.pipelineID = pipelineID
	d.catchUpTimeout = timeout
	d.catchUpInterval = 500 * time.Millisecond
	if logger == nil {
		logger = slog.Default()
	}
	d.logger = logger
	return d
}

// MissingFromApplied trả về các GTID đã executed trên source nhưng chưa có trong tập đã apply
// của target ("" nghĩa là target đã theo kịp).
func MissingFromApplied(sourceExecuted string, applied checkpoint.AppliedSet) (string, error) {
	src, err := checkpoint.ParseMySQLGTIDSet(sourceExecuted)
	if err != nil {
		return "", fmt.Errorf("parse source gtid_executed: %w", err)
	}
	if my, ok := applied.(*checkpoint.MySQLAppliedSet); ok {
		src.Subtract(my)
	} else if applied != nil {
		return "", fmt.Errorf("applied set is not a MySQL GTID set")
	}
	return src.String(), nil
}

// NewMySQLTargetDriver khởi tạo MySQLTargetDriver.
func NewMySQLTargetDriver(db *sql.DB) *MySQLTargetDriver {
	return &MySQLTargetDriver{db: db}
}

// HoldReadOnly (bước 2) chặn ghi của ứng dụng nhưng để Strand vẫn apply được ở bước 3:
// read_only=ON, super_read_only=OFF. User của Strand có CONNECTION_ADMIN nên ghi được;
// super_read_only=ON sẽ chặn cả Strand (design §6.5 rev 90, 03 §2; quyết định P1-17 câu 1).
func (d *MySQLTargetDriver) HoldReadOnly(ctx context.Context) error {
	if _, err := d.db.ExecContext(ctx, "SET GLOBAL super_read_only = OFF"); err != nil {
		return err
	}
	_, err := d.db.ExecContext(ctx, "SET GLOBAL read_only = ON")
	return err
}

// IsReadOnly kiểm tra xem target có đang ở chế độ read-only hay không.
func (d *MySQLTargetDriver) IsReadOnly(ctx context.Context) (bool, error) {
	var val int
	err := d.db.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only").Scan(&val)
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

// ApplyRemaining (bước 3) chờ đến khi mọi GTID đã executed trên source (đã fence) nằm trong
// tập đã apply của pipeline trên target. Không xác minh được thì trả lỗi, để promote
// không bao giờ mở ghi khi target còn thiếu dữ liệu (PR-1).
// Failover khẩn cấp (source không truy cập được): theo 05 §5, mất dữ liệu bằng lag là
// chấp nhận được, nên chỉ cảnh báo và đi tiếp.
func (d *MySQLTargetDriver) ApplyRemaining(ctx context.Context) error {
	if d.source == nil || d.pipelineID == "" || d.db == nil {
		return fmt.Errorf("apply remaining: cannot verify the target caught up (no source connection / pipeline id configured)")
	}
	if err := d.source.PingContext(ctx); err != nil {
		d.logger.Warn("source unreachable: cannot verify remaining log was applied (failover; data loss bounded by lag)", "error", err)
		return nil
	}
	var executed string
	if err := d.source.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return fmt.Errorf("apply remaining: read source gtid_executed: %w", err)
	}
	deadline := time.Now().Add(d.catchUpTimeout)
	for {
		applied, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, d.db, d.pipelineID, model.EngineMySQL)
		if err != nil {
			return fmt.Errorf("apply remaining: load target applied set: %w", err)
		}
		missing, err := MissingFromApplied(executed, applied)
		if err != nil {
			return err
		}
		if missing == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("apply remaining: target still missing %s after %s", missing, d.catchUpTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.catchUpInterval):
		}
	}
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

// OpenWrites (bước 5) mở quyền ghi cho ứng dụng: tắt cả super_read_only lẫn read_only.
func (d *MySQLTargetDriver) OpenWrites(ctx context.Context) error {
	if _, err := d.db.ExecContext(ctx, "SET GLOBAL super_read_only = OFF"); err != nil {
		return err
	}
	_, err := d.db.ExecContext(ctx, "SET GLOBAL read_only = OFF")
	return err
}

// Package validate thực hiện preflight checks cấu hình và môi trường theo spec/02-specs.md §8, §10, §12.
package validate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/config"
	"strand/internal/exitcode"
)

// Result lưu kết quả preflight validate
type Result struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// Preflight kiểm tra ngữ nghĩa cấu hình pipeline Strand.
// spec: 02 §8, §10
func Preflight(ctx context.Context, cfg *config.Config) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if cfg == nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "config is nil", nil)
	}

	// 1. Kiểm tra source kind (mysql | postgres)
	srcKind := strings.ToLower(cfg.Source.Kind)
	if srcKind != "mysql" && srcKind != "postgres" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("unsupported source kind: %q (must be mysql or postgres)", cfg.Source.Kind),
			nil,
		)
	}

	// 2. Kiểm tra target kind (mysql | postgres)
	dstKind := strings.ToLower(cfg.Target.Kind)
	if dstKind != "mysql" && dstKind != "postgres" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("unsupported target kind: %q (must be mysql or postgres)", cfg.Target.Kind),
			nil,
		)
	}

	// 3. Kiểm tra danh sách bảng
	if len(cfg.Tables.Include) == 0 {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			"no tables specified in tables.include",
			nil,
		)
	}

	// 4. Kiểm tra commit_order (strict | relaxed)
	if cfg.Target.CommitOrder != "" {
		co := strings.ToLower(cfg.Target.CommitOrder)
		if co != "strict" && co != "relaxed" {
			return exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				fmt.Sprintf("invalid target.commit_order: %q (must be strict or relaxed)", cfg.Target.CommitOrder),
				nil,
			)
		}
	}

	// 5. Kiểm tra write_mode (upsert | insert)
	if cfg.Target.WriteMode != "" {
		wm := strings.ToLower(cfg.Target.WriteMode)
		if wm != "upsert" && wm != "insert" {
			return exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				fmt.Sprintf("invalid target.write_mode: %q (must be upsert or insert)", cfg.Target.WriteMode),
				nil,
			)
		}
	}

	return nil
}

// CheckMySQLBinlogRowValueOptions kiểm tra binlog_row_value_options theo spec/02 §8:
// "binlog_row_value_options phải rỗng, vì partial JSON bị từ chối."
func CheckMySQLBinlogRowValueOptions(val string) error {
	trimmed := strings.TrimSpace(val)
	if trimmed != "" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("binlog_row_value_options must be empty, partial JSON rejected: %q", val),
			nil,
		)
	}
	return nil
}

// CheckMySQLSource kiểm cấu hình bắt buộc của source MySQL (spec 02 §8) trên DB thật:
// binlog_format=ROW, binlog_row_image=FULL, gtid_mode=ON, binlog_row_value_options rỗng,
// và mọi bảng trong tables ("schema.table", không hỗ trợ wildcard) tồn tại và có PK.
func CheckMySQLSource(ctx context.Context, db *sql.DB, tables []string) error {
	var format, image, gtidMode, valueOpts string
	err := db.QueryRowContext(ctx,
		"SELECT @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.gtid_mode, @@GLOBAL.binlog_row_value_options").
		Scan(&format, &image, &gtidMode, &valueOpts)
	if err != nil {
		return fmt.Errorf("read source binlog settings: %w", err)
	}
	reject := func(msg string) error {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, msg, nil)
	}
	if !strings.EqualFold(format, "ROW") {
		return reject(fmt.Sprintf("binlog_format must be ROW, got %q", format))
	}
	if !strings.EqualFold(image, "FULL") {
		return reject(fmt.Sprintf("binlog_row_image must be FULL, got %q", image))
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return reject(fmt.Sprintf("gtid_mode must be ON, got %q", gtidMode))
	}
	if err := CheckMySQLBinlogRowValueOptions(valueOpts); err != nil {
		return err
	}

	for _, t := range tables {
		schema, table, ok := strings.Cut(t, ".")
		if !ok || strings.ContainsAny(t, "*?") {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("table %q: expected schema.table (wildcards are not supported by validate yet)", t), nil)
		}
		var exists, hasPK int
		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			schema, table).Scan(&exists)
		if err == nil && exists > 0 {
			err = db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
				  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'PRIMARY KEY'`,
				schema, table).Scan(&hasPK)
		}
		if err != nil {
			return fmt.Errorf("inspect table %s: %w", t, err)
		}
		if exists == 0 {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("table %s does not exist on source", t), nil)
		}
		if hasPK == 0 {
			return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected,
				fmt.Sprintf("table %s has no primary key", t), nil)
		}
	}
	return nil
}

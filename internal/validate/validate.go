// Package validate thực hiện preflight checks cấu hình và môi trường theo spec/02-specs.md §8, §10, §12.
package validate

import (
	"context"
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

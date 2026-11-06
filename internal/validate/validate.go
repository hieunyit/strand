// Package validate thực hiện preflight checks cấu hình và môi trường theo spec/02-specs.md §8, §10, §12.
package validate

import (
	"context"
	"database/sql"
	"fmt"
	"path"
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
		// review G1: PRIMARY KEY có cột prefix (SUB_PART) — pipeline từ chối khi nạp schema, validate báo sớm.
		var pkPrefix int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.STATISTICS
			  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' AND SUB_PART IS NOT NULL`,
			schema, table).Scan(&pkPrefix); err != nil {
			return fmt.Errorf("inspect table %s: %w", t, err)
		}
		if pkPrefix > 0 {
			return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected,
				fmt.Sprintf("table %s: primary key with prefix part is not supported; use a full-column primary key", t), nil)
		}
	}
	return nil
}

// SessionBinlogIssue là một thread trên source còn ghi binlog với giá trị session khác yêu cầu.
type SessionBinlogIssue struct {
	ThreadID uint64
	User     string
	Host     string
	Name     string // tên thread (vd. thread/sql/one_connection, thread/sql/replica_sql)
	Variable string
	Value    string
}

// CheckMySQLSessionBinlog kiểm từng session đang mở trên source (performance_schema.variables_by_thread):
// binlog_format và binlog_row_image có phạm vi session, `SET GLOBAL` không đổi các kết nối đang mở, nên
// kết nối cũ (pool sống lâu, luồng applier của replica) vẫn có thể ghi binlog STATEMENT/MIXED hay MINIMAL.
// Còn session như vậy thì từ chối (exit 3). Không đọc được bảng (thiếu quyền SELECT trên
// performance_schema, hoặc performance_schema tắt) thì chỉ trả về cảnh báo. Cảnh báo cả khi
// binlog_row_metadata khác FULL (02 §11: nên bật).
func CheckMySQLSessionBinlog(ctx context.Context, db *sql.DB) (warnings []string, err error) {
	var meta string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_row_metadata").Scan(&meta); err == nil && !strings.EqualFold(meta, "FULL") {
		warnings = append(warnings, fmt.Sprintf("binlog_row_metadata is %q; FULL is recommended (column names and PK in TABLE_MAP): SET PERSIST binlog_row_metadata = FULL", meta))
	}
	rows, qerr := db.QueryContext(ctx, `
		SELECT v.THREAD_ID, COALESCE(t.PROCESSLIST_USER, ''), COALESCE(t.PROCESSLIST_HOST, ''), COALESCE(t.NAME, ''),
		       v.VARIABLE_NAME, v.VARIABLE_VALUE
		  FROM performance_schema.variables_by_thread v
		  LEFT JOIN performance_schema.threads t ON t.THREAD_ID = v.THREAD_ID
		 WHERE (v.VARIABLE_NAME = 'binlog_format'    AND UPPER(v.VARIABLE_VALUE) <> 'ROW')
		    OR (v.VARIABLE_NAME = 'binlog_row_image' AND UPPER(v.VARIABLE_VALUE) <> 'FULL')
		 ORDER BY v.THREAD_ID`)
	if qerr != nil {
		warnings = append(warnings, fmt.Sprintf("cannot verify per-session binlog settings (%v); grant SELECT ON performance_schema.variables_by_thread and performance_schema.threads to the strand user, or check manually", qerr))
		return warnings, nil
	}
	defer rows.Close()
	var issues []SessionBinlogIssue
	for rows.Next() {
		var is SessionBinlogIssue
		if err := rows.Scan(&is.ThreadID, &is.User, &is.Host, &is.Name, &is.Variable, &is.Value); err != nil {
			return warnings, fmt.Errorf("read performance_schema.variables_by_thread: %w", err)
		}
		issues = append(issues, is)
	}
	if err := rows.Err(); err != nil {
		return warnings, fmt.Errorf("read performance_schema.variables_by_thread: %w", err)
	}
	if len(issues) == 0 {
		return warnings, nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d open session(s) on the source still log with an old session value (SET GLOBAL does not change open connections):", len(issues))
	for i, is := range issues {
		if i == 5 {
			fmt.Fprintf(&b, " ... and %d more", len(issues)-5)
			break
		}
		fmt.Fprintf(&b, " [thread %d %s@%s %s: %s=%s]", is.ThreadID, is.User, is.Host, is.Name, is.Variable, is.Value)
	}
	b.WriteString(". Reconnect these clients (restart the application pool; on a replica run STOP REPLICA; START REPLICA) and validate again")
	return warnings, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, b.String(), nil)
}

// ExpandTables mở rộng các mẫu glob tables.include / tables.exclude (path.Match trên "schema.table",
// như pipeline) thành danh sách bảng thật trên source. Mẫu include không khớp bảng nào bị từ chối,
// để lỗi gõ tên không âm thầm thành "không đồng bộ gì".
func ExpandTables(ctx context.Context, db *sql.DB, include, exclude []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) FROM information_schema.TABLES
		 WHERE TABLE_TYPE = 'BASE TABLE'
		   AND TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')
		 ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}
	defer rows.Close()
	var all []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		all = append(all, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []string
	matched := make([]bool, len(include))
	for _, n := range all {
		in := false
		for i, p := range include {
			ok, err := path.Match(p, n)
			if err != nil {
				return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid table pattern %q: %v", p, err), err)
			}
			if ok {
				in, matched[i] = true, true
			}
		}
		if !in {
			continue
		}
		ex := false
		for _, p := range exclude {
			if ok, _ := path.Match(p, n); ok {
				ex = true
				break
			}
		}
		if !ex {
			out = append(out, n)
		}
	}
	for i, p := range include {
		if !matched[i] {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("tables.include pattern %q matches no table on source", p), nil)
		}
	}
	return out, nil
}

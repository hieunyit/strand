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
	"strand/internal/findings"
	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/rowkey"
	"strand/internal/schemagen"
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
	_, err := CheckMySQLSourceOpts(ctx, db, tables, KeylessOpts{})
	return err
}

// CheckMySQLSourceOpts như CheckMySQLSource, kèm chế độ keyless (tables.keyless=full_row): bảng không khóa vẫn
// qua khi dữ liệu đến từ seed và kiểu cột so bằng được; warnings liệt kê các bảng chạy keyless.
func CheckMySQLSourceOpts(ctx context.Context, db *sql.DB, tables []string, ko KeylessOpts) (warnings []string, err error) {
	return CheckMySQLSourceReport(ctx, db, tables, ko, nil)
}

// CheckMySQLSourceReport là CheckMySQLSourceOpts với bộ gom c. c == nil: dừng ở lỗi đầu tiên (hành vi của
// validate). c != nil (strand doctor): mỗi điều kiện bị từ chối thành một vấn đề trong c và kiểm tiếp; lỗi trả về
// chỉ còn là lỗi không đọc được source; bảng keyless thành một cảnh báo trong c thay vì warnings.
func CheckMySQLSourceReport(ctx context.Context, db *sql.DB, tables []string, ko KeylessOpts, c *findings.Collector) (warnings []string, err error) {
	// Phiên bản server quyết định câu SQL: 5.7 không có binlog_row_value_options (8.0.3+) nên coi là rỗng.
	ver, err := mysqlver.Cached(ctx, db)
	if err != nil {
		return warnings, err
	}
	var stop error
	// fail: c == nil thì giữ lỗi để trả ngay (true); c != nil thì ghi vấn đề và kiểm tiếp (false).
	fail := func(code, object, fix string, e error) bool {
		if c == nil {
			stop = e
			return true
		}
		c.Err(findings.Error, code, object, e, fix)
		return false
	}
	if err := mysqlver.CheckSupported(ver); err != nil {
		if fail("source_version_unsupported", "source", "upgrade the source to MariaDB 10.3+ or MySQL 5.5+", err) {
			return warnings, stop
		}
	}
	// Đọc từng biến, biến vắng (5.5/5.6/5.7) lấy mặc định của phiên bản đó (legacy_vars.go).
	format, image, gtidMode, valueOpts, err := sourceBinlogVars(ctx, db)
	if err != nil {
		return warnings, fmt.Errorf("read source binlog settings: %w", err)
	}
	reject := func(msg string) error {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, msg, nil)
	}
	if !strings.EqualFold(format, "ROW") {
		if fail("source_binlog_format", "source", ver.PersistAdvice("binlog_format", "ROW")+"; then reconnect clients that write", reject(fmt.Sprintf("binlog_format must be ROW, got %q", format))) {
			return warnings, stop
		}
	}
	if !strings.EqualFold(image, "FULL") {
		if fail("source_binlog_row_image", "source", ver.PersistAdvice("binlog_row_image", "FULL")+"; then reconnect clients that write", reject(fmt.Sprintf("binlog_row_image must be FULL, got %q", image))) {
			return warnings, stop
		}
	}
	// gtid_mode ON: chế độ GTID. OFF hoặc không có biến (5.5): chế độ vị trí binlog (file:pos, bắt đầu bằng
	// --start-at <file>:<pos>). *_PERMISSIVE trộn Txn có/không GTID trong cùng binlog: không chế độ nào đúng.
	if ko.RequireBinlogPosition && strings.EqualFold(gtidMode, "ON") {
		if fail("source_gtid_auto_seed", "source", "set bootstrap.auto_seed: false (use bootstrap.default: logical or a seed), or run the source with gtid_mode=OFF",
			reject("bootstrap.auto_seed currently requires binlog file position mode")) {
			return warnings, stop
		}
	}
	switch strings.ToUpper(gtidMode) {
	case "ON", "OFF", "":
	default:
		if fail("source_gtid_mode", "source", "finish the GTID transition: SET GLOBAL gtid_mode step by step to ON (or back to OFF)",
			reject(fmt.Sprintf("gtid_mode must be ON (GTID mode) or OFF (binlog position mode), got %q", gtidMode))) {
			return warnings, stop
		}
	}
	// Không còn bắt buộc GTID nên phải kiểm binlog bật riêng (trước đây gtid_mode=ON ngầm đòi log-bin).
	var logBin string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.log_bin").Scan(&logBin); err != nil {
		return warnings, fmt.Errorf("read source log_bin: %w", err)
	}
	if logBin != "1" && !strings.EqualFold(logBin, "ON") {
		if fail("source_binlog_disabled", "source", "add log-bin=mysql-bin and server-id=<unique id> to the [mysqld] section of my.cnf and restart the source",
			reject("binary logging is disabled on the source (log_bin=OFF): start mysqld with --log-bin and --server-id")) {
			return warnings, stop
		}
	}
	if err := CheckMySQLBinlogRowValueOptions(valueOpts); err != nil {
		if fail("source_binlog_row_value_options", "source", ver.PersistAdvice("binlog_row_value_options", "''"), err) {
			return warnings, stop
		}
	}
	if ver.IsMariaDB() {
		// log_bin_compress=ON ghi ROWS/QUERY dạng nén: reader chưa hỗ trợ (dừng khi gặp), báo sớm ở đây.
		var compress string
		if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.log_bin_compress").Scan(&compress); err != nil {
			return warnings, fmt.Errorf("read source log_bin_compress: %w", err)
		}
		if compress == "1" || strings.EqualFold(compress, "ON") {
			if fail("source_binlog_compressed", "source", "SET GLOBAL log_bin_compress=OFF; and set log_bin_compress=OFF in my.cnf",
				reject("log_bin_compress is ON on the MariaDB source: compressed binlog events are not supported; SET GLOBAL log_bin_compress=OFF (and in my.cnf)")) {
				return warnings, stop
			}
		}
	}

	keylessWhy := map[string]string{}
	var missing []string // bảng không có khóa dùng được: gom hết để in gợi ý cho từng bảng
	for _, t := range tables {
		schema, table, ok := strings.Cut(t, ".")
		if !ok || strings.ContainsAny(t, "*?") {
			if fail("tables_include_invalid", t, "list tables as schema.table in tables.include",
				exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
					fmt.Sprintf("table %q: expected schema.table (wildcards are not supported by validate yet)", t), nil)) {
				return warnings, stop
			}
			continue
		}
		var exists int
		var estRows int64
		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			schema, table).Scan(&exists)
		if err == nil && exists > 0 {
			_ = db.QueryRowContext(ctx, "SELECT COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
				schema, table).Scan(&estRows)
		}
		if err != nil {
			return warnings, fmt.Errorf("inspect table %s: %w", t, err)
		}
		if exists == 0 {
			if fail("source_table_missing", t, "remove it from tables.include, or grant the strand user SELECT on it",
				exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
					fmt.Sprintf("table %s does not exist on source", t), nil)) {
				return warnings, stop
			}
			continue
		}
		// Khóa hàng: PRIMARY KEY, hoặc unique index NOT NULL (STATISTICS thay TABLE_CONSTRAINTS: MariaDB 10.6 ẩn
		// TABLE_CONSTRAINTS với user chỉ có SELECT, LOCK TABLES nên bảng có PK bị báo missing_pk).
		res, err := rowkey.ResolveMySQL(ctx, db, schema, table)
		if err != nil {
			return warnings, fmt.Errorf("inspect table %s: %w", t, err)
		}
		if res.Key == nil && ko.On {
			why, terr := ko.DecideMySQL(ctx, db, schema, table)
			if terr != nil {
				return warnings, fmt.Errorf("inspect table %s: %w", t, terr)
			}
			if why == "" {
				if ko.FoundKeyless != nil {
					ko.FoundKeyless(model.TableID{Schema: schema, Table: table})
				}
				if c != nil {
					c.Warn("source_table_keyless", t, "table has no primary or unique NOT NULL key: it runs in keyless mode (rows matched on all columns, changes applied serially, verify is count+checksum only)",
						"add a key on the source if you can; otherwise add a non-unique index on selective columns on the target")
				} else {
					warnings = append(warnings, KeylessWarnings(t)...)
				}
				continue
			}
			keylessWhy[t] = why
		}
		if res.Key == nil {
			flavor := "mysql"
			if ver.IsMariaDB() {
				flavor = "mariadb"
			}
			suggestion := rowkey.Suggest(rowkey.SuggestInput{
				Flavor: flavor, Major: ver.Major, Minor: ver.Minor, Patch: ver.Patch,
				Table: model.TableID{Schema: schema, Table: table}, EstRows: estRows, Notes: rowkey.Explain(res.Rejected), KeylessWhy: keylessWhy[t],
			})
			if c != nil {
				msg := "table has no usable row key (no primary key, no unique key on NOT NULL columns)"
				if why := keylessWhy[t]; why != "" {
					msg += "; keyless mode is not possible: " + why
				}
				c.Errorf("source_table_no_key", t, msg, suggestion)
			} else {
				missing = append(missing, suggestion)
			}
			continue
		}
		// review G1: PRIMARY KEY có cột prefix (SUB_PART) — pipeline từ chối khi nạp schema, validate báo sớm.
		if res.Info.PKPrefix {
			if fail("source_table_pk_prefix", t, "replace the prefix primary key with a full-column primary key on the source",
				exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected,
					fmt.Sprintf("table %s: primary key with prefix part is not supported; use a full-column primary key", t), nil)) {
				return warnings, stop
			}
			continue
		}
		if ver.IsMariaDB() {
			if err := schemagen.CheckMariaDBTable(ctx, db, schema, table); err != nil {
				if fail("source_table_unsupported", t, "change the column on the source as the message says, or add the table to tables.exclude", err) {
					return warnings, stop
				}
			}
		}
	}
	return warnings, missingKeysError(missing)
}

// binlogRowMetadataWarning trả cảnh báo về binlog_row_metadata ("" = không cảnh báo). MySQL 5.7 không có biến
// này (8.0.1+): chỉ báo thông tin, không khuyên SET PERSIST.
func binlogRowMetadataWarning(ctx context.Context, db *sql.DB, ver mysqlver.Version) string {
	if ver.IsMariaDB() {
		return "" // cấu trúc bảng MariaDB luôn đọc từ information_schema của nguồn
	}
	if !ver.HasBinlogRowMetadata() {
		return binlogRowMetadataNote57(ver)
	}
	var meta string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_row_metadata").Scan(&meta); err == nil && !strings.EqualFold(meta, "FULL") {
		return fmt.Sprintf("binlog_row_metadata is %q; FULL is recommended (column names and PK in TABLE_MAP): %s", meta, ver.PersistAdvice("binlog_row_metadata", "FULL"))
	}
	return ""
}

// binlogRowMetadataNote57: ghi chú thông tin cho nguồn 5.7 (TABLE_MAP không có tên cột/PK).
func binlogRowMetadataNote57(ver mysqlver.Version) string {
	return fmt.Sprintf("source is MySQL %s: binlog_row_metadata is not available (TABLE_MAP has no column names or PK); column metadata is read from the source catalog", ver.Line())
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
	ver, verr := mysqlver.Cached(ctx, db)
	if verr != nil {
		return nil, verr
	}
	if w := binlogRowMetadataWarning(ctx, db, ver); w != "" {
		warnings = append(warnings, w)
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
		if h := sessionBinlogHint(qerr); h != "" {
			return append(warnings, h), nil
		}
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
	out, unmatched, err := ExpandTablesReport(ctx, db, include, exclude)
	if err != nil {
		return nil, err
	}
	if len(unmatched) > 0 {
		return nil, unmatchedPatternError(unmatched[0])
	}
	return out, nil
}

func unmatchedPatternError(p string) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
		fmt.Sprintf("tables.include pattern %q matches no table on source", p), nil)
}

// UnmatchedPatternMessage là thông điệp validate in khi mẫu include p không khớp bảng nào.
func UnmatchedPatternMessage(p string) string { return findings.Message(unmatchedPatternError(p)) }

// ExpandTablesReport như ExpandTables nhưng không từ chối mẫu include không khớp: trả chúng trong unmatched
// (theo thứ tự trong include) cùng các bảng của những mẫu còn lại.
func ExpandTablesReport(ctx context.Context, db *sql.DB, include, exclude []string) (out, unmatched []string, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) FROM information_schema.TABLES
		 WHERE TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') -- MariaDB system-versioned: chỉ chép hàng hiện hành
		   AND TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')
		 ORDER BY 1`)
	if err != nil {
		return nil, nil, fmt.Errorf("list source tables: %w", err)
	}
	defer rows.Close()
	var all []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, nil, err
		}
		all = append(all, n)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	matched := make([]bool, len(include))
	for _, n := range all {
		in := false
		for i, p := range include {
			ok, err := path.Match(p, n)
			if err != nil {
				return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid table pattern %q: %v", p, err), err)
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
			unmatched = append(unmatched, p)
		}
	}
	return out, unmatched, nil
}

// MaxParallelWarning: bootstrap.logical.max_parallel > 1 với source PostgreSQL không có tác dụng (snapshot
// logic của source PG luôn tuần tự từng bảng).
func MaxParallelWarning(cfg *config.Config) string {
	if cfg.Bootstrap.Logical.MaxParallel > 1 && !strings.EqualFold(cfg.Source.Kind, "mysql") {
		return fmt.Sprintf("bootstrap.logical.max_parallel = %d is ignored for PostgreSQL sources: tables are snapshotted one at a time",
			cfg.Bootstrap.Logical.MaxParallel)
	}
	return ""
}

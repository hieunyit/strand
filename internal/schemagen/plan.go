// Package schemagen lập kế hoạch tạo bảng còn thiếu; không thay đổi bảng đã có.
package schemagen

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

type Request struct {
	Source, Target         *sql.DB
	SourceKind, TargetKind string
	Tables                 []model.TableID
	Rename                 map[string]string
}

type Plan struct {
	Statements []string
	Warnings   []string
	Tables     []model.TableID
	postgres   bool
}

func quote(s string, pg bool) string {
	q := "`"
	if pg {
		q = `"`
	}
	return q + strings.ReplaceAll(s, q, q+q) + q
}
func name(t model.TableID, pg bool) string { return quote(t.Schema, pg) + "." + quote(t.Table, pg) }
func literal(s string) string              { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func (r Request) target(t model.TableID) model.TableID {
	if s, ok := r.Rename[t.String()]; ok {
		a, b, _ := strings.Cut(s, ".")
		return model.TableID{Schema: a, Table: b}
	}
	return t
}
func reject(format string, a ...any) error {
	return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, fmt.Sprintf(format, a...), nil)
}

// Exists phân biệt bảng thật với view; view cùng tên phải được người vận hành xử lý.
func Exists(ctx context.Context, db *sql.DB, kind string, t model.TableID) (bool, error) {
	var typ string
	q := `SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`
	if kind == "postgres" {
		q = `SELECT table_type FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2`
	}
	err := db.QueryRowContext(ctx, q, t.Schema, t.Table).Scan(&typ)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if typ != "BASE TABLE" {
		return false, reject("target %s is %s, not a base table", t, typ)
	}
	return true, nil
}

// Discover chỉ đọc bảng người dùng; view được liệt kê thành cảnh báo thay vì sao chép.
func Discover(ctx context.Context, db *sql.DB, kind string, include, exclude []string) ([]model.TableID, []string, error) {
	q := `SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema') ORDER BY TABLE_SCHEMA,TABLE_NAME`
	if kind == "postgres" {
		q = `SELECT n.nspname,c.relname,CASE WHEN c.relkind IN ('r','p') THEN 'BASE TABLE' ELSE 'VIEW' END FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m') AND NOT c.relispartition AND n.nspname NOT IN ('information_schema','pg_catalog') AND n.nspname NOT LIKE 'pg\_%' ORDER BY 1,2`
	}
	for _, p := range append(append([]string{}, include...), exclude...) {
		if _, err := path.Match(p, ""); err != nil {
			return nil, nil, err
		}
	}
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var tables []model.TableID
	var warnings []string
	for rows.Next() {
		var t model.TableID
		var typ string
		if err := rows.Scan(&t.Schema, &t.Table, &typ); err != nil {
			return nil, nil, err
		}
		matches := func(patterns []string) bool {
			for _, p := range patterns {
				if ok, _ := path.Match(p, t.String()); ok {
					return true
				}
			}
			return false
		}
		if !matches(include) || matches(exclude) {
			continue
		}
		switch typ {
		case "BASE TABLE":
		case "SYSTEM VERSIONED":
			// MariaDB WITH SYSTEM VERSIONING: chỉ chép hàng hiện hành sang bảng thường; hình dạng không chép đúng thì từ chối.
			if err := CheckMariaDBTable(ctx, db, t.Schema, t.Table); err != nil {
				return nil, nil, err
			}
			warnings = append(warnings, "system-versioned table "+t.String()+": only current rows are migrated to a plain table; row history (FOR SYSTEM_TIME queries) is not migrated")
		case "SEQUENCE":
			warnings = append(warnings, "MariaDB sequence omitted (MySQL has no sequences): "+t.String())
			continue
		default:
			warnings = append(warnings, "view omitted: "+t.String())
			continue
		}
		tables = append(tables, t)
	}
	return tables, warnings, rows.Err()
}

// Build hoàn tất mọi kiểm tra trước khi có bất kỳ DDL nào được áp vào target.
func Build(ctx context.Context, r Request) (*Plan, error) {
	if r.SourceKind != "mysql" && r.SourceKind != "postgres" {
		return nil, reject("unsupported source engine %s", r.SourceKind)
	}
	if r.TargetKind != "mysql" && r.TargetKind != "postgres" || r.SourceKind == "postgres" && r.TargetKind != "postgres" {
		return nil, reject("unsupported schema direction %s → %s", r.SourceKind, r.TargetKind)
	}
	p := &Plan{postgres: r.TargetKind == "postgres"}
	tables := append([]model.TableID{}, r.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].String() < tables[j].String() })
	targets := map[model.TableID]bool{}
	var foreign []string
	for _, t := range tables {
		dst := r.target(t)
		if dst.Schema == "" || dst.Table == "" {
			return nil, reject("invalid target name for %s", t)
		}
		if targets[dst] {
			return nil, reject("multiple source tables map to %s", dst)
		}
		targets[dst] = true
		exists, err := Exists(ctx, r.Target, r.TargetKind, dst)
		if err != nil {
			return nil, err
		}
		if exists {
			continue
		}
		var ddl, fk, w []string
		if r.SourceKind == "postgres" {
			ddl, fk, w, err = pgToPG(ctx, r, t)
		} else if p.postgres {
			ddl, fk, w, err = mysqlToPG(ctx, r, t)
		} else {
			ddl, w, err = mysqlToMySQL(ctx, r, t)
		}
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", t, err)
		}
		p.Statements = append(p.Statements, ddl...)
		foreign = append(foreign, fk...)
		p.Warnings = append(p.Warnings, w...)
		p.Tables = append(p.Tables, dst)
	}
	p.Statements = append(p.Statements, foreign...)
	return p, nil
}

// Apply không ghi checkpoint: DDL hoàn tất trước snapshot hoặc trong barrier đã drain.
// invariant: CP-1/2/3, CO-1 — không đánh dấu Txn đã apply và không cho row vượt barrier.
// PG: một transaction cho toàn kế hoạch, kể cả FK; crash không để lại nửa bảng.
// MySQL: DDL auto-commit, chạy lại bỏ qua bảng đã tạo; không ALTER bảng đã tồn tại.
func (p *Plan) Apply(ctx context.Context, db *sql.DB) (retErr error) {
	if len(p.Statements) == 0 {
		return nil
	}
	if p.postgres {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, q := range p.Statements {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("apply schema DDL: %w", err)
			}
		}
		return tx.Commit()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var previous int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.FOREIGN_KEY_CHECKS").Scan(&previous); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0"); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, fmt.Sprintf("SET SESSION FOREIGN_KEY_CHECKS=%d", previous)); err != nil {
			// Không trả kết nối có trạng thái sai về pool, kể cả khi context chính bị huỷ.
			discardErr := conn.Raw(func(any) error { return driver.ErrBadConn })
			retErr = errors.Join(retErr, fmt.Errorf("restore FOREIGN_KEY_CHECKS: %w", err))
			if discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) {
				retErr = errors.Join(retErr, discardErr)
			}
		}
	}()
	for _, q := range p.Statements {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("apply schema DDL: %w", err)
		}
	}
	return nil
}

package objects

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"

	"strand/internal/exitcode"
)

// Phase là giai đoạn áp lên target.
type Phase string

const (
	PhasePre   Phase = "pre"   // view, procedure, function, event (tạo ở trạng thái DISABLE)
	PhasePost  Phase = "post"  // trigger, bật event
	PhaseUsers Phase = "users" // user, role, quyền
)

// Action là kết quả của một đối tượng.
type Action string

const (
	ActCreated   Action = "created"
	ActIdentical Action = "identical" // đã có và giống source: bỏ qua
	ActEnabled   Action = "enabled"
	ActWould     Action = "would-create" // --dry-run
	ActFailed    Action = "failed"
	ActSkipped   Action = "skipped"
)

// Result là kết quả áp một đối tượng (hoặc một tài khoản).
type Result struct {
	Label  string
	Action Action
	Stmt   string
	Detail string
	Err    error
}

// ApplyOptions điều khiển Apply.
type ApplyOptions struct {
	Phase  Phase
	DryRun bool
	// OnResult được gọi ngay khi có kết quả của từng đối tượng.
	OnResult func(Result)
}

func newName() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "__strand_tmp_" + hex.EncodeToString(b)
}

// withConn chạy fn trên một kết nối riêng; nếu đổi session (sql_mode, time_zone) thì bỏ kết nối thay vì trả về pool.
func withConn(ctx context.Context, db *sql.DB, o Object, fn func(c *sql.Conn) error) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	dirty := false
	defer func() {
		if dirty {
			_ = c.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = c.Close()
	}()
	if o.SQLMode != "" {
		dirty = true
		if _, err := c.ExecContext(ctx, "SET SESSION sql_mode = "+quoteStr(o.SQLMode)); err != nil {
			return fmt.Errorf("set sql_mode: %w", err)
		}
	} else if o.Kind != KindView {
		dirty = true
		if _, err := c.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
			return fmt.Errorf("set sql_mode: %w", err)
		}
	}
	if o.TimeZone != "" {
		dirty = true
		if _, err := c.ExecContext(ctx, "SET SESSION time_zone = "+quoteStr(o.TimeZone)); err != nil {
			return fmt.Errorf("set time_zone %s: %w", o.TimeZone, err)
		}
	}
	return fn(c)
}

func exists(ctx context.Context, db *sql.DB, o Object) (bool, error) {
	var q string
	var args []any
	switch o.Kind {
	case KindView:
		q, args = `SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, []any{o.TargetSchema, o.TargetName}
	case KindProcedure, KindFunction:
		q, args = `SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=? AND ROUTINE_NAME=? AND ROUTINE_TYPE=?`, []any{o.TargetSchema, o.TargetName, strings.ToUpper(string(o.Kind))}
	case KindTrigger:
		q, args = `SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND TRIGGER_NAME=?`, []any{o.TargetSchema, o.TargetName}
	case KindEvent:
		q, args = `SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA=? AND EVENT_NAME=?`, []any{o.TargetSchema, o.TargetName}
	}
	var n int
	if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 && o.Kind == KindView {
		var typ string
		if err := db.QueryRowContext(ctx, `SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, o.TargetSchema, o.TargetName).Scan(&typ); err != nil {
			return false, err
		}
		if typ != "VIEW" {
			return false, fmt.Errorf("%s.%s exists on the target as a %s, not a view", o.TargetSchema, o.TargetName, typ)
		}
	}
	return n > 0, nil
}

type viewInfo struct{ def, check, security string }

func readView(ctx context.Context, db *sql.DB, schema, name string) (viewInfo, error) {
	var v viewInfo
	err := db.QueryRowContext(ctx, `SELECT VIEW_DEFINITION, CHECK_OPTION, SECURITY_TYPE FROM information_schema.VIEWS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, schema, name).Scan(&v.def, &v.check, &v.security)
	return v, err
}

// same: đối tượng đã có trên target có giống định nghĩa chuyển từ source không.
func same(ctx context.Context, db *sql.DB, o Object) (bool, error) {
	if o.Kind == KindView {
		// MySQL chuẩn hóa thân view khi lưu: tạo bản tạm cùng định nghĩa rồi so VIEW_DEFINITION.
		ts := lex(o.Stmt)
		h, ok := parseHeader(ts, KindView)
		if !ok {
			return false, fmt.Errorf("cannot parse the view statement")
		}
		tmp := newName()
		stmt := applyEdits(ts, []edit{{from: h.nameFrom, to: h.nameTo, text: qname(o.TargetSchema, tmp)}})
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return false, fmt.Errorf("compare with the existing view: %w", err)
		}
		defer db.ExecContext(context.WithoutCancel(ctx), "DROP VIEW IF EXISTS "+qname(o.TargetSchema, tmp)) //nolint:errcheck
		a, err := readView(ctx, db, o.TargetSchema, o.TargetName)
		if err != nil {
			return false, err
		}
		b, err := readView(ctx, db, o.TargetSchema, tmp)
		if err != nil {
			return false, err
		}
		return a == b, nil
	}
	kw := strings.ToUpper(string(o.Kind))
	create, _, _, err := showCreate(ctx, db, "SHOW CREATE "+kw+" "+qname(o.TargetSchema, o.TargetName))
	if err != nil {
		return false, err
	}
	want := o.Stmt
	if o.Kind == KindProcedure || o.Kind == KindFunction {
		// MySQL chuẩn hóa kiểu khi lưu (int(11) -> int...): tạo bản tạm cùng định nghĩa rồi so SHOW CREATE hai bên.
		ts := lex(o.Stmt)
		h, ok := parseHeader(ts, o.Kind)
		if !ok {
			return false, fmt.Errorf("cannot parse the %s statement", o.Kind)
		}
		tmp := newName()
		stmt := applyEdits(ts, []edit{{from: h.nameFrom, to: h.nameTo, text: qname(o.TargetSchema, tmp)}})
		if err := withConn(ctx, db, o, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, stmt)
			return err
		}); err != nil {
			return false, fmt.Errorf("compare with the existing %s: %w", o.Kind, err)
		}
		defer db.ExecContext(context.WithoutCancel(ctx), "DROP "+kw+" IF EXISTS "+qname(o.TargetSchema, tmp)) //nolint:errcheck
		tc, _, _, err := showCreate(ctx, db, "SHOW CREATE "+kw+" "+qname(o.TargetSchema, tmp))
		if err != nil {
			return false, err
		}
		want = strings.ReplaceAll(tc, quoteIdent(tmp), quoteIdent(o.TargetName))
	}
	return Canonical(create, o.Kind) == Canonical(want, o.Kind), nil
}

// create tạo một đối tượng, idempotent: đã có và giống thì bỏ qua, khác thì lỗi rõ.
func create(ctx context.Context, db *sql.DB, o Object, dry bool) Result {
	r := Result{Label: o.Label(), Stmt: o.Stmt}
	if o.Skipped != "" {
		r.Action, r.Detail = ActSkipped, o.Skipped
		return r
	}
	ex, err := exists(ctx, db, o)
	if err != nil {
		r.Action, r.Err = ActFailed, err
		return r
	}
	if ex {
		if dry {
			r.Action, r.Detail = ActSkipped, "already exists on the target (not compared in --dry-run)"
			return r
		}
		ok, err := same(ctx, db, o)
		switch {
		case err != nil:
			r.Action, r.Err = ActFailed, fmt.Errorf("exists on the target; cannot compare: %w", err)
		case ok:
			r.Action = ActIdentical
		default:
			r.Action = ActFailed
			r.Err = fmt.Errorf("exists on the target with a DIFFERENT definition: drop it (or fix it) and run again; not overwritten")
		}
		return r
	}
	if dry {
		r.Action = ActWould
		return r
	}
	if err := withConn(ctx, db, o, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, o.Stmt)
		return err
	}); err != nil {
		r.Action, r.Err = ActFailed, err
		return r
	}
	r.Action = ActCreated
	return r
}

func enableEvent(ctx context.Context, db *sql.DB, o Object, dry bool) Result {
	r := Result{Label: o.Label(), Stmt: o.EnableStmt}
	var status string
	err := db.QueryRowContext(ctx, `SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA=? AND EVENT_NAME=?`, o.TargetSchema, o.TargetName).Scan(&status)
	if err == sql.ErrNoRows {
		r.Action, r.Err = ActFailed, fmt.Errorf("event does not exist on the target: run --phase pre first")
		return r
	}
	if err != nil {
		r.Action, r.Err = ActFailed, err
		return r
	}
	if strings.EqualFold(status, "ENABLED") {
		r.Action, r.Detail = ActIdentical, "already enabled"
		return r
	}
	if dry {
		r.Action = ActWould
		return r
	}
	if _, err := db.ExecContext(ctx, o.EnableStmt); err != nil {
		r.Action, r.Err = ActFailed, err
		return r
	}
	r.Action = ActEnabled
	return r
}

func isMissingDep(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "Error 1146") || strings.Contains(err.Error(), "Error 1356") || strings.Contains(err.Error(), "Error 1109"))
}

// Apply áp set lên target theo phase. Mỗi đối tượng một câu lệnh; lỗi một đối tượng không dừng các đối tượng khác.
func Apply(ctx context.Context, db *sql.DB, set *Set, o ApplyOptions) []Result {
	var out []Result
	emit := func(r Result) {
		out = append(out, r)
		if o.OnResult != nil {
			o.OnResult(r)
		}
	}
	switch o.Phase {
	case PhasePre:
		// View: lặp lại các view lỗi vì thiếu phụ thuộc cho tới khi không còn tiến triển.
		pending := set.Of(KindView)
		for len(pending) > 0 {
			var retry []Object
			progress := false
			var retryRes []Result
			for _, v := range pending {
				r := create(ctx, db, v, o.DryRun)
				if r.Action == ActFailed && isMissingDep(r.Err) {
					retry = append(retry, v)
					retryRes = append(retryRes, r)
					continue
				}
				progress = true
				emit(r)
			}
			if !progress {
				for _, r := range retryRes {
					emit(r)
				}
				break
			}
			pending = retry
		}
		for _, ob := range set.Of(KindProcedure, KindFunction, KindPackage) {
			emit(create(ctx, db, ob, o.DryRun))
		}
		for _, ob := range set.Of(KindEvent) {
			emit(create(ctx, db, ob, o.DryRun))
		}
	case PhasePost:
		for _, ob := range set.Of(KindTrigger) {
			emit(create(ctx, db, ob, o.DryRun))
		}
		for _, ob := range set.Of(KindEvent) {
			if ob.Skipped != "" || ob.EnableStmt == "" {
				continue
			}
			emit(enableEvent(ctx, db, ob, o.DryRun))
		}
	case PhaseUsers:
		if set.Users == nil {
			return out
		}
		for _, it := range set.Users.Items {
			emit(applyUser(ctx, db, it, o.DryRun))
		}
	}
	return out
}

func applyUser(ctx context.Context, db *sql.DB, it UserItem, dry bool) Result {
	r := Result{Label: "user " + it.Account, Stmt: strings.Join(it.Stmts, ";\n")}
	if it.Role {
		r.Label = "role " + it.Account
	}
	if dry {
		r.Action = ActWould
		return r
	}
	for i, st := range it.Stmts {
		if i == 0 {
			// CREATE USER IF NOT EXISTS không báo user đã có: đếm trước để báo cáo đúng.
			var n int
			user, host := splitAccount(it.Account)
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.user WHERE User=? AND Host=?", user, host).Scan(&n); err == nil && n > 0 {
				r.Detail = "account already exists (kept as is); grants re-applied"
			}
		}
		if _, err := db.ExecContext(ctx, st); err != nil {
			r.Action, r.Err = ActFailed, fmt.Errorf("%s: %w", firstN(st, 120), err)
			return r
		}
	}
	r.Action = ActCreated
	if r.Detail != "" {
		r.Action = ActIdentical
	}
	return r
}

// splitAccount đảo `'u'@'h'` hoặc `r` (role) thành user, host.
func splitAccount(a string) (string, string) {
	ts := lex(a)
	s := sig(ts)
	val := func(t token) string {
		if t.kind == tkString {
			x := t.text[1 : len(t.text)-1]
			return strings.ReplaceAll(strings.ReplaceAll(x, "''", "'"), `\\`, `\`)
		}
		n, _ := t.name()
		return n
	}
	if len(s) >= 3 && ts[s[1]].isPunct("@") {
		return val(ts[s[0]]), val(ts[s[2]])
	}
	if len(s) >= 1 {
		return val(ts[s[0]]), "%"
	}
	return a, "%"
}

// FailureError gộp các đối tượng lỗi thành một error (nil nếu không có lỗi).
func FailureError(rs []Result) error {
	var bad []string
	for _, r := range rs {
		if r.Action == ActFailed {
			bad = append(bad, r.Label)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	return exitcode.New("objects_apply_failed", exitcode.Unclassified,
		fmt.Sprintf("%d object(s) failed: %s", len(bad), strings.Join(bad, ", ")), nil)
}

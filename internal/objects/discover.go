package objects

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"

	"strand/internal/mysqlver"
	"strand/internal/schemagen"
)

func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, s); ok {
			return true
		}
	}
	return false
}

func inList(n int) string {
	if n == 0 {
		return "('')"
	}
	return "(" + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
}

// showCreate chạy SHOW CREATE và trả cột "Create ..." (hoặc "SQL Original Statement") cùng sql_mode/time_zone.
func showCreate(ctx context.Context, db *sql.DB, q string) (create, sqlMode, tz string, err error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", "", "", err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", "", "", err
		}
		return "", "", "", fmt.Errorf("no result")
	}
	vals := make([]sql.NullString, len(cols))
	ptr := make([]any, len(cols))
	for i := range ptr {
		ptr[i] = &vals[i]
	}
	if err := rows.Scan(ptr...); err != nil {
		return "", "", "", err
	}
	for i, c := range cols {
		lc := strings.ToLower(c)
		switch {
		case strings.HasPrefix(lc, "create ") || lc == "sql original statement":
			create = vals[i].String
		case lc == "sql_mode":
			sqlMode = vals[i].String
		case lc == "time_zone":
			tz = vals[i].String
		}
	}
	if create == "" {
		return "", "", "", fmt.Errorf("definition is hidden: the account lacks the privilege to read it (SHOW VIEW / SHOW_ROUTINE / TRIGGER / EVENT)")
	}
	return create, sqlMode, tz, nil
}

func qname(s, n string) string { return quoteIdent(s) + "." + quoteIdent(n) }

// Discover đọc view/routine/trigger/event ở source và chuyển sang MySQL 8.x.
func Discover(ctx context.Context, db *sql.DB, o Options) (*Set, error) {
	ver, err := mysqlver.Cached(ctx, db)
	if err != nil {
		return nil, err
	}
	tables, _, err := schemagen.Discover(ctx, db, "mysql", o.Include, o.Exclude)
	if err != nil {
		return nil, err
	}
	tableSet := map[string]bool{}
	schemaSet := map[string]bool{}
	for _, t := range tables {
		tableSet[t.Schema+"."+t.Table] = true
		schemaSet[t.Schema] = true
	}
	schemas := sortedKeys(schemaSet)
	set := &Set{Schemas: schemas, Tables: sortedKeys(tableSet)}
	if len(schemas) == 0 {
		return set, nil
	}
	args := make([]any, len(schemas))
	for i, s := range schemas {
		args[i] = s
	}
	query := func(q string) (*sql.Rows, error) { return db.QueryContext(ctx, q, args...) }
	if !ver.IsMariaDB() && !ver.AtLeast(8, 0, 0) {
		set.Notes = append(set.Notes, fmt.Sprintf("source is MySQL %s: MySQL 8.0 added reserved words (RANK, GROUPS, ROW, LEAD, ...) and removed features (GROUP BY ASC, PROCEDURE ANALYSE); a definition may fail to compile on the target", ver.Raw))
	}

	// View
	type vref struct{ schema, name string }
	var views []vref
	rows, err := query(`SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA IN ` + inList(len(schemas)) + ` ORDER BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("list views: %w", err)
	}
	for rows.Next() {
		var v vref
		if err := rows.Scan(&v.schema, &v.name); err != nil {
			rows.Close()
			return nil, err
		}
		if matchAny(o.Exclude, v.schema+"."+v.name) {
			continue
		}
		views = append(views, v)
	}
	rows.Close()
	viewSet := map[string]bool{}
	for _, v := range views {
		viewSet[v.schema+"."+v.name] = true
	}
	var viewObjs []Object
	for _, v := range views {
		create, _, _, err := showCreate(ctx, db, "SHOW CREATE VIEW "+qname(v.schema, v.name))
		obj := Object{Kind: KindView, Schema: v.schema, Name: v.name, TargetSchema: v.schema, TargetName: v.name}
		if err != nil {
			obj.Skipped = "cannot read definition: " + err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		stmt, w, err := convertCreate(create, KindView, v.schema, v.name, o.KeepDefiner, o.Rename, v.schema)
		if err != nil {
			obj.Skipped = err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		obj.Stmt, obj.Warnings = stmt, w
		if ver.IsMariaDB() {
			obj.Warnings = append(obj.Warnings, DetectMariaDBOnly(create, KindView, "")...)
		}
		obj.Deps = viewDeps(create, v.schema, viewSet)
		viewObjs = append(viewObjs, obj)
	}
	ordered, cyc := OrderViews(viewObjs)
	for _, name := range cyc {
		set.Notes = append(set.Notes, "views reference each other in a cycle, order not guaranteed: "+name)
	}
	set.Objects = append(set.Objects, ordered...)

	// Routine
	rows, err = query(`SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA IN ` + inList(len(schemas)) + ` ORDER BY 1, 3, 2`)
	if err != nil {
		return nil, fmt.Errorf("list routines: %w", err)
	}
	type rref struct{ schema, name, typ string }
	var routines []rref
	for rows.Next() {
		var r rref
		if err := rows.Scan(&r.schema, &r.name, &r.typ); err != nil {
			rows.Close()
			return nil, err
		}
		routines = append(routines, r)
	}
	rows.Close()
	for _, r := range routines {
		var k Kind
		switch strings.ToUpper(r.typ) {
		case "PROCEDURE":
			k = KindProcedure
		case "FUNCTION":
			k = KindFunction
		default:
			set.Objects = append(set.Objects, Object{Kind: KindPackage, Schema: r.schema, Name: r.name, TargetSchema: r.schema, TargetName: r.name,
				Skipped: "MariaDB " + r.typ + " has no MySQL equivalent: rewrite it as plain procedures/functions by hand"})
			continue
		}
		obj := Object{Kind: k, Schema: r.schema, Name: r.name, TargetSchema: r.schema, TargetName: r.name}
		create, mode, _, err := showCreate(ctx, db, "SHOW CREATE "+strings.ToUpper(string(k))+" "+qname(r.schema, r.name))
		if err != nil {
			obj.Skipped = "cannot read definition: " + err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		stmt, w, err := convertCreate(create, k, r.schema, r.name, o.KeepDefiner, o.Rename, r.schema)
		if err != nil {
			obj.Skipped = err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		obj.Stmt, obj.Warnings = stmt, w
		fillMode(&obj, mode)
		if ver.IsMariaDB() {
			obj.Warnings = append(obj.Warnings, DetectMariaDBOnly(create, k, mode)...)
		}
		set.Objects = append(set.Objects, obj)
	}

	// Trigger
	rows, err = query(`SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA IN ` +
		inList(len(schemas)) + ` ORDER BY 1, 4, ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER, 2`)
	if err != nil {
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	type tref struct{ schema, name, tschema, table string }
	var trigs []tref
	for rows.Next() {
		var t tref
		if err := rows.Scan(&t.schema, &t.name, &t.tschema, &t.table); err != nil {
			rows.Close()
			return nil, err
		}
		trigs = append(trigs, t)
	}
	rows.Close()
	for _, t := range trigs {
		if !tableSet[t.tschema+"."+t.table] {
			set.Notes = append(set.Notes, fmt.Sprintf("trigger %s.%s skipped: its table %s.%s is not in the migration (tables include/exclude)", t.schema, t.name, t.tschema, t.table))
			continue
		}
		ts, tt := t.tschema, t.table
		if to, ok := o.Rename[ts+"."+tt]; ok {
			if s2, t2, ok := strings.Cut(to, "."); ok {
				ts, tt = s2, t2
			}
		}
		obj := Object{Kind: KindTrigger, Schema: t.schema, Name: t.name, Table: t.table, TargetSchema: ts, TargetName: t.name}
		create, mode, _, err := showCreate(ctx, db, "SHOW CREATE TRIGGER "+qname(t.schema, t.name))
		if err != nil {
			obj.Skipped = "cannot read definition: " + err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		stmt, w, err := convertTrigger(create, ts, t.name, tt, o.KeepDefiner, o.Rename, t.schema)
		if err != nil {
			obj.Skipped = err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		obj.Stmt, obj.Warnings = stmt, w
		fillMode(&obj, mode)
		if ver.IsMariaDB() {
			obj.Warnings = append(obj.Warnings, DetectMariaDBOnly(create, KindTrigger, mode)...)
		}
		set.Objects = append(set.Objects, obj)
	}

	// Event
	rows, err = query(`SELECT EVENT_SCHEMA, EVENT_NAME, STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA IN ` + inList(len(schemas)) + ` ORDER BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	type eref struct{ schema, name, status string }
	var events []eref
	for rows.Next() {
		var e eref
		if err := rows.Scan(&e.schema, &e.name, &e.status); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
	}
	rows.Close()
	for _, e := range events {
		obj := Object{Kind: KindEvent, Schema: e.schema, Name: e.name, TargetSchema: e.schema, TargetName: e.name}
		create, mode, tz, err := showCreate(ctx, db, "SHOW CREATE EVENT "+qname(e.schema, e.name))
		if err != nil {
			obj.Skipped = "cannot read definition: " + err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		stmt, w, err := convertCreate(create, KindEvent, e.schema, e.name, o.KeepDefiner, o.Rename, e.schema)
		if err == nil {
			stmt, err = eventDisabled(stmt)
		}
		if err != nil {
			obj.Skipped = err.Error()
			set.Objects = append(set.Objects, obj)
			continue
		}
		obj.Stmt, obj.Warnings = stmt, w
		fillMode(&obj, mode)
		if tz != "" && !strings.EqualFold(tz, "SYSTEM") {
			obj.TimeZone = tz
		}
		// ENABLED, hoặc SLAVESIDE_DISABLED (bật ở master gốc): bật khi cutover. DISABLED: giữ tắt.
		if strings.EqualFold(e.status, "ENABLED") || strings.EqualFold(e.status, "SLAVESIDE_DISABLED") {
			obj.EnableStmt = "ALTER EVENT " + qname(e.schema, e.name) + " ENABLE"
		}
		if ver.IsMariaDB() {
			obj.Warnings = append(obj.Warnings, DetectMariaDBOnly(create, KindEvent, mode)...)
		}
		set.Objects = append(set.Objects, obj)
	}
	if o.WithUsers {
		known := map[string]bool{}
		for t := range tableSet {
			known[t] = true
		}
		for _, ob := range set.Objects {
			if ob.Kind == KindView || ob.Kind == KindProcedure || ob.Kind == KindFunction {
				known[ob.Schema+"."+ob.Name] = true
			}
		}
		u, err := DiscoverUsers(ctx, db, ver, UserOptions{Schemas: schemas, Known: known, Rename: o.Rename, SelfUser: o.SelfUser, Auth: o.Auth})
		if err != nil {
			return nil, err
		}
		set.Users = u
	}
	return set, nil
}

// fillMode lọc sql_mode của source cho MySQL 8.x; mode bị bỏ thành cảnh báo.
func fillMode(o *Object, mode string) {
	kept, dropped := FilterSQLMode(mode)
	o.SQLMode = kept
	if len(dropped) > 0 {
		o.Warnings = append(o.Warnings, "sql_mode "+strings.Join(dropped, ",")+" is not available on MySQL 8.x and is dropped: behaviour may differ")
	}
}

// OrderViews xếp view theo phụ thuộc (view được tham chiếu đứng trước). Vòng phụ thuộc: trả tên view còn lại
// ở cuối theo thứ tự gốc.
func OrderViews(views []Object) (ordered []Object, cyclic []string) {
	byName := map[string]Object{}
	var names []string
	for _, v := range views {
		k := v.Schema + "." + v.Name
		byName[k] = v
		names = append(names, k)
	}
	sort.Strings(names)
	done := map[string]bool{}
	state := map[string]int{} // 1 = đang xét
	var visit func(k string)
	visit = func(k string) {
		if done[k] {
			return
		}
		if state[k] == 1 {
			cyclic = append(cyclic, k)
			return
		}
		state[k] = 1
		for _, d := range byName[k].Deps {
			if _, ok := byName[d]; ok && d != k {
				visit(d)
			}
		}
		state[k] = 0
		if !done[k] {
			done[k] = true
			ordered = append(ordered, byName[k])
		}
	}
	for _, k := range names {
		visit(k)
	}
	return ordered, cyclic
}

// Summary đếm nhanh đối tượng sẽ không được Strand chép tự động (chỉ đọc information_schema).
func Summary(ctx context.Context, db *sql.DB, o Options) (map[Kind]int, error) {
	tables, _, err := schemagen.Discover(ctx, db, "mysql", o.Include, o.Exclude)
	if err != nil {
		return nil, err
	}
	tableSet := map[string]bool{}
	schemaSet := map[string]bool{}
	for _, t := range tables {
		tableSet[t.Schema+"."+t.Table] = true
		schemaSet[t.Schema] = true
	}
	out := map[Kind]int{}
	if len(schemaSet) == 0 {
		return out, nil
	}
	var args []any
	for _, s := range sortedKeys(schemaSet) {
		args = append(args, s)
	}
	in := inList(len(args))
	count := func(k Kind, q string) error {
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a, b, c string
			if err := rows.Scan(&a, &b, &c); err != nil {
				return err
			}
			switch k {
			case KindTrigger:
				if !tableSet[b+"."+c] {
					continue
				}
			case KindView:
				if matchAny(o.Exclude, a+"."+b) {
					continue
				}
			case KindProcedure: // cột c là ROUTINE_TYPE
				if strings.EqualFold(c, "FUNCTION") {
					out[KindFunction]++
					continue
				}
			}
			out[k]++
		}
		return rows.Err()
	}
	if err := count(KindView, `SELECT TABLE_SCHEMA, TABLE_NAME, '' FROM information_schema.VIEWS WHERE TABLE_SCHEMA IN `+in); err != nil {
		return nil, err
	}
	if err := count(KindProcedure, `SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA IN `+in); err != nil {
		return nil, err
	}
	if err := count(KindTrigger, `SELECT TRIGGER_SCHEMA, EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA IN `+in); err != nil {
		return nil, err
	}
	if err := count(KindEvent, `SELECT EVENT_SCHEMA, EVENT_NAME, '' FROM information_schema.EVENTS WHERE EVENT_SCHEMA IN `+in); err != nil {
		return nil, err
	}
	return out, nil
}

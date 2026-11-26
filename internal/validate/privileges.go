package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/findings"
)

// Grants là SHOW GRANTS của user hiện tại đã phân tích (chỉ quyền cấp server, database và bảng; bỏ quyền cột,
// routine và PROXY). Dòng GRANT chứa mật khẩu băm (MariaDB IDENTIFIED BY PASSWORD) không bao giờ được giữ lại.
type Grants struct {
	// User là CURRENT_USER() dạng 'user'@'host', để in câu GRANT.
	User   string
	global map[string]bool
	dbs    []dbGrant
	tables map[string]map[string]bool // "schema.table" (chữ thường) → quyền
	// HasRoles: user được cấp role; quyền đến từ role không hiện trong SHOW GRANTS nên không kết luận chắc chắn.
	HasRoles bool
}

type dbGrant struct {
	pattern string // tên database, có thể chứa wildcard % _ (escape bằng \)
	privs   map[string]bool
}

// privAlias: tên khác của cùng một quyền (MariaDB 10.5+ đổi tên REPLICATION CLIENT/SLAVE MONITOR...).
var privAlias = map[string]string{
	"BINLOG MONITOR":      "REPLICATION CLIENT",
	"REPLICATION REPLICA": "REPLICATION SLAVE",
	"REPLICA MONITOR":     "SLAVE MONITOR",
	"ALL PRIVILEGES":      "ALL",
}

// ReadGrants đọc SHOW GRANTS và CURRENT_USER() của kết nối db (chỉ đọc).
func ReadGrants(ctx context.Context, db *sql.DB) (*Grants, error) {
	var user string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil {
		return nil, fmt.Errorf("read current user: %w", err)
	}
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, fmt.Errorf("SHOW GRANTS: %w", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	g := ParseGrants(lines)
	g.User = quoteAccount(user)
	return g, nil
}

// quoteAccount: "u@%" → 'u'@'%'.
func quoteAccount(user string) string {
	i := strings.LastIndex(user, "@")
	if i < 0 {
		return "'" + strings.ReplaceAll(user, "'", "''") + "'"
	}
	return "'" + strings.ReplaceAll(user[:i], "'", "''") + "'@'" + strings.ReplaceAll(user[i+1:], "'", "''") + "'"
}

// ParseGrants phân tích các dòng SHOW GRANTS (MySQL và MariaDB).
func ParseGrants(lines []string) *Grants {
	g := &Grants{global: map[string]bool{}, tables: map[string]map[string]bool{}}
	for _, line := range lines {
		up := strings.ToUpper(line)
		if !strings.HasPrefix(up, "GRANT ") {
			continue
		}
		on := indexOutsideParens(up, " ON ")
		to := strings.LastIndex(up, " TO ")
		if on < 0 {
			if to > 0 { // GRANT `role`@`%` TO user: cấp role
				g.HasRoles = true
			}
			continue
		}
		if to < on {
			continue
		}
		privs := parsePrivList(line[len("GRANT "):on])
		obj := strings.TrimSpace(line[on+len(" ON ") : to])
		if strings.HasPrefix(strings.ToUpper(obj), "FUNCTION ") || strings.HasPrefix(strings.ToUpper(obj), "PROCEDURE ") || strings.HasPrefix(obj, "''@") {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(obj), "TABLE ") {
			obj = strings.TrimSpace(obj[len("TABLE "):])
		}
		schema, table := splitGrantObject(obj)
		switch {
		case schema == "*":
			for p := range privs {
				g.global[p] = true
			}
		case table == "*":
			g.dbs = append(g.dbs, dbGrant{pattern: schema, privs: privs})
		default:
			key := strings.ToLower(schema + "." + table)
			if g.tables[key] == nil {
				g.tables[key] = map[string]bool{}
			}
			for p := range privs {
				g.tables[key][p] = true
			}
		}
	}
	return g
}

func indexOutsideParens(s, sub string) int {
	depth := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// parsePrivList: "SELECT, INSERT (a, b), REPLICATION SLAVE" → {SELECT, REPLICATION SLAVE}; quyền cột bị bỏ.
func parsePrivList(s string) map[string]bool {
	out := map[string]bool{}
	depth, start := 0, 0
	flush := func(part string) {
		part = strings.ToUpper(strings.Join(strings.Fields(part), " "))
		if part == "" || strings.Contains(part, "(") {
			return // quyền cột: không đủ cho cả bảng
		}
		if a, ok := privAlias[part]; ok {
			part = a
		}
		out[part] = true
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				flush(s[start:i])
				start = i + 1
			}
		}
	}
	flush(s[start:])
	return out
}

// splitGrantObject: "`app`.*" → (app, *); "*.*" → (*, *); "`a`.`t`" → (a, t).
func splitGrantObject(obj string) (schema, table string) {
	var parts []string
	for len(obj) > 0 {
		if obj[0] == '`' {
			end := 1
			var b strings.Builder
			for end < len(obj) {
				if obj[end] == '`' {
					if end+1 < len(obj) && obj[end+1] == '`' {
						b.WriteByte('`')
						end += 2
						continue
					}
					break
				}
				b.WriteByte(obj[end])
				end++
			}
			parts = append(parts, b.String())
			obj = obj[min(end+1, len(obj)):]
		} else {
			i := strings.IndexByte(obj, '.')
			if i < 0 {
				parts = append(parts, obj)
				obj = ""
				break
			}
			parts = append(parts, obj[:i])
			obj = obj[i:]
		}
		obj = strings.TrimPrefix(obj, ".")
	}
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// likeMatch: so tên database với mẫu GRANT (% và _ là wildcard, \ escape); không phân biệt hoa thường.
func likeMatch(pattern, name string) bool {
	p, n := []rune(strings.ToLower(pattern)), []rune(strings.ToLower(name))
	var match func(i, j int) bool
	match = func(i, j int) bool {
		for i < len(p) {
			switch p[i] {
			case '%':
				for k := j; k <= len(n); k++ {
					if match(i+1, k) {
						return true
					}
				}
				return false
			case '_':
				if j >= len(n) {
					return false
				}
			case '\\':
				if i+1 < len(p) {
					i++
				}
				if j >= len(n) || n[j] != p[i] {
					return false
				}
			default:
				if j >= len(n) || n[j] != p[i] {
					return false
				}
			}
			i++
			j++
		}
		return j == len(n)
	}
	return match(0, 0)
}

// HasGlobal: quyền priv cấp server (ON *.*).
func (g *Grants) HasGlobal(priv string) bool {
	return g.global["ALL"] || g.global[priv]
}

// HasSchema: quyền priv trên cả database schema (cấp server hoặc database).
func (g *Grants) HasSchema(priv, schema string) bool {
	if g.HasGlobal(priv) {
		return true
	}
	for _, d := range g.dbs {
		if (d.privs["ALL"] || d.privs[priv]) && likeMatch(d.pattern, schema) {
			return true
		}
	}
	return false
}

// HasTable: quyền priv trên bảng schema.table (cấp server, database hoặc bảng).
func (g *Grants) HasTable(priv, schema, table string) bool {
	if g.HasSchema(priv, schema) {
		return true
	}
	t := g.tables[strings.ToLower(schema+"."+table)]
	return t["ALL"] || t[priv]
}

// GrantSQL là câu GRANT privs ON on TO user.
func (g *Grants) GrantSQL(privs []string, on string) string {
	return fmt.Sprintf("GRANT %s ON %s TO %s;", strings.Join(privs, ", "), on, g.User)
}

// SchemaObject: `schema`.* cho câu GRANT.
func SchemaObject(schema string) string {
	return "`" + strings.ReplaceAll(schema, "`", "``") + "`.*"
}

// PrivilegeNeed là một quyền cần cho cấu hình hiện tại.
type PrivilegeNeed struct {
	Priv     string // tên chuẩn hóa (REPLICATION CLIENT, REPLICATION SLAVE...)
	Display  string // tên in trong câu GRANT nếu khác Priv (vd. BINLOG MONITOR); rỗng = Priv
	Schema   string // "" = cấp server
	Why      string
	Severity findings.Severity
}

// ReportPrivileges ghi vào c mỗi quyền trong needs mà g không có (gom theo schema/mức độ thành một câu GRANT).
// side là "source" hoặc "target". Khi user có role, thiếu quyền chỉ là cảnh báo (quyền của role không hiện).
// tables: bảng theo schema (tên chữ thường không đổi) — schema đủ quyền trên từng bảng cũng được tính là có.
func ReportPrivileges(g *Grants, side string, needs []PrivilegeNeed, tables map[string][]string, c *findings.Collector) {
	type key struct {
		schema string
		sev    findings.Severity
	}
	missing := map[key][]PrivilegeNeed{}
	var order []key
	for _, n := range needs {
		ok := false
		if n.Schema == "" {
			ok = g.HasGlobal(n.Priv)
		} else {
			ok = g.HasSchema(n.Priv, n.Schema)
			if !ok && len(tables[n.Schema]) > 0 {
				ok = true
				for _, t := range tables[n.Schema] {
					if !g.HasTable(n.Priv, n.Schema, t) {
						ok = false
						break
					}
				}
			}
		}
		if ok {
			continue
		}
		sev := n.Severity
		if g.HasRoles && sev == findings.Error {
			sev = findings.Warning
		}
		k := key{n.Schema, sev}
		if _, seen := missing[k]; !seen {
			order = append(order, k)
		}
		missing[k] = append(missing[k], n)
	}
	sort.SliceStable(order, func(i, j int) bool { return order[i].schema < order[j].schema })
	for _, k := range order {
		ns := missing[k]
		var privs, whys []string
		for _, n := range ns {
			name := n.Priv
			if n.Display != "" {
				name = n.Display
			}
			privs = append(privs, name)
			whys = append(whys, name+" ("+n.Why+")")
		}
		on, obj := "*.*", side
		if k.schema != "" {
			on, obj = SchemaObject(k.schema), side+":"+k.schema
		}
		msg := fmt.Sprintf("the %s user %s lacks %s", side, g.User, strings.Join(whys, ", "))
		if g.HasRoles {
			msg += "; the user has roles whose privileges SHOW GRANTS does not list, so this may be a false alarm"
		}
		c.Add(findings.Finding{Severity: k.sev, Code: side + "_privilege_missing", Object: obj, Message: msg, Fix: g.GrantSQL(privs, on)})
	}
}

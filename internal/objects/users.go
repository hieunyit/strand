package objects

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"strand/internal/mysqlver"
)

// Users là danh sách tài khoản và quyền đã chuyển cho target.
type Users struct {
	Items []UserItem `json:"items"`
	// Skipped: tài khoản không chuyển, kèm lý do.
	Skipped []string `json:"skipped,omitempty"`
}

// UserItem là một user hoặc role: Stmts chạy được; Notes là chú thích (quyền bị bỏ, plugin không hỗ trợ).
type UserItem struct {
	Account string   `json:"account"`
	Role    bool     `json:"role,omitempty"`
	Stmts   []string `json:"stmts"`
	Notes   []string `json:"notes,omitempty"`
}

// AuthMode: cách tạo mật khẩu trên target.
type AuthMode string

const (
	AuthNative AuthMode = "native" // giữ hash mysql_native_password (MySQL 8.4 cần mysql_native_password=ON)
	AuthReset  AuthMode = "reset"  // mật khẩu ngẫu nhiên mới, caching_sha2_password
)

// UserOptions điều khiển DiscoverUsers.
type UserOptions struct {
	Schemas  []string // schema đã bao gồm
	Known    map[string]bool
	Rename   map[string]string
	SelfUser string // user của Strand (bỏ qua)
	Auth     AuthMode
}

var systemUsers = map[string]bool{
	"root": true, "mariadb.sys": true, "mysql.sys": true, "mysql.session": true, "mysql.infoschema": true,
	"debian-sys-maint": true, "healthcheck": true, "public": true,
}

// privMap: tên quyền MariaDB → MySQL 8.x ("" = không có tương đương, ghi chú).
var privMap = map[string]string{
	"BINLOG MONITOR": "REPLICATION CLIENT", "SLAVE MONITOR": "REPLICATION CLIENT", "REPLICA MONITOR": "REPLICATION CLIENT",
	"BINLOG ADMIN": "BINLOG_ADMIN", "BINLOG REPLAY": "BINLOG_ADMIN", "CONNECTION ADMIN": "CONNECTION_ADMIN",
	"REPLICATION MASTER ADMIN": "REPLICATION_SLAVE_ADMIN", "REPLICATION SLAVE ADMIN": "REPLICATION_SLAVE_ADMIN",
	"SET USER": "SET_USER_ID", "SHOW CREATE ROUTINE": "SHOW_ROUTINE",
}

// privNoEquivalent: quyền MariaDB không có tương đương hẹp ở MySQL.
var privNoEquivalent = map[string]string{
	"READ_ONLY ADMIN": "no narrow MySQL equivalent (SYSTEM_VARIABLES_ADMIN is much broader)",
	"READ ONLY ADMIN": "no narrow MySQL equivalent (SYSTEM_VARIABLES_ADMIN is much broader)",
	"DELETE HISTORY":  "MySQL has no system-versioned tables",
	"FEDERATED ADMIN": "MySQL has no FederatedX",
}

var commonPrivs = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "CREATE": true, "DROP": true, "REFERENCES": true,
	"INDEX": true, "ALTER": true, "CREATE TEMPORARY TABLES": true, "LOCK TABLES": true, "EXECUTE": true,
	"CREATE VIEW": true, "SHOW VIEW": true, "CREATE ROUTINE": true, "ALTER ROUTINE": true, "EVENT": true,
	"TRIGGER": true, "GRANT OPTION": true, "ALL PRIVILEGES": true, "ALL": true, "USAGE": true,
	"REPLICATION CLIENT": true, "REPLICATION SLAVE": true, "SHOW DATABASES": true, "PROCESS": true, "FILE": true,
	"RELOAD": true, "SHUTDOWN": true, "SUPER": true, "CREATE USER": true,
}

// MapPrivilege đổi tên quyền MariaDB/MySQL cũ sang MySQL 8.x. ok=false: bỏ (note nêu lý do).
func MapPrivilege(name string) (mapped, note string, ok bool) {
	n := strings.ToUpper(strings.Join(strings.Fields(name), " "))
	if m, found := privMap[n]; found {
		return m, "", true
	}
	if why, found := privNoEquivalent[n]; found {
		return "", fmt.Sprintf("privilege %s dropped: %s", n, why), false
	}
	if commonPrivs[n] {
		return n, "", true
	}
	return "", fmt.Sprintf("privilege %s dropped: unknown on MySQL 8.x", n), false
}

type privItem struct{ name, cols string }

type grantLine struct {
	privs      []privItem
	objType    string // "", TABLE, FUNCTION, PROCEDURE
	schema     string // đã bỏ quote, giữ escape \_ \%
	object     string // "*" hoặc tên
	global     bool   // ON *.*
	withGrant  bool
	roleGrant  bool
	roles      []string
	adminOpt   bool
	rawGrantee string
}

// splitTop tách theo dấu phẩy ngoài ngoặc, trả các đoạn token (sig).
func splitTop(ts []token, idx []int) [][]int {
	var out [][]int
	var cur []int
	depth := 0
	for _, i := range idx {
		t := ts[i]
		if t.isPunct("(") {
			depth++
		} else if t.isPunct(")") {
			depth--
		}
		if depth == 0 && t.isPunct(",") {
			out = append(out, cur)
			cur = nil
			continue
		}
		cur = append(cur, i)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

func parseGrant(line string) (grantLine, bool) {
	ts := lex(line)
	s := sig(ts)
	var g grantLine
	if len(s) < 3 || !ts[s[0]].isWord("GRANT") {
		return g, false
	}
	on, to := -1, -1
	depth := 0
	for n := 1; n < len(s); n++ {
		t := ts[s[n]]
		if t.isPunct("(") {
			depth++
		} else if t.isPunct(")") {
			depth--
		}
		if depth != 0 {
			continue
		}
		if on < 0 && t.isWord("ON") {
			on = n
		}
		if to < 0 && t.isWord("TO") {
			to = n
			break
		}
	}
	if to < 0 {
		return g, false
	}
	if on < 0 || on > to { // GRANT role[, role] TO grantee [WITH ADMIN OPTION]
		g.roleGrant = true
		for _, seg := range splitTop(ts, s[1:to]) {
			var sb strings.Builder
			for _, i := range seg {
				if nm, ok := ts[i].name(); ok {
					sb.WriteString(nm)
				}
			}
			g.roles = append(g.roles, sb.String())
		}
		for n := to; n+1 < len(s); n++ {
			if ts[s[n]].isWord("ADMIN") && ts[s[n+1]].isWord("OPTION") {
				g.adminOpt = true
			}
		}
		return g, true
	}
	for _, seg := range splitTop(ts, s[1:on]) {
		var name []string
		var cols string
		for k := 0; k < len(seg); k++ {
			t := ts[seg[k]]
			if t.isPunct("(") {
				cols = strings.TrimSpace(line[t.start:ts[seg[len(seg)-1]].end])
				break
			}
			name = append(name, t.text)
		}
		g.privs = append(g.privs, privItem{name: strings.Join(name, " "), cols: cols})
	}
	m := on + 1
	if m < to && (ts[s[m]].isWord("TABLE") || ts[s[m]].isWord("FUNCTION") || ts[s[m]].isWord("PROCEDURE")) {
		g.objType = strings.ToUpper(ts[s[m]].text)
		m++
	}
	obj := s[m:to]
	// schema.object | *.* | *
	part := func(i int) string {
		if ts[i].isPunct("*") {
			return "*"
		}
		nm, _ := ts[i].name()
		return nm
	}
	switch {
	case len(obj) == 3 && ts[obj[1]].isPunct("."):
		g.schema, g.object = part(obj[0]), part(obj[2])
	case len(obj) == 1:
		g.object = part(obj[0])
	default:
		return g, false
	}
	g.global = g.schema == "*" || g.schema == ""
	for n := to; n+1 < len(s); n++ {
		if ts[s[n]].isWord("GRANT") && ts[s[n+1]].isWord("OPTION") {
			g.withGrant = true
		}
	}
	return g, true
}

// schemaPatternRe biến tên schema trong GRANT (\_ \% là literal; % _ là wildcard) thành regexp.
func schemaPatternRe(p string) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("^")
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c == '\\' && i+1 < len(p):
			i++
			sb.WriteString(regexp.QuoteMeta(string(p[i])))
		case c == '%':
			sb.WriteString(".*")
		case c == '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	return regexp.MustCompile(sb.String())
}

func unescapeLiteral(p string) string {
	return strings.NewReplacer(`\_`, "_", `\%`, "%").Replace(p)
}

func acct(user, host string) string {
	return quoteStr(user) + "@" + quoteStr(host)
}

type rawAccount struct {
	user, host, plugin, authStr, password, defaultRole string
	isRole                                             bool
}

func listAccounts(ctx context.Context, db *sql.DB, ver mysqlver.Version) ([]rawAccount, error) {
	var queries []string
	if ver.IsMariaDB() {
		queries = []string{`SELECT User, Host, IFNULL(plugin,''), IFNULL(authentication_string,''), IFNULL(Password,''), IFNULL(is_role,'N'), IFNULL(default_role,'') FROM mysql.user`}
	} else {
		queries = []string{
			`SELECT User, Host, IFNULL(plugin,''), IFNULL(authentication_string,''), IFNULL(Password,''), 'N', '' FROM mysql.user`, // 5.5/5.6/5.7 trước 5.7.6
			`SELECT User, Host, IFNULL(plugin,''), IFNULL(authentication_string,''), '', 'N', '' FROM mysql.user`,                  // 5.7.6+ và 8.x
		}
	}
	var lastErr error
	for _, q := range queries {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			lastErr = err
			continue
		}
		var out []rawAccount
		for rows.Next() {
			var a rawAccount
			var isRole string
			if err := rows.Scan(&a.user, &a.host, &a.plugin, &a.authStr, &a.password, &isRole, &a.defaultRole); err != nil {
				rows.Close()
				return nil, err
			}
			a.isRole = strings.EqualFold(isRole, "Y")
			out = append(out, a)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	return nil, fmt.Errorf("read mysql.user (needs SELECT on mysql.user): %w", lastErr)
}

func showGrants(ctx context.Context, db *sql.DB, a rawAccount) ([]string, error) {
	q := "SHOW GRANTS FOR " + acct(a.user, a.host)
	if a.isRole {
		q = "SHOW GRANTS FOR " + quoteIdent(a.user)
	}
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

var nativeHashRe = regexp.MustCompile(`^\*[0-9A-Fa-f]{40}$`)

func randomPassword() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// DiscoverUsers đọc user/role và quyền trên các schema đã bao gồm.
func DiscoverUsers(ctx context.Context, db *sql.DB, ver mysqlver.Version, o UserOptions) (*Users, error) {
	accounts, err := listAccounts(ctx, db, ver)
	if err != nil {
		return nil, err
	}
	matchSchema := func(p string) bool {
		re := schemaPatternRe(p)
		for _, s := range o.Schemas {
			if re.MatchString(s) {
				return true
			}
		}
		return false
	}
	res := &Users{}
	// Role trước (user có thể tham chiếu role).
	sort.SliceStable(accounts, func(i, j int) bool { return accounts[i].isRole && !accounts[j].isRole })
	roleKept := map[string]bool{}
	for _, a := range accounts {
		label := acct(a.user, a.host)
		if a.isRole {
			label = quoteIdent(a.user)
		}
		skip := func(why string) { res.Skipped = append(res.Skipped, label+": "+why) }
		if a.user == "" {
			skip("anonymous account")
			continue
		}
		if systemUsers[strings.ToLower(a.user)] {
			if strings.ToLower(a.user) != "root" && strings.ToLower(a.user) != "public" {
				skip("system account")
			}
			continue
		}
		if a.user == o.SelfUser {
			skip("Strand account")
			continue
		}
		lines, err := showGrants(ctx, db, a)
		if err != nil {
			return nil, err
		}
		item := UserItem{Account: label, Role: a.isRole}
		relevant := false
		repl := false
		var stmts []string
		granteeSQL := acct(a.user, a.host)
		if a.isRole {
			granteeSQL = quoteIdent(a.user)
		}
		var roleStmts []string
		for _, line := range lines {
			if ts := lex(line); len(sig(ts)) >= 4 && ts[sig(ts)[0]].isWord("SET") && ts[sig(ts)[1]].isWord("DEFAULT") && ts[sig(ts)[2]].isWord("ROLE") {
				if nm, ok := ts[sig(ts)[3]].name(); ok { // MariaDB: SET DEFAULT ROLE `r` FOR `u`@`h`
					a.defaultRole = nm
				}
				continue
			}
			g, ok := parseGrant(line)
			if !ok {
				item.Notes = append(item.Notes, "grant not understood, skipped: "+line)
				continue
			}
			if g.roleGrant {
				for _, r := range g.roles {
					if !roleKept[r] {
						item.Notes = append(item.Notes, fmt.Sprintf("role %s not migrated (no privileges on included schemas), grant skipped", r))
						continue
					}
					st := "GRANT " + quoteIdent(r) + " TO " + granteeSQL
					if g.adminOpt {
						st += " WITH ADMIN OPTION"
					}
					roleStmts = append(roleStmts, st)
					relevant = true
				}
				continue
			}
			if g.global {
				var gl []string
				for _, p := range g.privs {
					pn := strings.ToUpper(strings.Join(strings.Fields(p.name), " "))
					if pn == "USAGE" {
						continue
					}
					if pn == "REPLICATION SLAVE" {
						repl = true
					}
					gl = append(gl, p.name)
				}
				if len(gl) > 0 {
					item.Notes = append(item.Notes, "global privileges NOT copied (review by hand): "+strings.Join(gl, ", "))
				}
				continue
			}
			if !matchSchema(g.schema) {
				continue
			}
			lit := unescapeLiteral(g.schema)
			obj := g.object
			target := quoteIdent(g.schema)
			if g.schema != lit && !strings.Contains(g.schema, "%") {
				target = quoteIdent(g.schema) // giữ \_ như nguồn
			}
			objSQL := target + ".*"
			if g.object != "*" {
				sc, nm := lit, obj
				if g.objType == "" || g.objType == "TABLE" {
					if to, ok := o.Rename[sc+"."+nm]; ok {
						if s2, t2, ok := strings.Cut(to, "."); ok {
							sc, nm = s2, t2
						}
					}
				}
				if !o.Known[lit+"."+g.object] && !o.Known[sc+"."+nm] {
					item.Notes = append(item.Notes, fmt.Sprintf("grant on %s.%s skipped: object is not part of the migration", lit, g.object))
					continue
				}
				objSQL = quoteIdent(sc) + "." + quoteIdent(nm)
			}
			if g.objType == "FUNCTION" || g.objType == "PROCEDURE" {
				objSQL = g.objType + " " + objSQL
			}
			var privs []string
			for _, p := range g.privs {
				if strings.EqualFold(p.name, "USAGE") {
					continue
				}
				m, note, ok := MapPrivilege(p.name)
				if !ok {
					item.Notes = append(item.Notes, note+" (on "+lit+"."+g.object+")")
					continue
				}
				if p.cols != "" {
					m += " " + p.cols
				}
				privs = append(privs, m)
			}
			relevant = true
			if len(privs) == 0 {
				continue
			}
			st := "GRANT " + strings.Join(privs, ", ") + " ON " + objSQL + " TO " + granteeSQL
			if g.withGrant {
				st += " WITH GRANT OPTION"
			}
			stmts = append(stmts, st)
		}
		if repl {
			skip("replication account")
			continue
		}
		if !relevant {
			if len(item.Notes) > 0 {
				skip("only global privileges or none on included schemas (" + strings.Join(item.Notes, "; ") + ")")
			} else {
				skip("no privileges on included schemas")
			}
			continue
		}
		// Phần tạo tài khoản.
		var create string
		if a.isRole {
			create = "CREATE ROLE IF NOT EXISTS " + quoteIdent(a.user)
			roleKept[a.user] = true
		} else {
			create = "CREATE USER IF NOT EXISTS " + granteeSQL
			hash := a.authStr
			if !strings.HasPrefix(hash, "*") && nativeHashRe.MatchString(a.password) {
				hash = a.password
			}
			plugin := strings.ToLower(a.plugin)
			switch {
			case o.Auth == AuthReset:
				pw := randomPassword()
				create += " IDENTIFIED WITH caching_sha2_password BY " + quoteStr(pw)
				item.Notes = append(item.Notes, "password reset to a random value: "+pw)
			case (plugin == "" || plugin == "mysql_native_password") && nativeHashRe.MatchString(hash):
				create += " IDENTIFIED WITH mysql_native_password AS " + quoteStr(hash)
			case (plugin == "" || plugin == "mysql_native_password") && hash == "":
				item.Notes = append(item.Notes, "account has an empty password on the source")
			default:
				item.Notes = append(item.Notes, fmt.Sprintf("authentication plugin %q cannot be copied: the account is created WITHOUT a usable password (account locked); set one with ALTER USER", a.plugin))
				create += " ACCOUNT LOCK"
			}
		}
		item.Stmts = append(item.Stmts, create)
		item.Stmts = append(item.Stmts, stmts...)
		item.Stmts = append(item.Stmts, roleStmts...)
		if a.defaultRole != "" && roleKept[a.defaultRole] && !a.isRole {
			item.Stmts = append(item.Stmts, "SET DEFAULT ROLE "+quoteIdent(a.defaultRole)+" TO "+granteeSQL)
		}
		res.Items = append(res.Items, item)
	}
	return res, nil
}

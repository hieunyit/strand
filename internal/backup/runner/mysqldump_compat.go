package runner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// MySQLDump80Tool is the MySQL 8.0 mysqldump used for MySQL 8.0/8.1 servers. Every Oracle mysqldump from
// 8.2 on (8.4 LTS, 9.x) sends SHOW BINARY LOG STATUS for --source-data/--master-data to any 8.x server,
// and that statement only exists from MySQL 8.2: the dump of an 8.0 server fails with error 1064. The
// 8.0 client cannot be used the other way round (it sends SHOW MASTER STATUS, removed in 8.4), so the Web
// image ships both and Strand picks one by server version. Resolved like every tool (ResolveTool).
const MySQLDump80Tool = "mysqldump-8.0"

// needsMySQLDump80 reports whether a dump of a server reporting serverVersion (SELECT VERSION()) with
// the given mysqldump must use MySQLDump80Tool instead.
func needsMySQLDump80(serverVersion string, tool MySQLDumpFlavor) bool {
	if tool.MariaDB || strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		return false
	}
	m := verRE.FindStringSubmatch(serverVersion)
	if m == nil {
		return false
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	if major != 8 || minor >= 2 {
		return false
	}
	return tool.Major > 8 || (tool.Major == 8 && tool.Minor >= 2)
}

// mysqlServerVersion returns SELECT VERSION() of c ("" when the server cannot be asked: the dump then
// runs with the default tool and reports its own connection error).
func (e *Env) mysqlServerVersion(ctx context.Context, c backup.ConnSpec) string {
	db, err := openBackupMySQL(e.Getenv, c)
	if err != nil {
		return ""
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var v string
	if db.QueryRowContext(qctx, "SELECT VERSION()").Scan(&v) != nil {
		return ""
	}
	return v
}

// mysqlDumpTool picks the mysqldump for job's server: method's tool, or MySQLDump80Tool for a MySQL
// 8.0/8.1 server when the default tool is 8.2 or newer. It returns the tool name, its --version output
// and flavor.
func (e *Env) mysqlDumpTool(ctx context.Context, job backup.Job) (string, string, MySQLDumpFlavor, error) {
	tool := job.Method
	ver, _ := RunCapture(ctx, e.Getenv, tool, "--version")
	flavor := ParseDumpVersion(ver)
	if job.Method == backup.MethodMariaDBDump {
		flavor.MariaDB = true
		return tool, ver, flavor, nil
	}
	if flavor.Major < 8 || (flavor.Major == 8 && flavor.Minor < 2) {
		return tool, ver, flavor, nil
	}
	server := e.mysqlServerVersion(ctx, job.Conn)
	if !needsMySQLDump80(server, flavor) {
		return tool, ver, flavor, nil
	}
	if _, err := ResolveTool(e.Getenv, MySQLDump80Tool); err != nil {
		return "", "", flavor, fmt.Errorf("MySQL %s server: mysqldump %d.%d cannot record the binlog position "+
			"(it sends SHOW BINARY LOG STATUS, MySQL 8.2+); provide MySQL 8.0's mysqldump as %s: %w",
			server, flavor.Major, flavor.Minor, MySQLDump80Tool, err)
	}
	ver, _ = RunCapture(ctx, e.Getenv, MySQLDump80Tool, "--version")
	e.logf("mysqldump: MySQL %s server, using %s (%s)", server, MySQLDump80Tool, strings.TrimSpace(ver))
	return MySQLDump80Tool, ver, ParseDumpVersion(ver), nil
}

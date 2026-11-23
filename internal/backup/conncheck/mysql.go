package conncheck

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"strand/internal/backup"
)

func joinHostPort(h string, p int) string { return net.JoinHostPort(h, strconv.Itoa(p)) }

// mysqlRequired are the privileges a logical dump plus binlog archiving needs.
var mysqlRequired = []string{"SELECT", "RELOAD", "LOCK TABLES", "REPLICATION CLIENT", "SHOW VIEW", "EVENT", "TRIGGER"}

// globalOnly privileges only count when granted on *.*.
var mysqlGlobalOnly = map[string]bool{"RELOAD": true, "REPLICATION CLIENT": true}

var tlsSeq atomic.Int64

func mysqlTLS(t Target) (name string, cleanup func(), err error) {
	cfg, err := t.tlsSpec().TLSConfig()
	if err != nil {
		return "", nil, err
	}
	if cfg == nil {
		return "false", func() {}, nil
	}
	name = fmt.Sprintf("strand-conncheck-%d", tlsSeq.Add(1))
	if err := mysql.RegisterTLSConfig(name, cfg); err != nil {
		return "", nil, err
	}
	return name, func() { mysql.DeregisterTLSConfig(name) }, nil
}

var (
	grantRE  = regexp.MustCompile("^GRANT (.+?) ON (.+?) TO ")
	parenRE  = regexp.MustCompile(`\([^)]*\)`)
	systemDB = map[string]bool{"information_schema": true, "performance_schema": true, "mysql": true, "sys": true}
)

// parseGrants returns the privileges covered by SHOW GRANTS output: global (on *.*) and in any scope.
func parseGrants(lines []string) (global, any map[string]bool) {
	global, any = map[string]bool{}, map[string]bool{}
	for _, l := range lines {
		m := grantRE.FindStringSubmatch(l)
		if m == nil {
			continue // role grants ("GRANT role TO user") have no ON clause
		}
		scope := strings.ReplaceAll(strings.ReplaceAll(m[2], "`", ""), "'", "")
		isGlobal := scope == "*.*"
		for _, p := range strings.Split(parenRE.ReplaceAllString(m[1], ""), ",") {
			p = strings.ToUpper(strings.TrimSpace(p))
			if p == "" {
				continue
			}
			any[p] = true
			if isGlobal {
				global[p] = true
			}
		}
	}
	return global, any
}

// missingMySQL lists required privileges absent from the grants. ALL PRIVILEGES on *.* covers everything;
// MariaDB 10.5+ shows REPLICATION CLIENT as BINLOG MONITOR.
func missingMySQL(global, any map[string]bool) (granted, missing []string) {
	all := global["ALL PRIVILEGES"] || global["ALL"]
	for _, need := range mysqlRequired {
		has := all
		if !has {
			if mysqlGlobalOnly[need] {
				has = global[need]
				if need == "REPLICATION CLIENT" {
					has = has || global["BINLOG MONITOR"] || global["REPLICA MONITOR"] || global["SLAVE MONITOR"]
				}
			} else {
				has = any[need]
			}
		}
		if has {
			granted = append(granted, need)
		} else {
			missing = append(missing, need)
		}
	}
	return granted, missing
}

func checkMySQL(ctx context.Context, t Target, res *Result) {
	tlsName, cleanup, err := mysqlTLS(t)
	if err != nil {
		res.fail("tls", err)
		return
	}
	defer cleanup()
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = t.User, t.Password, "tcp", hostPort(t)
	cfg.TLSConfig = tlsName
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 8*time.Second, 10*time.Second, 10*time.Second
	cfg.AllowNativePasswords = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		res.fail("connect", err)
		return
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	start := time.Now()
	if err := db.PingContext(ctx); err != nil {
		res.fail("connect", err)
		return
	}
	res.LatencyMS = time.Since(start).Milliseconds()
	res.OK = true
	res.add("connect", "ok", fmt.Sprintf("%d ms", res.LatencyMS))

	var version string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		res.add("version", "warn", err.Error())
	} else {
		res.Version = version
		isMaria := strings.Contains(strings.ToLower(version), "mariadb")
		switch {
		case isMaria && t.Engine == backup.ConnMySQL:
			res.add("version", "warn", version+": server is MariaDB but the connection engine is mysql")
		case !isMaria && t.Engine == backup.ConnMariaDB:
			res.add("version", "warn", version+": server is MySQL but the connection engine is mariadb")
		default:
			res.add("version", "ok", version)
		}
	}

	var lines []string
	rows, err := db.QueryContext(ctx, `SHOW GRANTS FOR CURRENT_USER()`)
	if err != nil {
		res.add("privileges", "warn", "SHOW GRANTS failed: "+err.Error())
	} else {
		for rows.Next() {
			var l string
			if rows.Scan(&l) == nil {
				lines = append(lines, l)
			}
		}
		rows.Close()
		g, a := parseGrants(lines)
		res.GrantedPrivileges, res.MissingPrivileges = missingMySQL(g, a)
		if res.MissingPrivileges == nil {
			res.MissingPrivileges = []string{}
		}
		if len(res.MissingPrivileges) == 0 {
			res.add("privileges", "ok", "all required privileges granted")
		} else {
			res.add("privileges", "warn", "missing: "+strings.Join(res.MissingPrivileges, ", "))
		}
	}

	var caps backup.PITRCaps
	var logBin int
	if err := db.QueryRowContext(ctx, `SELECT @@log_bin`).Scan(&logBin); err == nil {
		caps.BinlogOn = logBin == 1
		res.BinlogEnabled = boolp(caps.BinlogOn)
		st := "ok"
		if !caps.BinlogOn {
			st = "warn"
		}
		res.add("log_bin", st, fmt.Sprintf("log_bin=%d", logBin))
	}
	var gtid string
	if t.Engine == backup.ConnMariaDB {
		res.GTIDMode = "MariaDB GTID"
		res.add("gtid_mode", "warn", "MariaDB GTID is not supported for binlog archiving")
	} else if err := db.QueryRowContext(ctx, `SELECT @@gtid_mode`).Scan(&gtid); err != nil {
		res.add("gtid_mode", "warn", err.Error())
	} else {
		res.GTIDMode = gtid
		caps.GTIDOn = strings.EqualFold(gtid, "ON")
		st := "ok"
		if !caps.GTIDOn {
			st = "warn"
		}
		res.add("gtid_mode", st, "gtid_mode="+gtid)
	}

	drows, err := db.QueryContext(ctx, `SHOW DATABASES`)
	if err != nil {
		res.add("databases", "warn", err.Error())
	} else {
		for drows.Next() {
			var d string
			if drows.Scan(&d) == nil && !systemDB[strings.ToLower(d)] {
				res.Databases = append(res.Databases, d)
			}
		}
		drows.Close()
		sort.Strings(res.Databases)
		res.add("databases", "ok", fmt.Sprintf("%d user database(s)", len(res.Databases)))
	}
	res.assessPITR(caps)
}

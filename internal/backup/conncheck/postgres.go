package conncheck

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"strand/internal/backup"
)

func checkPostgres(ctx context.Context, t Target, res *Result) {
	port := t.Port
	if port == 0 {
		port = 5432
	}
	q := url.Values{"connect_timeout": {"8"}}
	tlsConfig, err := t.tlsSpec().TLSConfig()
	if err != nil {
		res.fail("tls", err)
		return
	}
	mode := "prefer"
	if tlsConfig != nil {
		mode = "require"
	} else if t.TLSMode == "disable" {
		mode = "disable"
	}
	q.Set("sslmode", mode)
	u := url.URL{Scheme: "postgres", User: url.UserPassword(t.User, t.Password), Host: hostPort(Target{Host: t.Host, Port: port, Engine: t.Engine}),
		Path: "/postgres", RawQuery: q.Encode()}
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		res.fail("connect", fmt.Errorf("invalid connection settings"))
		return
	}
	if tlsConfig != nil {
		cfg.TLSConfig = tlsConfig
		cfg.Fallbacks = nil
	}
	start := time.Now()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		res.fail("connect", err)
		return
	}
	defer conn.Close(context.WithoutCancel(ctx))
	res.LatencyMS = time.Since(start).Milliseconds()
	res.OK = true
	res.add("connect", "ok", strconv.FormatInt(res.LatencyMS, 10)+" ms")

	var version string
	if err := conn.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		res.add("version", "warn", err.Error())
	} else {
		res.Version = version
		res.add("version", "ok", version)
	}
	var caps backup.PITRCaps
	var replication bool
	if err := conn.QueryRow(ctx, `SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&replication); err != nil {
		res.add("replication_role", "warn", err.Error())
	} else {
		caps.ReplicationRole = replication
		res.ReplicationRole = boolp(replication)
		if replication {
			res.add("replication_role", "ok", "role has the REPLICATION attribute (or is superuser)")
		} else {
			res.MissingPrivileges = append(res.MissingPrivileges, "REPLICATION")
			res.add("replication_role", "warn", "role lacks REPLICATION: pg_basebackup and WAL archiving need it")
		}
	}
	var wal string
	if err := conn.QueryRow(ctx, `SHOW wal_level`).Scan(&wal); err != nil {
		res.add("wal_level", "warn", err.Error())
	} else {
		res.WALLevel = wal
		caps.WALLevelOK = wal == "replica" || wal == "logical"
		st := "ok"
		if !caps.WALLevelOK {
			st = "warn"
		}
		res.add("wal_level", st, "wal_level="+wal)
	}
	var senders string
	if err := conn.QueryRow(ctx, `SHOW max_wal_senders`).Scan(&senders); err == nil {
		if n, _ := strconv.Atoi(senders); n < 1 {
			res.add("max_wal_senders", "warn", "max_wal_senders=0: pg_basebackup cannot connect")
		} else {
			res.add("max_wal_senders", "ok", "max_wal_senders="+senders)
		}
	}
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY 1`)
	if err != nil {
		res.add("databases", "warn", err.Error())
	} else {
		for rows.Next() {
			var d string
			if rows.Scan(&d) == nil {
				res.Databases = append(res.Databases, d)
			}
		}
		rows.Close()
		res.add("databases", "ok", fmt.Sprintf("%d database(s)", len(res.Databases)))
	}
	res.assessPITR(caps)
}

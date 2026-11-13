package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Opts gom mọi tham số; ghi nguyên vào plan.json trước khi đo.
type Opts struct {
	Engine                                   string // mysql | postgres
	Strand                                   string
	SourceDSN, TargetDSN                     string
	SourceContainer, TargetContainer         string
	HelperImage                              string
	FixtureDir                               string
	Out                                      string
	Governors                                string
	Repeats                                  int
	Seed                                     int64
	BigRows, AppRows, AppTables, PGScale     int
	Clients, Jobs                            int
	Wave                                     Wave
	Warmup, StreamAfter                      time.Duration
	CatchupTimeout, SnapshotTimeout          time.Duration
	MaxChunk                                 time.Duration
	SoftIOPS, HardIOPS                       int
	LagSLO, MetricsAddr                      string
	ClientContainer, ClientBinary, ClientSHA string
	Official, Attest, RerunFailed            bool
	Only                                     string
	RelaxBreaker                             bool // chỉ smoke dev: nới breaker p99 (như harness e2e)
}

// connInfo: host/port/user/password/db rút từ DSN để dựng dòng lệnh client.
type connInfo struct{ Host, Port, User, Password, DB string }

func parseConn(engine, dsn string) (connInfo, error) {
	var c connInfo
	if engine == "mysql" {
		cfg, err := mysqldrv.ParseDSN(dsn)
		if err != nil {
			return c, err
		}
		h, p, err := net.SplitHostPort(cfg.Addr)
		if err != nil {
			return c, err
		}
		return connInfo{h, p, cfg.User, cfg.Passwd, cfg.DBName}, nil
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return c, err
	}
	pw, _ := u.User.Password()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return connInfo{u.Hostname(), port, u.User.Username(), pw, strings.TrimPrefix(u.Path, "/")}, nil
}

func openDB(ctx context.Context, engine, dsn string, timeout time.Duration) (*sql.DB, error) {
	drv := "mysql"
	if engine == "postgres" {
		drv = "pgx"
	}
	deadline := time.Now().Add(timeout)
	for {
		db, err := sql.Open(drv, dsn)
		if err == nil {
			if err = db.PingContext(ctx); err == nil {
				db.SetMaxOpenConns(16)
				return db, nil
			}
			db.Close()
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("connect %s: %w", redactDSN(dsn), err)
		}
		time.Sleep(time.Second)
	}
}

func redactDSN(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, ok := u.User.Password(); ok {
			u.User = url.UserPassword(u.User.Username(), "***")
			return u.String()
		}
	}
	if i := strings.Index(dsn, "@"); i > 0 {
		if j := strings.Index(dsn[:i], ":"); j >= 0 {
			return dsn[:j+1] + "***" + dsn[i:]
		}
	}
	return dsn
}

// tableNames: các bảng được so khớp / đưa vào manifest.
func tableNames(o Opts) []string {
	if o.Engine == "postgres" {
		t := []string{"bench.big"}
		for _, n := range []string{"pgbench_accounts", "pgbench_branches", "pgbench_tellers", "pgbench_history"} {
			t = append(t, "bench."+n)
		}
		return t
	}
	t := []string{"bench.big"}
	for i := 1; i <= o.AppTables; i++ {
		t = append(t, fmt.Sprintf("bench.sbtest%d", i))
	}
	return t
}

// ComputeManifest: đếm + checksum nội dung từng bảng (không phụ thuộc thứ tự dòng).
func ComputeManifest(ctx context.Context, db *sql.DB, o Opts) (Manifest, error) {
	m := Manifest{Engine: o.Engine}
	for _, t := range tableNames(o) {
		var rows int64
		var sum string
		q, err := manifestQuery(ctx, db, o.Engine, t)
		if err != nil {
			return m, err
		}
		if err := db.QueryRowContext(ctx, q).Scan(&rows, &sum); err != nil {
			return m, fmt.Errorf("manifest %s: %w", t, err)
		}
		m.Tables = append(m.Tables, TableSum{Name: t, Rows: rows, Checksum: sum})
	}
	return m, nil
}

func manifestQuery(ctx context.Context, db *sql.DB, engine, table string) (string, error) {
	if engine == "postgres" {
		return fmt.Sprintf(`SELECT count(*), COALESCE(sum(('x'||substr(md5(t::text),1,8))::bit(32)::bigint),0)::text FROM %s t`, table), nil
	}
	schema, name, _ := strings.Cut(table, ".")
	rs, err := db.QueryContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ordinal_position", schema, name)
	if err != nil {
		return "", err
	}
	defer rs.Close()
	var cols []string
	for rs.Next() {
		var c string
		if err := rs.Scan(&c); err != nil {
			return "", err
		}
		cols = append(cols, fmt.Sprintf("IFNULL(CAST(`%s` AS CHAR),'\\\\N')", c))
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("table %s has no columns (missing)", table)
	}
	return fmt.Sprintf("SELECT COUNT(*), CAST(COALESCE(BIT_XOR(CRC32(CONCAT_WS('#',%s))),0) AS CHAR) FROM %s", strings.Join(cols, ","), table), nil
}

func compareManifests(src, dst Manifest) (bool, string) {
	d := DiffManifest(src, dst)
	if len(d) == 0 {
		return true, fmt.Sprintf("match: %d tables, manifest %s", len(src.Tables), short(src.Hash()))
	}
	return false, strings.Join(d, "; ")
}

// ---------------------------------------------------------------------------
// Lệnh client ngoài
// ---------------------------------------------------------------------------

// clientArgv: argv chạy một đoạn tải. Client chạy qua `docker exec <container> env K=V binary ...`
// (hoặc trực tiếp nếu không có container). prefix = đường dẫn log thô của đoạn.
func clientArgv(o Opts, ci connInfo, seg Segment, threads int, prefix string, raw bool) []string {
	secs := strconv.Itoa(int(math.Ceil(seg.Dur.Seconds())))
	var env, args []string
	bin := o.ClientBinary
	if o.Engine == "postgres" {
		env = []string{"PGPASSWORD=" + ci.Password, "PGOPTIONS=-c search_path=bench"}
		jobs := o.Jobs
		if jobs > threads {
			jobs = threads
		}
		args = []string{"-h", ci.Host, "-p", ci.Port, "-U", ci.User, "-d", ci.DB, "-n", "-b", "tpcb-like",
			"-c", strconv.Itoa(threads), "-j", strconv.Itoa(jobs), "-T", secs}
		if seg.Rate > 0 {
			args = append(args, "-R", strconv.FormatFloat(seg.Rate, 'f', -1, 64))
		}
		if raw {
			args = append(args, "-l", "--log-prefix="+prefix)
		}
	} else {
		if raw {
			env = []string{"STRAND_BENCH_RAW_PREFIX=" + prefix}
		}
		args = append(sysbenchConn(ci, o), "--threads="+strconv.Itoa(threads), "--time="+secs)
		rate := int(math.Round(seg.Rate))
		if seg.Rate > 0 && rate < 1 {
			rate = 1
		}
		args = append(args, "--rate="+strconv.Itoa(rate), "oltp_read_write", "run")
	}
	return wrapExec(o, env, bin, args)
}

func sysbenchConn(ci connInfo, o Opts) []string {
	return []string{"--db-driver=mysql", "--mysql-host=" + ci.Host, "--mysql-port=" + ci.Port,
		"--mysql-user=" + ci.User, "--mysql-password=" + ci.Password, "--mysql-db=bench",
		"--tables=" + strconv.Itoa(o.AppTables), "--table-size=" + strconv.Itoa(o.AppRows)}
}

func wrapExec(o Opts, env []string, bin string, args []string) []string {
	argv := []string{"env"}
	argv = append(argv, env...)
	argv = append(argv, bin)
	argv = append(argv, args...)
	if o.ClientContainer != "" {
		return append([]string{"docker", "exec", o.ClientContainer}, argv...)
	}
	return argv
}

// ---------------------------------------------------------------------------
// Strand
// ---------------------------------------------------------------------------

func strandYAML(o Opts, gov, runDir string) string {
	y := strandYAMLBase(o, gov, runDir)
	if o.RelaxBreaker {
		y = strings.Replace(y, "governor: { algorithm: "+gov+", tick: 1s, lag_slo: "+o.LagSLO+" }",
			"governor: { algorithm: "+gov+", tick: 1s, lag_slo: "+o.LagSLO+", breaker: { strand_query_p99: { max_ratio: 1000 } } }", 1)
	}
	return y
}

func strandYAMLBase(o Opts, gov, runDir string) string {
	if o.Engine == "postgres" {
		return fmt.Sprintf(`version: 1
name: bench
source: { kind: postgres, dsn: %q, slot: g2bench_slot, publication: g2bench_pub, create_slot: true }
target: { kind: postgres, dsn: %q }
tables: { include: ["bench.*"] }
bootstrap: { default: logical }
checkpoint: { store: target, flush_every: 200ms }
resources:
  source: { iops: { soft: %d, hard: %d } }
  spool: { path: %q, hard: 2GiB }
governor: { algorithm: %s, tick: 1s, lag_slo: %s }
observability: { metrics: %q, log: { level: info, format: json } }
`, o.SourceDSN, o.TargetDSN, o.SoftIOPS, o.HardIOPS, filepath.Join(runDir, "state"), gov, o.LagSLO, o.MetricsAddr)
	}
	return fmt.Sprintf(`version: 1
name: bench
source: { kind: mysql, dsn: %q, server_id: 47999 }
target: { kind: mysql, dsn: %q }
tables: { include: ["bench.*"] }
bootstrap: { default: logical }
checkpoint: { store: target, flush_every: 200ms }
resources:
  source: { iops: { soft: %d, hard: %d } }
  spool: { path: %q, hard: 2GiB }
governor: { algorithm: %s, tick: 1s, lag_slo: %s }
observability: { metrics: %q, log: { level: info, format: json } }
`, o.SourceDSN, o.TargetDSN+"strand", o.SoftIOPS, o.HardIOPS, filepath.Join(runDir, "state"), gov, o.LagSLO, o.MetricsAddr)
}

// waitCaughtUp: chờ target apply bền mọi thay đổi của source sau khi dừng tải.
func waitCaughtUp(ctx context.Context, o Opts, src, dst *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	if o.Engine == "postgres" {
		var head string
		if err := src.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&head); err != nil {
			return err
		}
		// confirmed_flush_lsn chỉ tiến khi có keepalive/ack nên có thể đứng yên sau khi dừng tải: chờ tối đa 30 s,
		// rồi dùng đúng điều kiện cần chứng minh — manifest source = target — thăm dò mỗi 10 s tới hạn chót.
		slotDeadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && time.Now().Before(slotDeadline) {
			var ok bool
			err := src.QueryRowContext(ctx, `SELECT COALESCE(confirmed_flush_lsn >= $1::pg_lsn, false) FROM pg_replication_slots WHERE slot_name='g2bench_slot'`, head).Scan(&ok)
			if err == nil && ok {
				return nil
			}
			last = err
			time.Sleep(time.Second)
		}
		for time.Now().Before(deadline) {
			sm, e1 := ComputeManifest(ctx, src, o)
			dm, e2 := ComputeManifest(ctx, dst, o)
			if e1 == nil && e2 == nil {
				if ok, d := compareManifests(sm, dm); ok {
					return nil
				} else {
					last = fmt.Errorf("%s", d)
				}
			}
			time.Sleep(10 * time.Second)
		}
		return fmt.Errorf("target not equal to source after %s: %v", timeout, last)
	}
	for time.Now().Before(deadline) {
		var executed string
		if err := src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			return err
		}
		want, err := checkpoint.ParseMySQLGTIDSet(executed)
		if err != nil {
			return err
		}
		gotSet, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, dst, "bench", model.EngineMySQL)
		if err == nil {
			if got, _ := gotSet.(*checkpoint.MySQLAppliedSet); got != nil {
				miss := want.Clone().(*checkpoint.MySQLAppliedSet)
				miss.Subtract(got)
				if strings.TrimSpace(miss.String()) == "" {
					return nil
				}
				last = fmt.Errorf("missing %s", miss.String())
			}
		} else {
			last = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("target not caught up after %s: %v", timeout, last)
}

// Redacted: bản sao đã che mật khẩu trong DSN, dùng khi ghi plan/results.
func (o Opts) Redacted() Opts {
	o.SourceDSN, o.TargetDSN = redactDSN(o.SourceDSN), redactDSN(o.TargetDSN)
	return o
}

func init() {
	// driver ghi "unexpected EOF" khi chờ server khởi động lại; lỗi thật vẫn trả về qua error.
	_ = mysqldrv.SetLogger(log.New(io.Discard, "", 0))
}

// Package doctor chạy toàn bộ preflight của một pipeline SQL (MySQL/MariaDB/PostgreSQL) và gom MỌI vấn đề vào
// một findings.Collector thay vì dừng ở lỗi đầu tiên như `strand validate`. Các luật dùng chung với validate và
// với bước khởi động của sync (internal/validate, internal/pipeline); doctor chỉ nối chúng lại.
//
// Chỉ đọc: không DDL, không ghi, không LOCK TABLES / FLUSH TABLES, không tạo slot hay publication.
package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/findings"
	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/objects"
	"strand/internal/pipeline"
	"strand/internal/schemagen"
	"strand/internal/typemap"
	"strand/internal/validate"
)

// PingTimeout giới hạn thời gian chờ kết nối mỗi phía.
var PingTimeout = 15 * time.Second

// WaitFreeSampleInterval: khoảng tối thiểu giữa hai lần đọc Innodb_buffer_pool_wait_free.
var WaitFreeSampleInterval = 2 * time.Second

type run struct {
	cfg              *config.Config
	c                *findings.Collector
	srcKind, dstKind string
	src, dst         *sql.DB
	srcVer, dstVer   *mysqlver.Version
	ko               validate.KeylessOpts
	keyless          []model.TableID
	ids              []model.TableID // bảng nguồn trong phạm vi (schemagen.Discover)
	waitFree         map[string]waitFreeSample
}

type waitFreeSample struct {
	at    time.Time
	value uint64
}

// Run kiểm cấu hình cfg (pipeline SQL; MongoDB do cmd/strand xử lý) và ghi mọi vấn đề vào c.
func Run(ctx context.Context, cfg *config.Config, c *findings.Collector) {
	d := &run{cfg: cfg, c: c, srcKind: strings.ToLower(cfg.Source.Kind), dstKind: strings.ToLower(cfg.Target.Kind), waitFree: map[string]waitFreeSample{}}
	if !d.configChecks(ctx) {
		return
	}
	d.open(ctx)
	defer func() {
		for _, db := range []*sql.DB{d.src, d.dst} {
			if db != nil {
				mysqlver.Forget(db)
				db.Close()
			}
		}
	}()
	d.ko = validate.KeylessOptsFromConfig(cfg)
	d.ko.FoundKeyless = func(id model.TableID) { d.keyless = append(d.keyless, id) }
	if d.src != nil {
		ids, warnings, err := schemagen.Discover(ctx, d.src, d.srcKind, cfg.Tables.Include, cfg.Tables.Exclude)
		if err != nil {
			d.fail("tables_discover", "source", err, "")
		}
		for _, w := range warnings {
			d.c.Warn("schema_warning", "", w, "")
		}
		d.ids = ids
	}
	if d.src != nil && d.srcKind == "mysql" {
		d.sampleWaitFree(ctx, "source", d.src)
		d.mysqlSource(ctx)
	}
	if d.dst != nil && d.dstKind == "mysql" {
		d.sampleWaitFree(ctx, "target", d.dst)
		d.mysqlTarget(ctx)
	}
	if d.src != nil && d.dst != nil {
		d.cross(ctx)
	}
	d.finishWaitFree(ctx)
}

// fail ghi err của một kiểm tra: lỗi từ chối (exit 3/5) thành vấn đề error với mã code; lỗi khác (không đọc
// được catalog, mất kết nối...) thành cảnh báo check_failed: kiểm tra đó không kết luận được.
func (d *run) fail(code, object string, err error, fix string) {
	if err == nil {
		return
	}
	switch exitcode.GetExitCode(err) {
	case exitcode.Rejected, exitcode.DataError, exitcode.InvalidSyntax:
		d.c.Err(findings.Error, code, object, err, fix)
	default:
		d.c.Add(findings.Finding{Severity: findings.Warning, Code: "check_failed", Object: object,
			Message: fmt.Sprintf("could not complete check %s: %s", code, findings.Message(err)),
			Fix:     "check the strand user's privileges on information_schema and the connection, then run doctor again"})
	}
}

func (d *run) mapTarget(id model.TableID) model.TableID {
	if to, ok := d.cfg.Tables.Rename[id.String()]; ok {
		if s, t, ok := strings.Cut(to, "."); ok {
			return model.TableID{Schema: s, Table: t}
		}
	}
	return id
}

// configChecks: kiểm cấu hình không cần kết nối. false = không đi tiếp được (kind không hợp lệ).
func (d *run) configChecks(ctx context.Context) bool {
	cfg := d.cfg
	for _, k := range cfg.UnimplementedKeys() {
		d.c.Warn("config_key_unimplemented", k, config.UnimplementedWarning(k), "remove "+k+" from the config")
	}
	if w := validate.MaxParallelWarning(cfg); w != "" {
		d.c.Warn("config_max_parallel_ignored", "bootstrap.logical.max_parallel", w, "remove bootstrap.logical.max_parallel")
	}
	if err := validate.Preflight(ctx, cfg); err != nil {
		d.c.Err(findings.Error, "config_invalid", "config", err, "fix the key named in the message in the pipeline YAML")
	}
	okKind := func(k string) bool { return k == "mysql" || k == "postgres" }
	return okKind(d.srcKind) && okKind(d.dstKind)
}

func driver(kind string) string {
	if kind == "postgres" {
		return "pgx"
	}
	return "mysql"
}

// dsnPassword: mật khẩu trong DSN (để che khỏi mọi thông điệp).
func dsnPassword(kind, dsn string) string {
	if kind == "postgres" {
		if pc, err := config.ParsePGConfig(dsn); err == nil {
			return pc.Password
		}
		return ""
	}
	if mc, err := drivermysql.ParseDSN(dsn); err == nil {
		return mc.Passwd
	}
	return ""
}

func (d *run) open(ctx context.Context) {
	d.c.AddSecret(dsnPassword(d.srcKind, d.cfg.Source.DSN))
	d.c.AddSecret(dsnPassword(d.dstKind, d.cfg.Target.DSN))
	openOne := func(side, kind, dsn string) *sql.DB {
		if dsn == "" {
			d.c.Errorf(side+"_dsn_invalid", side, side+".dsn is empty", "set "+side+".dsn (or host/user/password_env) in the pipeline YAML")
			return nil
		}
		db, err := config.OpenSQL(driver(kind), dsn)
		if err != nil {
			d.c.Errorf(side+"_dsn_invalid", side, "invalid "+side+" DSN: "+err.Error(), "fix "+side+".dsn in the pipeline YAML")
			return nil
		}
		db.SetMaxOpenConns(4)
		pctx, cancel := context.WithTimeout(ctx, PingTimeout)
		defer cancel()
		if err := db.PingContext(pctx); err != nil {
			db.Close()
			d.c.Errorf(side+"_unreachable", side, "cannot connect to the "+side+": "+err.Error(),
				"check "+side+".dsn (host, port, user, password), that the server is up, and network/firewall from this host")
			return nil
		}
		return db
	}
	d.src = openOne("source", d.srcKind, d.cfg.Source.DSN)
	d.dst = openOne("target", d.dstKind, d.cfg.Target.DSN)
}

// ---------- source MySQL / MariaDB ----------

func (d *run) mysqlSource(ctx context.Context) {
	cfg, src := d.cfg, d.src
	ver, err := mysqlver.Cached(ctx, src)
	if err != nil {
		d.fail("source_version", "source", err, "")
		return
	}
	d.srcVer = &ver
	flavor := "MySQL"
	if ver.IsMariaDB() {
		flavor = "MariaDB"
	}
	d.c.Info("source_version", "source", fmt.Sprintf("source is %s %s", flavor, ver.Raw), "")
	if err := mysqlver.CheckTargetForSource(ver, cfg.Target.Kind); err != nil {
		d.fail("source_target_pair_unsupported", "source", err, "use a MySQL 8.x target (target.kind: mysql)")
	}
	tables, unmatched, err := validate.ExpandTablesReport(ctx, src, cfg.Tables.Include, cfg.Tables.Exclude)
	if err != nil {
		d.fail("tables_include_invalid", "tables.include", err, "fix the pattern in tables.include (patterns match schema.table, e.g. app.*)")
		return
	}
	for _, p := range unmatched {
		d.c.Errorf("tables_include_no_match", p, validate.UnmatchedPatternMessage(p),
			"fix or remove the pattern in tables.include (patterns match schema.table, e.g. app.*), or grant the strand user SELECT on those tables")
	}
	if _, err := validate.CheckMySQLSourceReport(ctx, src, tables, d.ko, d.c); err != nil {
		d.fail("source_binlog", "source", err, "")
	}
	d.sourcePrivileges(ctx, ver, tables)
	warns, err := validate.CheckMySQLSessionBinlog(ctx, src)
	for _, w := range warns {
		if strings.HasPrefix(w, "binlog_row_metadata") {
			d.c.Warn("source_binlog_row_metadata", "source", w, ver.PersistAdvice("binlog_row_metadata", "FULL"))
		} else if strings.HasPrefix(w, "source is MySQL") {
			d.c.Info("source_binlog_row_metadata", "source", w, "")
		} else if strings.Contains(w, "does not exist") {
			d.c.Warn("source_session_binlog_unverified", "source", w,
				"set performance_schema=ON in the [mysqld] section of my.cnf (restart needed), or check that no open connection still logs with binlog_format other than ROW")
		} else {
			d.c.Warn("source_session_binlog_unverified", "source", w,
				"GRANT SELECT ON performance_schema.variables_by_thread TO <strand user>; GRANT SELECT ON performance_schema.threads TO <strand user>;")
		}
	}
	d.fail("source_binlog_session_stale", "source", err,
		"reconnect the listed clients (restart the application connection pool; on a replica run STOP REPLICA; START REPLICA)")
	if w := validate.ReplicaStatusWarning(ctx, src); w != "" {
		grant := "GRANT REPLICATION CLIENT ON *.* TO <strand user>;"
		if ver.IsMariaDB() && ver.AtLeast(10, 5, 0) {
			grant = "GRANT SLAVE MONITOR ON *.* TO <strand user>;"
		}
		d.c.Warn("source_replica_status_unreadable", "source", w, grant)
	}
	d.binlogRetention(ctx, ver)
	if sum, err := objects.Summary(ctx, src, objects.Options{Include: cfg.Tables.Include, Exclude: cfg.Tables.Exclude}); err != nil {
		d.c.Warn("source_objects_uncounted", "source", "cannot count views/routines/triggers/events (source account lacks privileges?): "+findings.Message(err), "")
	} else if w := objects.SummaryWarning(sum, d.dstKind == "mysql"); w != "" {
		d.c.Info("source_objects_not_migrated", "source", w, "")
	}
	if d.dstKind == "postgres" && d.dst != nil {
		opts := typemap.DefaultOptions()
		opts.AllowKeyless = cfg.Tables.KeylessOn()
		opts.ZeroDateFor = cfg.Tables.ZeroDateFor
		ws, err := typemap.CheckMySQLToPGTablesMapped(ctx, src, d.dst, tables, opts, cfg.Tables.Rename)
		for _, w := range ws {
			d.c.Warn("schema_type_mapping", "", w, "")
		}
		d.fail("source_type_unmappable", "source", err, "change the column type on the source, or exclude the table")
	}
}

// sourcePrivileges: quyền tài khoản source cần cho cấu hình này (docs/USAGE.md §4.3), theo đúng câu lệnh code dùng:
// binlog dump (REPLICATION SLAVE), SHOW MASTER/BINARY LOG STATUS và SHOW BINLOG EVENTS (REPLICATION CLIENT,
// MariaDB 10.5+: BINLOG MONITOR), SELECT trên bảng, LOCK TABLES … READ của snapshot logic ở chế độ vị trí binlog,
// FLUSH TABLES WITH READ LOCK của auto-seed trên MySQL (RELOAD; MariaDB đọc Binlog_snapshot_file nên không cần).
func (d *run) sourcePrivileges(ctx context.Context, ver mysqlver.Version, tables []string) {
	g, err := validate.ReadGrants(ctx, d.src)
	if err != nil {
		d.fail("source_privileges", "source", err, "")
		return
	}
	client := "REPLICATION CLIENT"
	if ver.IsMariaDB() && ver.AtLeast(10, 5, 2) {
		client = "BINLOG MONITOR"
	}
	needs := []validate.PrivilegeNeed{
		{Priv: "REPLICATION SLAVE", Why: "read the binlog", Severity: findings.Error},
		// Tên chuẩn hóa để so; câu GRANT in tên của server (MariaDB 10.5.2+: BINLOG MONITOR).
		{Priv: "REPLICATION CLIENT", Display: client, Why: "read the binlog position", Severity: findings.Error},
	}
	posMode := ver.IsMariaDB()
	if !posMode {
		var gtid string
		if err := d.src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&gtid); err != nil || !strings.EqualFold(gtid, "ON") {
			posMode = true
		}
	}
	def := strings.ToLower(strings.TrimSpace(d.cfg.Bootstrap.Default))
	logical := def == "" || def == "logical" || d.cfg.Bootstrap.AutoSeed
	if d.cfg.Bootstrap.AutoSeed && !ver.IsMariaDB() {
		needs = append(needs, validate.PrivilegeNeed{Priv: "RELOAD", Why: "auto-seed takes a short FLUSH TABLES WITH READ LOCK on MySQL sources", Severity: findings.Error})
	}
	bySchema := map[string][]string{}
	for _, t := range tables {
		s, tb, _ := strings.Cut(t, ".")
		bySchema[s] = append(bySchema[s], tb)
	}
	schemas := make([]string, 0, len(bySchema))
	for s := range bySchema {
		schemas = append(schemas, s)
	}
	sort.Strings(schemas)
	for _, s := range schemas {
		needs = append(needs, validate.PrivilegeNeed{Priv: "SELECT", Schema: s, Why: "snapshot and verify", Severity: findings.Error})
		if posMode && logical {
			needs = append(needs, validate.PrivilegeNeed{Priv: "LOCK TABLES", Schema: s, Why: "logical snapshot in binlog position mode", Severity: findings.Error})
		}
	}
	needs = append(needs, validate.PrivilegeNeed{Priv: "PROCESS", Why: "list long-running transactions when a snapshot waits", Severity: findings.Warning})
	validate.ReportPrivileges(g, "source", needs, bySchema, d.c)
}

func (d *run) binlogRetention(ctx context.Context, ver mysqlver.Version) {
	expire, err := mysqlver.BinlogRetentionSeconds(ctx, d.src)
	if err != nil {
		d.fail("source_binlog_retention", "source", err, "")
		return
	}
	variable, value := "binlog_expire_logs_seconds", "604800"
	if !ver.HasBinlogExpireLogsSeconds() {
		variable, value = "expire_logs_days", "7"
	}
	switch {
	case expire <= 0:
		d.c.Info("source_binlog_retention", "source", "binlog retention is unlimited (binlogs are never purged automatically)", "")
	case expire < 24*3600:
		d.c.Warn("source_binlog_retention_short", "source", fmt.Sprintf(
			"binlog retention is only %s: if the initial copy or an outage lasts longer, the binlog Strand still needs is purged (log_gap) and the copy must start over",
			time.Duration(expire)*time.Second), ver.PersistAdvice(variable, value)+"; run strand plan to compare with the copy ETA (ETA must be at most 50% of retention)")
	default:
		d.c.Info("source_binlog_retention", "source", fmt.Sprintf("binlog retention is %s; strand plan checks the initial copy ETA against it (ETA must be at most 50%%)", time.Duration(expire)*time.Second), "")
	}
}

// ---------- target MySQL ----------

func (d *run) mysqlTarget(ctx context.Context) {
	dst := d.dst
	ver, err := mysqlver.Cached(ctx, dst)
	if err != nil {
		d.fail("target_version", "target", err, "")
		return
	}
	d.dstVer = &ver
	if d.srcVer != nil && d.srcVer.IsMariaDB() && (ver.IsMariaDB() || !ver.AtLeast(8, 0, 0)) {
		d.c.Warn("target_version_unsupported", "target", fmt.Sprintf("MariaDB sources are supported only with a MySQL 8.x target; the target is %s", ver.Raw),
			"use a MySQL 8.0/8.4 target")
	}
	var readOnly, superReadOnly sql.NullInt64
	_ = dst.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only").Scan(&readOnly)
	_ = dst.QueryRowContext(ctx, "SELECT @@GLOBAL.super_read_only").Scan(&superReadOnly) // MariaDB: không có biến
	g, gerr := validate.ReadGrants(ctx, dst)
	switch {
	case superReadOnly.Int64 == 1:
		d.c.Errorf("target_read_only", "target", "the target has super_read_only=ON (a Group Replication secondary or a replica): every write is refused",
			"point target.dsn at the writable member (the Group Replication primary), or SET GLOBAL super_read_only=OFF")
	case readOnly.Int64 == 1 && (gerr != nil || !g.HasGlobal("SUPER") && !g.HasGlobal("CONNECTION_ADMIN")):
		d.c.Warn("target_read_only", "target", "the target has read_only=ON: writes need the CONNECTION_ADMIN (or SUPER) privilege",
			"SET GLOBAL read_only=OFF, or GRANT CONNECTION_ADMIN ON *.* TO <strand user>;")
	}
	if gerr != nil {
		d.fail("target_privileges", "target", gerr, "")
	} else {
		d.targetPrivileges(ctx, g, ver)
	}
	active, err := pipeline.TargetGroupReplicationActive(ctx, dst)
	if err != nil {
		d.fail("target_group_replication", "target", err, "")
	} else if active {
		d.c.Info("target_group_replication", "target",
			"the target is a running Group Replication member: every table needs a primary key (or a unique NOT NULL key), large transactions are split (auto-seed commits in parts below group_replication_transaction_size_limit)", "")
	}
}

func (d *run) targetPrivileges(ctx context.Context, g *validate.Grants, ver mysqlver.Version) {
	var meta sql.NullString
	_ = d.dst.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&meta)
	schemas := map[string]bool{}
	for _, id := range d.ids {
		schemas[d.mapTarget(id).Schema] = true
	}
	var needs []validate.PrivilegeNeed
	names := make([]string, 0, len(schemas))
	for s := range schemas {
		names = append(names, s)
	}
	sort.Strings(names)
	for _, s := range names {
		for _, p := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
			needs = append(needs, validate.PrivilegeNeed{Priv: p, Schema: s, Why: "apply changes", Severity: findings.Error})
		}
		if d.cfg.Bootstrap.CreateSchemaAuto() {
			for _, p := range []string{"CREATE", "ALTER", "INDEX", "REFERENCES"} {
				needs = append(needs, validate.PrivilegeNeed{Priv: p, Schema: s, Why: "create missing target tables (bootstrap.create_schema: auto)", Severity: findings.Error})
			}
		}
		if d.cfg.Bootstrap.AutoSeed {
			needs = append(needs, validate.PrivilegeNeed{Priv: "DROP", Schema: s, Why: "TRUNCATE when auto-seed redoes an interrupted copy", Severity: findings.Warning})
		}
	}
	if meta.Valid && meta.String != "" {
		for _, p := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE"} {
			needs = append(needs, validate.PrivilegeNeed{Priv: p, Schema: meta.String, Why: "checkpoint tables _strand_*", Severity: findings.Error})
		}
	}
	validate.ReportPrivileges(g, "target", needs, nil, d.c)
	if !ver.IsMariaDB() && ver.AtLeast(8, 0, 0) && !g.HasGlobal("CONNECTION_ADMIN") && !g.HasGlobal("SUPER") {
		d.c.Warn("target_privilege_missing", "target", fmt.Sprintf("the target user %s lacks CONNECTION_ADMIN (write while read_only=ON, disconnect a stale instance of this pipeline)", g.User),
			g.GrantSQL([]string{"CONNECTION_ADMIN"}, "*.*"))
	}
}

// ---------- hai phía ----------

func (d *run) cross(ctx context.Context) {
	cfg, src, dst := d.cfg, d.src, d.dst
	ids := d.ids
	if len(ids) == 0 {
		return
	}
	if d.srcKind == d.dstKind {
		engine := model.EngineMySQL
		if d.srcKind == "postgres" {
			engine = model.EnginePG
		}
		d.fail("target_is_source", "target", validate.CheckTableOverlap(ctx, src, dst, engine, ids, d.mapTarget),
			"point target.dsn at another server or database, or map the tables elsewhere with tables.rename")
	}
	if d.srcKind == "postgres" {
		ws, err := validate.CheckPGSourceReport(ctx, src, cfg, ids, d.c)
		for _, w := range ws {
			if strings.Contains(w, "not explicitly configured") {
				d.c.Info("source_pg_slot_unset", "source", w, "set source.slot: <name> (with source.create_slot: true sync creates it) so doctor can check it")
			} else {
				d.c.Warn("source_pg_slot", "source", w, "")
			}
		}
		d.fail("source_pg", "source", err, "")
	}
	d.schemaPlan(ctx, ids)
	lines, err := validate.CheckKeysReport(ctx, src, d.srcKind, dst, d.dstKind, ids, cfg.Tables.Rename, d.ko, d.c)
	for _, l := range lines {
		if w, ok := strings.CutPrefix(l, "warning: "); ok {
			d.c.Warn("source_table_keyless", "", w, "")
		} else {
			d.c.Info("table_row_key", "", l, "")
		}
	}
	d.fail("table_keys", "", err, "")
	if d.srcKind == "mysql" && d.dstKind == "mysql" {
		d.foreignKeys(ctx, ids)
		if len(d.keyless) > 0 {
			d.fail("target_gr_keyless", "target", pipeline.GroupReplicationKeylessReport(ctx, dst, d.keyless, d.mapTarget, cfg.Bootstrap.CreateSchemaAuto(), d.c), "")
		}
	}
	if d.dstKind == "mysql" {
		targets := make([]model.TableID, 0, len(ids))
		for _, id := range ids {
			targets = append(targets, d.mapTarget(id))
		}
		d.fail("target_engine_not_innodb", "target", validate.CheckMySQLTargetEngines(ctx, dst, targets), "ALTER TABLE <table> ENGINE=InnoDB; for each table listed")
		if d.srcVer != nil && d.srcVer.IsMariaDB() {
			names := make([]string, 0, len(ids))
			for _, id := range ids {
				names = append(names, id.String())
			}
			ws, err := validate.CheckSourceCollationsOnTargetReport(ctx, src, dst, names, d.c)
			for _, w := range ws {
				d.c.Warn("schema_warning", "", w, "")
			}
			d.fail("target_collation_unmapped", "target", err, "")
		}
	}
}

// schemaPlan: bảng target còn thiếu và DDL schemagen sẽ chạy; lỗi được gán cho từng bảng.
func (d *run) schemaPlan(ctx context.Context, ids []model.TableID) {
	r := schemagen.Request{SourceKind: d.srcKind, TargetKind: d.dstKind, Rename: d.cfg.Tables.Rename, Source: d.src, Target: d.dst, Tables: ids}
	plan, err := schemagen.Build(ctx, r)
	if err != nil {
		attributed := false
		var created []model.TableID
		for _, id := range ids {
			one := r
			one.Tables = []model.TableID{id}
			p, err := schemagen.Build(ctx, one)
			if err != nil {
				attributed = true
				d.fail("schema_create_unsupported", id.String(), err, "change the table on the source as the message says, create the target table by hand, or add it to tables.exclude")
				continue
			}
			created = append(created, p.Tables...)
		}
		if !attributed {
			d.fail("schema_create_unsupported", "target", err, "")
		}
		plan = &schemagen.Plan{Tables: created}
	}
	for _, w := range plan.Warnings {
		d.c.Warn("schema_warning", "", w, "")
	}
	if len(plan.Tables) == 0 {
		return
	}
	names := make([]string, 0, len(plan.Tables))
	for _, t := range plan.Tables {
		names = append(names, t.String())
	}
	sort.Strings(names)
	shown := names
	if len(shown) > 10 {
		shown = append(shown[:10:10], fmt.Sprintf("and %d more", len(names)-10))
	}
	if !d.cfg.Bootstrap.CreateSchemaAuto() {
		d.c.Errorf("target_table_missing", "target", fmt.Sprintf("%d target table(s) missing and bootstrap.create_schema=off: %s", len(names), strings.Join(shown, ", ")),
			"set bootstrap.create_schema: auto, or create them first: strand schema init -c <config>")
		return
	}
	d.c.Info("target_tables_to_create", "target", fmt.Sprintf("%d table(s) will be created on the target: %s", len(names), strings.Join(shown, ", ")), "")
}

// foreignKeys: cùng nhánh validate/Start: auto-seed chưa có checkpoint so FK cho phép FK còn thiếu (auto-seed
// thêm) và kiểm nhóm FK; còn lại so FK như Start (FK thiếu trên target bị từ chối).
func (d *run) foreignKeys(ctx context.Context, ids []model.TableID) {
	cfg := d.cfg
	if cfg.Bootstrap.AutoSeed {
		resumed, err := pipeline.AutoSeedResumed(ctx, d.dst, cfg.Name)
		if err != nil {
			d.fail("target_dsn_no_database", "target", err, "add the checkpoint database to target.dsn, e.g. user:pass@tcp(host:3306)/strand_meta")
			return
		}
		if !resumed {
			d.fail("autoseed_preflight", "target", pipeline.AutoSeedPreflightReport(ctx, d.src, d.dst, cfg.Bootstrap.CreateSchemaAuto(), ids, d.keyless, d.mapTarget, d.c), "")
			return
		}
	}
	d.fail("target_fk", "target", pipeline.ReportMySQLForeignKeys(ctx, d.src, d.dst, ids, d.keyless, d.mapTarget, d.c), "")
}

// ---------- Innodb_buffer_pool_wait_free ----------

func readStatus(ctx context.Context, db *sql.DB, name string) (uint64, error) {
	var n, v string
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE '"+name+"'").Scan(&n, &v); err != nil {
		return 0, err
	}
	return strconv.ParseUint(v, 10, 64)
}

func (d *run) sampleWaitFree(ctx context.Context, side string, db *sql.DB) {
	v, err := readStatus(ctx, db, "Innodb_buffer_pool_wait_free")
	if err != nil {
		return
	}
	d.waitFree[side] = waitFreeSample{at: time.Now(), value: v}
}

// finishWaitFree: governor mở breaker khi Innodb_buffer_pool_wait_free tăng giữa hai tick (tín hiệu cứng). Đọc
// lại sau ít nhất WaitFreeSampleInterval; tăng nghĩa là InnoDB đang phải chờ trang trống ngay lúc này.
func (d *run) finishWaitFree(ctx context.Context) {
	for _, side := range []string{"source", "target"} {
		first, ok := d.waitFree[side]
		if !ok {
			continue
		}
		db := d.src
		if side == "target" {
			db = d.dst
		}
		if wait := WaitFreeSampleInterval - time.Since(first.at); wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		v, err := readStatus(ctx, db, "Innodb_buffer_pool_wait_free")
		if err != nil || v <= first.value {
			continue
		}
		advice := "SET PERSIST innodb_buffer_pool_size = <larger>"
		ver := d.srcVer
		if side == "target" {
			ver = d.dstVer
		}
		if ver != nil {
			advice = ver.PersistAdvice("innodb_buffer_pool_size", "<larger>")
		}
		d.c.Warn(side+"_innodb_wait_free", side, fmt.Sprintf(
			"Innodb_buffer_pool_wait_free rose by %d in %s: InnoDB is waiting for free buffer pool pages now, and the Strand governor opens the breaker (pauses) on every increase",
			v-first.value, time.Since(first.at).Round(time.Second)),
			advice+"; or raise innodb_lru_scan_depth / innodb_page_cleaners, or run the initial copy off-peak")
	}
}

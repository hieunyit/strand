package runner

import (
	"archive/tar"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/backup"
)

// RestoreTarget is where a restore goes. It is always NEW: logical restores create Database (which must
// not exist) on the target server; physical restores fill Dir (which must not exist or be empty). The
// source database is never written.
type RestoreTarget struct {
	Conn     backup.ConnSpec
	Database string
	Dir      string
}

// Restore executes plan into target.
func (e *Env) Restore(ctx context.Context, plan RestorePlan, t RestoreTarget) (err error) {
	base := plan.Base()
	defer func() {
		detail := map[string]any{"backup_id": base.ID, "source": base.Source, "logs": len(plan.Logs), "target_host": t.Conn.Host,
			"target_database": t.Database, "target_dir": t.Dir}
		if plan.Target != nil {
			detail["target_time"] = plan.Target.UTC().Format(time.RFC3339)
		}
		err = e.audit(context.WithoutCancel(ctx), "backup.restore", err, detail)
	}()
	e.logf("restore: base %s (%s, %s) + %d log segment(s)", base.ID, base.Method, ConsistentTime(base).Format(time.RFC3339), len(plan.Logs))
	switch base.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		return e.restoreMySQL(ctx, plan, t)
	case backup.MethodPGDump:
		return e.restorePGDump(ctx, base, t)
	case backup.MethodPGBaseBackup:
		return e.restorePGBase(ctx, plan, t)
	case backup.MethodMongodump:
		return e.restoreMongo(ctx, plan, t)
	case backup.MethodXtrabackup, backup.MethodMariabackup:
		return e.restoreXtrabackup(ctx, plan, t)
	}
	return fmt.Errorf("restore of method %q is not supported; use `strand backup fetch` and the native tool", base.Method)
}

func (e *Env) openRecord(ctx context.Context, r backup.Record) (io.ReadCloser, error) {
	repo, err := e.OpenRecordRepository(ctx, r)
	if err != nil {
		return nil, err
	}
	return repo.OpenPayload(ctx, r, e.Keys)
}

// runTool runs a native tool with stdin and waits; output goes to the log.
func (e *Env) runTool(ctx context.Context, inv Invocation, stdin io.Reader) error {
	p, err := Start(ctx, e.Getenv, inv, stdin, e.Log)
	if err != nil {
		return err
	}
	defer p.Close()
	if e.Log != nil {
		_, _ = io.Copy(e.Log, p)
	} else {
		_, _ = io.Copy(io.Discard, p)
	}
	return p.Wait()
}

func (e *Env) tmpDir() (string, error) {
	return os.MkdirTemp(e.Getenv("STRAND_BACKUP_TMPDIR"), "strand-restore-*")
}

// stageLogs decodes log segments into files of dir, returning their paths in order.
func (e *Env) stageLogs(ctx context.Context, logs []backup.Record, dir string, name func(backup.Record) string) ([]string, error) {
	var out []string
	for _, l := range logs {
		body, err := e.openRecord(ctx, l)
		if err != nil {
			return nil, err
		}
		p := filepath.Join(dir, name(l))
		f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, err = io.Copy(f, body)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}
		body.Close()
		if err != nil {
			return nil, fmt.Errorf("stage log %s: %w", l.ID, err)
		}
		out = append(out, p)
	}
	return out, nil
}

func (e *Env) restoreMySQL(ctx context.Context, plan RestorePlan, t RestoreTarget) error {
	base := plan.Base()
	dbs := strings.Split(base.Details["databases"], ",")
	if t.Database == "" || len(dbs) != 1 || dbs[0] == "" {
		return errors.New("a logical MySQL restore needs --target-database and a single-database dump")
	}
	src := dbs[0]
	db, err := sql.Open("mysql", mysqlDSN(e.Getenv, t.Conn))
	if err != nil {
		return err
	}
	defer db.Close()
	var exists int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", t.Database).Scan(&exists); err != nil {
		return fmt.Errorf("connect to the restore target: %v", redactAll(err.Error(), []string{password(e.Getenv, t.Conn)}))
	}
	if exists != 0 {
		return fmt.Errorf("target database %s already exists; restores never overwrite", t.Database)
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE `"+strings.ReplaceAll(t.Database, "`", "``")+"`"); err != nil {
		return err
	}
	body, err := e.openRecord(ctx, base)
	if err != nil {
		return err
	}
	client := mysqlClient(e.Getenv, t)
	err = e.runTool(ctx, client, NewGTIDPurgedFilter(body))
	body.Close()
	if err != nil {
		return fmt.Errorf("load dump: %w", err)
	}
	if len(plan.Logs) == 0 {
		e.logf("restore: dump of %s loaded into %s", src, t.Database)
		return nil
	}
	dir, err := e.tmpDir()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	_ = os.Chmod(dir, 0o755)
	files, err := e.stageLogs(ctx, plan.Logs, dir, func(l backup.Record) string { return l.Details["seq"] + "-" + l.Details["server_file"] })
	if err != nil {
		return err
	}
	// Transactions already in the dump are excluded by GTID; GTIDs are stripped so the replay is not
	// skipped on a server that executed them (e.g. a new database next to the source).
	args := []string{"--exclude-gtids=" + base.Position.GTIDSet, "--skip-gtids", "--database=" + t.Database,
		"--rewrite-db=" + src + "->" + t.Database}
	if plan.Target != nil {
		args = append(args, "--stop-datetime="+plan.Target.UTC().Format("2006-01-02 15:04:05"))
	}
	args = append(args, files...)
	binlog, err := Start(ctx, e.Getenv, Invocation{Tool: "mysqlbinlog", Args: args}, nil, e.Log)
	if err != nil {
		return err
	}
	defer binlog.Close()
	if err := e.runTool(ctx, client, binlog); err != nil {
		return fmt.Errorf("replay binlogs: %w", err)
	}
	if err := binlog.Wait(); err != nil {
		return err
	}
	e.logf("restore: %s restored into %s up to %v", src, t.Database, plan.Target)
	return nil
}

func mysqlClient(getenv Getenv, t RestoreTarget) Invocation {
	inv := Invocation{Tool: "mysql", Args: append(mysqlConnArgs(t.Conn), "--binary-mode", "--default-character-set=utf8mb4", "--database="+t.Database)}
	if pw := password(getenv, t.Conn); pw != "" {
		inv.Env = append(inv.Env, "MYSQL_PWD="+pw)
	}
	return inv
}

func (e *Env) restorePGDump(ctx context.Context, base backup.Record, t RestoreTarget) error {
	if t.Database == "" {
		return errors.New("a pg_dump restore needs --target-database")
	}
	admin, err := pgconn.Connect(ctx, PGConnString(e.Getenv, t.Conn, "postgres", false))
	if err != nil {
		return fmt.Errorf("connect to the restore target: %v", redactAll(err.Error(), []string{password(e.Getenv, t.Conn)}))
	}
	defer admin.Close(context.Background())
	res, err := admin.Exec(ctx, "SELECT 1 FROM pg_database WHERE datname = '"+strings.ReplaceAll(t.Database, "'", "''")+"'").ReadAll()
	if err != nil {
		return err
	}
	if len(res) > 0 && len(res[0].Rows) > 0 {
		return fmt.Errorf("target database %s already exists; restores never overwrite", t.Database)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+strings.ReplaceAll(t.Database, `"`, `""`)+`"`).ReadAll(); err != nil {
		return err
	}
	body, err := e.openRecord(ctx, base)
	if err != nil {
		return err
	}
	defer body.Close()
	args := []string{"--host=" + t.Conn.Host, "--dbname=" + t.Database, "--no-owner", "--no-privileges", "--exit-on-error", "--no-password"}
	if t.Conn.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(t.Conn.Port))
	}
	if t.Conn.User != "" {
		args = append(args, "--username="+t.Conn.User)
	}
	inv := Invocation{Tool: "pg_restore", Args: args}
	if pw := password(e.Getenv, t.Conn); pw != "" {
		inv.Env = []string{"PGPASSWORD=" + pw}
	}
	return e.runTool(ctx, inv, body)
}

// newDir checks that dir does not exist or is empty, and creates it 0700.
func newDir(dir string) error {
	if dir == "" || !filepath.IsAbs(dir) {
		return errors.New("--target-dir must be an absolute path")
	}
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		return fmt.Errorf("target directory %s is not empty; restores never overwrite", dir)
	}
	return os.MkdirAll(dir, 0o700)
}

// untar extracts a tar stream into dir, refusing paths that escape it and links.
func untar(r io.Reader, dir string) error {
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(h.Name)
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return fmt.Errorf("unsafe path in archive: %q", h.Name)
		}
		p := filepath.Join(dir, name)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(p, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
				return err
			}
			f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, err = io.Copy(f, tr)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		case tar.TypeSymlink:
			// pg_basebackup writes pg_wal as a directory in tar mode; tablespace links are refused.
			return fmt.Errorf("symlink %q in archive is not supported (tablespaces)", h.Name)
		}
	}
}

func (e *Env) restorePGBase(ctx context.Context, plan RestorePlan, t RestoreTarget) error {
	base := plan.Base()
	if err := newDir(t.Dir); err != nil {
		return err
	}
	body, err := e.openRecord(ctx, base)
	if err != nil {
		return err
	}
	err = untar(body, t.Dir)
	if cerr := body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("extract base backup: %w", err)
	}
	if len(plan.Logs) == 0 {
		e.logf("restore: base backup extracted to %s (consistent at its end LSN %s); start PostgreSQL on it", t.Dir, base.EndPosition.LSN)
		return nil
	}
	walDir := filepath.Join(t.Dir, "strand_wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		return err
	}
	if _, err := e.stageLogs(ctx, plan.Logs, walDir, func(l backup.Record) string { return l.Details["segment"] }); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(t.Dir, "postgresql.auto.conf"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	conf := "\n# added by strand backup restore\nrestore_command = 'cp strand_wal/%f \"%p\"'\nrecovery_target_action = 'promote'\n"
	if plan.Target != nil {
		conf += "recovery_target_time = '" + plan.Target.UTC().Format("2006-01-02 15:04:05.999999") + "+00'\n"
	}
	if _, err := f.WriteString(conf); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(t.Dir, "recovery.signal"), nil, 0o600); err != nil {
		return err
	}
	e.logf("restore: data directory %s prepared with %d WAL segment(s); starting PostgreSQL on it recovers to %v and promotes", t.Dir, len(plan.Logs), plan.Target)
	return nil
}

func (e *Env) restoreMongo(ctx context.Context, plan RestorePlan, t RestoreTarget) error {
	base := plan.Base()
	uri, err := MongoURI(e.Getenv, t.Conn)
	if err != nil {
		return err
	}
	dir, err := e.tmpDir()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	_ = os.Chmod(dir, 0o755)
	cfg := map[string]string{"mongo.yaml": "uri: " + strconv.Quote(uri) + "\n"}
	args := []string{"--config={dir}/mongo.yaml", "--archive"}
	dbs := base.Details["databases"]
	switch {
	case dbs != "" && t.Database != "":
		args = append(args, "--nsFrom="+dbs+".*", "--nsTo="+t.Database+".*")
	case dbs == "" && t.Database != "":
		return errors.New("a whole-deployment dump (with oplog) restores into a NEW deployment: omit --target-database")
	default:
		args = append(args, "--oplogReplay")
	}
	body, err := e.openRecord(ctx, base)
	if err != nil {
		return err
	}
	err = e.runTool(ctx, Invocation{Tool: "mongorestore", Args: args, Files: cfg}, body)
	body.Close()
	if err != nil {
		return fmt.Errorf("mongorestore: %w", err)
	}
	if len(plan.Logs) == 0 {
		return nil
	}
	if dbs != "" {
		return errors.New("oplog replay needs a whole-deployment dump (mongodump --oplog)")
	}
	replay := filepath.Join(dir, "replay")
	if err := os.MkdirAll(replay, 0o755); err != nil {
		return err
	}
	out, err := os.Create(filepath.Join(replay, "oplog.bson"))
	if err != nil {
		return err
	}
	for _, l := range plan.Logs {
		body, err := e.openRecord(ctx, l)
		if err != nil {
			out.Close()
			return err
		}
		_, err = io.Copy(out, body)
		body.Close()
		if err != nil {
			out.Close()
			return err
		}
	}
	if err := out.Close(); err != nil {
		return err
	}
	_ = os.Chmod(filepath.Join(replay, "oplog.bson"), 0o644)
	rargs := []string{"--config={dir}/mongo.yaml", "--oplogReplay", "--dir=" + replay}
	if plan.Target != nil {
		rargs = append(rargs, fmt.Sprintf("--oplogLimit=%d:0", plan.Target.Unix()+1))
	}
	return e.runTool(ctx, Invocation{Tool: "mongorestore", Args: rargs, Files: cfg}, nil)
}

func (e *Env) restoreXtrabackup(ctx context.Context, plan RestorePlan, t RestoreTarget) error {
	if err := newDir(t.Dir); err != nil {
		return err
	}
	work, err := e.tmpDir()
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	_ = os.Chmod(work, 0o777)
	tool := plan.Base().Method
	var dirs []string
	for i, r := range plan.Chain {
		d := filepath.Join(work, fmt.Sprintf("%02d", i))
		if err := os.MkdirAll(d, 0o777); err != nil {
			return err
		}
		body, err := e.openRecord(ctx, r)
		if err != nil {
			return err
		}
		err = e.runTool(ctx, Invocation{Tool: "xbstream", Args: []string{"-x", "-C", d}}, body)
		body.Close()
		if err != nil {
			return fmt.Errorf("unpack %s: %w", r.ID, err)
		}
		dirs = append(dirs, d)
	}
	for i := range dirs {
		args := []string{"--prepare", "--target-dir=" + dirs[0]}
		if i < len(dirs)-1 {
			args = append(args, "--apply-log-only")
		}
		if i > 0 {
			args = append(args, "--incremental-dir="+dirs[i])
		}
		if err := e.runTool(ctx, Invocation{Tool: tool, Args: args}, nil); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}
	if err := os.Remove(t.Dir); err != nil {
		return err
	}
	if err := os.Rename(dirs[0], t.Dir); err != nil {
		return fmt.Errorf("move prepared data directory: %w", err)
	}
	e.logf("restore: prepared MySQL data directory at %s (%d backup(s) in the chain)", t.Dir, len(dirs))
	return nil
}

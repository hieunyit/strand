package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/backup"
)

var validRestoreObjectNameRE = regexp.MustCompile(`^[A-Za-z0-9_$.-]+$`)

// ValidateRestoreObjects checks that restore object names are valid and count does not exceed 1000.
func ValidateRestoreObjects(objects []string) error {
	if len(objects) > 1000 {
		return errors.New("too many objects: maximum is 1000")
	}
	for _, o := range objects {
		o = strings.TrimSpace(o)
		if o == "" || !validRestoreObjectNameRE.MatchString(o) {
			return fmt.Errorf("invalid object name %q: must match [A-Za-z0-9_$.-]", o)
		}
	}
	return nil
}

// ValidateRestoreThreads checks that threads is in range 0..16.
func ValidateRestoreThreads(threads int) error {
	if threads < 0 || threads > 16 {
		return errors.New("threads must be between 1 and 16 (0 = default)")
	}
	return nil
}

// RestoreTarget is where a restore goes. It is always NEW: logical restores create Database (which must
// not exist) on the target server; physical restores fill Dir (which must not exist or be empty). The
// source database is never written.
type RestoreTarget struct {
	Conn     backup.ConnSpec
	Database string
	Dir      string
	Threads  int
	Objects  []string
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
		if t.Threads > 0 {
			detail["threads"] = t.Threads
		}
		if len(t.Objects) > 0 {
			detail["object_count"] = len(t.Objects)
			if len(t.Objects) <= 20 {
				detail["objects"] = t.Objects
			}
		}
		err = e.audit(context.WithoutCancel(ctx), "backup.restore", err, detail)
	}()

	if err := ValidateRestoreThreads(t.Threads); err != nil {
		return err
	}
	if err := ValidateRestoreObjects(t.Objects); err != nil {
		return err
	}

	if t.Dir != "" || base.Method == backup.MethodPGBaseBackup {
		if t.Threads > 0 {
			return errors.New("parallel restore is not supported for physical backups")
		}
		if len(t.Objects) > 0 {
			return errors.New("partial restore is not supported for physical backups")
		}
	}

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

func (e *Env) restoreMySQL(ctx context.Context, plan RestorePlan, t RestoreTarget) (err error) {
	base := plan.Base()
	if t.Threads > 1 {
		return errors.New("parallel restore is not available for mysqldump backups")
	}
	if len(t.Objects) > 0 && (plan.Target != nil || len(plan.Logs) > 0) {
		return errors.New("partial restore with point-in-time recovery is not supported for MySQL backups")
	}
	dbs := strings.Split(base.Details["databases"], ",")
	if t.Database == "" || len(dbs) != 1 || dbs[0] == "" {
		return errors.New("a logical MySQL restore needs --target-database and a single-database dump")
	}
	if base.Details["objects"] != "" && len(t.Objects) > 0 {
		knownTables := strings.Split(base.Details["objects"], ",")
		knownMap := make(map[string]bool, len(knownTables))
		for _, k := range knownTables {
			knownMap[strings.TrimSpace(k)] = true
		}
		for _, obj := range t.Objects {
			if !knownMap[strings.TrimSpace(obj)] {
				return fmt.Errorf("table %q not found in backup", obj)
			}
		}
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
	createdDB := true
	defer func() {
		if err != nil && createdDB {
			cctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_, _ = db.ExecContext(cctx, "DROP DATABASE IF EXISTS `"+strings.ReplaceAll(t.Database, "`", "``")+"`")
		}
	}()

	body, err := e.openRecord(ctx, base)
	if err != nil {
		return err
	}
	defer body.Close()

	client := mysqlClient(e.Getenv, t)
	var stream io.Reader
	var partialFilter *mysqlPartialFilter
	if len(t.Objects) > 0 {
		partialFilter = NewMySQLPartialFilter(body, t.Objects)
		stream = NewGTIDPurgedFilter(partialFilter)
	} else {
		stream = NewGTIDPurgedFilter(body)
	}

	err = e.runTool(ctx, client, stream)
	if err != nil {
		return fmt.Errorf("load dump: %w", err)
	}
	if partialFilter != nil && partialFilter.Err() != nil {
		return partialFilter.Err()
	}

	if len(plan.Logs) == 0 {
		createdDB = false
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
	createdDB = false
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

func (e *Env) restorePGDump(ctx context.Context, base backup.Record, t RestoreTarget) (err error) {
	if t.Database == "" {
		return errors.New("a pg_dump restore needs --target-database")
	}

	var archiveFile string
	var listFile string
	dir, err := e.tmpDir()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	_ = os.Chmod(dir, 0o700)

	needStage := t.Threads > 1 || len(t.Objects) > 0
	if needStage {
		archiveFile = filepath.Join(dir, "archive.dump")
		f, err := os.OpenFile(archiveFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("stage pg_dump archive: %w", err)
		}
		body, err := e.openRecord(ctx, base)
		if err != nil {
			f.Close()
			return err
		}
		_, copyErr := io.Copy(f, body)
		closeErr := f.Close()
		body.Close()
		if copyErr != nil {
			if errors.Is(copyErr, syscall.ENOSPC) || strings.Contains(strings.ToLower(copyErr.Error()), "no space") {
				return fmt.Errorf("stage pg_dump archive: disk full: %w", copyErr)
			}
			return fmt.Errorf("stage pg_dump archive: %w", copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("stage pg_dump archive: %w", closeErr)
		}

		if len(t.Objects) > 0 {
			// Extract TOC before creating anything
			var tocBuf bytes.Buffer
			tocProc, err := Start(ctx, e.Getenv, Invocation{Tool: "pg_restore", Args: []string{"-l", archiveFile}}, nil, nil)
			if err != nil {
				return fmt.Errorf("read pg_dump TOC: %w", err)
			}
			_, _ = io.Copy(&tocBuf, tocProc)
			tocProc.Close()
			if err := tocProc.Wait(); err != nil {
				return fmt.Errorf("read pg_dump TOC: %w", err)
			}

			// Extract schema metadata (sequence ownership, indexes, FK constraints)
			var schemaBuf bytes.Buffer
			schemaProc, err := Start(ctx, e.Getenv, Invocation{Tool: "pg_restore", Args: []string{"-s", "-f", "-", archiveFile}}, nil, nil)
			if err != nil {
				return fmt.Errorf("read pg_dump schema metadata: %w", err)
			}
			_, _ = io.Copy(&schemaBuf, schemaProc)
			schemaProc.Close()
			if err := schemaProc.Wait(); err != nil {
				return fmt.Errorf("read pg_dump schema metadata: %w", err)
			}
			info, err := parsePGSchemaMetadata(schemaBuf.Bytes())
			if err != nil {
				return fmt.Errorf("parse pg_dump schema metadata: %w", err)
			}

			filteredTOC, warnings, err := FilterPGTOC(tocBuf.Bytes(), t.Objects, info)
			if err != nil {
				return err
			}
			for _, w := range warnings {
				e.logf("warning: %s", w)
			}
			listFile = filepath.Join(dir, "filtered.list")
			if err := os.WriteFile(listFile, filteredTOC, 0o600); err != nil {
				return fmt.Errorf("write filtered TOC list: %w", err)
			}
		}
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
	createdDB := true
	defer func() {
		if err != nil && createdDB {
			_, _ = admin.Exec(context.Background(), `DROP DATABASE IF EXISTS "`+strings.ReplaceAll(t.Database, `"`, `""`)+`" WITH (FORCE)`).ReadAll()
		}
	}()

	args := []string{"--host=" + t.Conn.Host, "--dbname=" + t.Database, "--no-owner", "--no-privileges", "--exit-on-error", "--no-password"}
	if t.Conn.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(t.Conn.Port))
	}
	if t.Conn.User != "" {
		args = append(args, "--username="+t.Conn.User)
	}
	if t.Threads > 1 {
		args = append(args, fmt.Sprintf("-j%d", t.Threads))
	}
	if listFile != "" {
		args = append(args, "-L", listFile)
	}

	var stdin io.Reader
	if needStage {
		args = append(args, archiveFile)
	} else {
		body, err := e.openRecord(ctx, base)
		if err != nil {
			return err
		}
		defer body.Close()
		stdin = body
	}

	inv := Invocation{Tool: "pg_restore", Args: args}
	if pw := password(e.Getenv, t.Conn); pw != "" {
		inv.Env = []string{"PGPASSWORD=" + pw}
	}
	if err := e.runTool(ctx, inv, stdin); err != nil {
		return err
	}
	createdDB = false
	return nil
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
	dbs := base.Details["databases"]

	if dbs == "" && len(t.Objects) > 0 {
		return errors.New("partial restore is not supported for whole-deployment MongoDB backups")
	}

	if base.Details["objects"] != "" && len(t.Objects) > 0 {
		knownColls := strings.Split(base.Details["objects"], ",")
		knownMap := map[string]bool{}
		for _, k := range knownColls {
			knownMap[k] = true
		}
		for _, obj := range t.Objects {
			if !knownMap[obj] {
				return fmt.Errorf("collection %q not found in backup", obj)
			}
		}
	}

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

	if t.Threads > 0 {
		args = append(args, fmt.Sprintf("--numParallelCollections=%d", t.Threads))
		workers := t.Threads / 2
		if workers < 1 {
			workers = 1
		}
		args = append(args, fmt.Sprintf("--numInsertionWorkersPerCollection=%d", workers))
	}

	switch {
	case dbs != "" && t.Database != "":
		if len(t.Objects) > 0 {
			for _, obj := range t.Objects {
				args = append(args, fmt.Sprintf("--nsInclude=%s.%s", dbs, obj))
			}
		}
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

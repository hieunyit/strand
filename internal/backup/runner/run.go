package runner

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
)

// Env is what a managed backup operation needs.
type Env struct {
	Getenv  Getenv
	Catalog *backup.Catalog
	// Repo looks up a repository definition by id.
	Repo func(ctx context.Context, id string) (backup.Repo, bool, error)
	Keys *backup.Keyring
	Log  io.Writer
	// Audit records an operation; a failure makes the operation fail (fail-closed).
	Audit func(ctx context.Context, action, result string, detail map[string]any) error
	Now   func() time.Time
	// ReadBack re-reads and decodes every new payload before completing it (default true).
	NoReadBack bool
}

func (e *Env) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC()
	}
	return time.Now().UTC()
}

func (e *Env) logf(format string, args ...any) {
	if e.Log != nil {
		fmt.Fprintf(e.Log, format+"\n", args...)
	}
}

func (e *Env) audit(ctx context.Context, action string, err error, detail map[string]any) error {
	if e.Audit == nil {
		return err
	}
	result := "ok"
	if err != nil {
		result, detail["error"] = "error", err.Error()
	}
	if aerr := e.Audit(ctx, action, result, detail); aerr != nil {
		return errors.Join(err, aerr)
	}
	return err
}

// NewID returns a new backup id with a prefix.
func NewID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

// OpenJobRepository opens the repository of a job (its repository row, else the default S3 + bucket).
func (e *Env) OpenJobRepository(ctx context.Context, repoID, bucket, prefix string) (*Repository, error) {
	var def *backup.Repo
	if repoID != "" {
		r, ok, err := e.Repo(ctx, repoID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("backup repository %q does not exist", repoID)
		}
		def = &r
		if bucket != "" && bucket != r.Bucket {
			def.Bucket = bucket
		}
	}
	return OpenRepository(e.Getenv, def, bucket, prefix, 0, 0)
}

// OpenRecordRepository opens the repository holding rec.
func (e *Env) OpenRecordRepository(ctx context.Context, rec backup.Record) (*Repository, error) {
	return e.OpenJobRepository(ctx, rec.Repository, rec.Bucket, "")
}

func (e *Env) encodeOptions(job backup.Job) (backup.EncodeOptions, error) {
	opt := backup.EncodeOptions{Compress: job.Compress, Level: job.CompressLevel}
	if job.Encrypt {
		if e.Keys == nil || e.Keys.Active == nil {
			return opt, errors.New("job requires encryption but no key is configured (STRAND_BACKUP_ENCRYPTION_KEYS)")
		}
		k := *e.Keys.Active
		opt.Key = &k
	}
	return opt, nil
}

func safeSegment(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		return "source"
	}
	return out
}

// payloadName is the object name of a method's payload.
func payloadName(method, format string) string {
	name := map[string]string{
		backup.MethodMySQLDump: "dump.sql", backup.MethodMariaDBDump: "dump.sql", backup.MethodPGDump: "dump.pgdump",
		backup.MethodPGBaseBackup: "base.tar", backup.MethodMongodump: "dump.archive", backup.MethodMydumper: "mydumper.tar",
	}[method]
	if name == "" {
		name = "artifact"
	}
	if format == backup.FormatSBK1 {
		name += ".sbk"
	}
	return name
}

// RunJob takes one backup with a built-in method and returns its complete catalog record.
func (e *Env) RunJob(ctx context.Context, job backup.Job) (rec backup.Record, err error) {
	if job.IsCustom() {
		return backup.Record{}, errors.New("RunJob handles built-in methods; custom jobs run their wrapper")
	}
	if backup.IsUnsupportedMethod(job.Method) {
		return backup.Record{}, fmt.Errorf("method %q is unsupported (removed: runs on the database host)", job.Method)
	}
	// Resolve the native tool before touching the repository or the catalog: a missing tool must not leave
	// a failed restore point behind.
	if err := CheckTools(e.Getenv, BackupToolNeeds(job.Method)); err != nil {
		return backup.Record{}, err
	}
	repo, err := e.OpenJobRepository(ctx, job.Repository, job.Bucket, job.Prefix)
	if err != nil {
		return backup.Record{}, err
	}
	enc, err := e.encodeOptions(job)
	if err != nil {
		return backup.Record{}, err
	}
	id := NewID("bkp")
	kind, parent := backup.KindFull, ""
	key := repo.Key(safeSegment(job.Source), id, payloadName(job.Method, enc.Format()))
	rec = backup.Record{ID: id, Source: job.Source, Engine: job.Engine(), Tool: job.Method, Bucket: repo.Bucket,
		ObjectKey: key, ManifestURI: repo.URI(repo.Key(safeSegment(job.Source), id, "manifest.json")),
		Status: backup.StatusQueued, CreatedAt: e.now(), JobID: job.ID, Repository: repo.ID, Method: job.Method,
		Kind: kind, ParentID: parent}
	defer func() {
		err = e.audit(context.WithoutCancel(ctx), "backup.run", err, map[string]any{"backup_id": id, "job": job.ID,
			"source": job.Source, "method": job.Method, "kind": kind, "object": repo.URI(key)})
	}()
	if _, _, err := e.Catalog.Create(ctx, rec); err != nil {
		return backup.Record{}, fmt.Errorf("create catalog record: %w", err)
	}
	fail := func(cause error) (backup.Record, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _, _ = e.Catalog.UpdateStatus(fctx, id, backup.StatusFailed, nil)
		return backup.Record{}, cause
	}
	if _, _, err := e.Catalog.UpdateStatus(ctx, id, backup.StatusUploading, nil); err != nil {
		return fail(err)
	}
	e.logf("backup %s: %s %s backup of %s to %s", id, job.Method, kind, job.Source, repo.URI(key))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := e.now()
	meta, res, err := e.stream(runCtx, job, repo, key, enc)
	if err != nil {
		return fail(err)
	}
	ended := e.now()
	meta.StartTime, meta.EndTime = &started, &ended
	if meta.Details == nil {
		meta.Details = map[string]string{}
	}
	meta.Details["duration_ms"] = fmt.Sprint(e.now().Sub(started).Milliseconds())
	meta.Format, meta.PlainBytes, meta.PlainSHA256, meta.VersionID = res.Format, res.Plain.PlainBytes, res.Plain.PlainSHA256, res.Object.VersionID
	if enc.Compress {
		meta.Compression = backup.CompressionZstd
	}
	if enc.Key != nil {
		meta.KeyID = enc.Key.ID
	}
	if res.Lock.LockMode != "" {
		t := res.Lock.RetainUntil
		meta.LockMode, meta.RetainUntil = res.Lock.LockMode, &t
	}
	if rec, _, err = e.Catalog.UpdateMeta(ctx, id, rec.Engine, meta); err != nil {
		return fail(fmt.Errorf("record backup metadata: %w", err))
	}
	return e.finalize(ctx, repo, rec, res.Object.Size, res.Object.SHA256, job.Retention.LockDays, fail)
}

// finalize verifies the stored payload, writes the manifest sidecar and completes the record.
func (e *Env) finalize(ctx context.Context, repo *Repository, rec backup.Record, size int64, sha string, lockDays int,
	fail func(error) (backup.Record, error)) (backup.Record, error) {
	if _, _, err := e.Catalog.UpdateStatus(ctx, rec.ID, backup.StatusVerifying, nil); err != nil {
		return fail(err)
	}
	head, err := repo.Store.HeadVersion(ctx, rec.ObjectKey, rec.VersionID)
	if err != nil {
		return fail(fmt.Errorf("verify uploaded payload: %w", err))
	}
	if head.Size != size || (head.SHA256 != "" && !strings.EqualFold(head.SHA256, sha)) {
		return fail(errors.New("verify uploaded payload: size or SHA-256 metadata differs"))
	}
	if !e.NoReadBack {
		check := rec
		check.Bytes, check.SHA256 = size, sha
		body, err := repo.OpenPayload(ctx, check, e.Keys)
		if err == nil {
			_, err = io.Copy(io.Discard, body)
			body.Close()
		}
		if err != nil {
			return fail(fmt.Errorf("read-back verification: %w", err))
		}
	}
	manifest := rec
	manifest.Bytes, manifest.SHA256, manifest.Status = size, sha, backup.StatusComplete
	manifestKey := strings.TrimPrefix(rec.ManifestURI, "s3://"+rec.Bucket+"/")
	if manifestKey == rec.ObjectKey {
		// Log segments are self-describing (the catalog row is their manifest): no sidecar.
		out, _, err := e.Catalog.Finalize(ctx, rec.ID, backup.StatusVerifying, size, sha, rec.ManifestURI, e.now())
		if err != nil {
			return fail(fmt.Errorf("finalize catalog record: %w", err))
		}
		return out, nil
	}
	minfo, err := repo.PutJSON(ctx, manifestKey, manifest, lockDays)
	if err != nil {
		return fail(fmt.Errorf("upload manifest sidecar: %w", err))
	}
	if minfo.VersionID != "" {
		m := backup.MetaOf(rec)
		if m.Details == nil {
			m.Details = map[string]string{}
		}
		m.Details["manifest_version"] = minfo.VersionID
		if rec, _, err = e.Catalog.UpdateMeta(ctx, rec.ID, rec.Engine, m); err != nil {
			return fail(err)
		}
	}
	out, _, err := e.Catalog.Finalize(ctx, rec.ID, backup.StatusVerifying, size, sha, rec.ManifestURI, e.now())
	if err != nil {
		return fail(fmt.Errorf("finalize catalog record: %w", err))
	}
	e.logf("backup %s complete: %d bytes stored (%d plaintext)", rec.ID, size, rec.PlainBytes)
	return out, nil
}

// stream runs the method's tool and stores its output.
func (e *Env) stream(ctx context.Context, job backup.Job, repo *Repository, key string, enc backup.EncodeOptions) (backup.Meta, PayloadResult, error) {
	var meta backup.Meta
	var inv Invocation
	var err error
	var mongoURI string
	switch job.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		ver, _ := RunCapture(ctx, e.Getenv, job.Method, "--version")
		flavor := ParseDumpVersion(ver)
		if job.Method == backup.MethodMariaDBDump {
			flavor.MariaDB = true
		}
		inv = MySQLDumpInvocation(e.Getenv, job.Method, job.Conn, flavor)
		meta.Details = map[string]string{"tool_version": strings.TrimSpace(ver)}
	case backup.MethodPGDump:
		inv, err = PGDumpInvocation(e.Getenv, job.Conn, enc.Compress)
	case backup.MethodPGBaseBackup:
		inv = PGBaseBackupInvocation(e.Getenv, job.Conn)
	case backup.MethodMongodump:
		if mongoURI, err = MongoURI(e.Getenv, job.Conn); err == nil {
			inv, err = MongodumpInvocation(mongoURI, job.Conn)
		}
		if err == nil && len(job.Conn.Databases) == 0 {
			ts, terr := LatestOplogTS(ctx, mongoURI)
			if terr != nil {
				return meta, PayloadResult{}, terr
			}
			meta.Position.OplogTS = FormatOplogTS(ts)
		}
	case backup.MethodMydumper:
		return e.streamMydumper(ctx, job, repo, key, enc)
	default:
		return meta, PayloadResult{}, fmt.Errorf("unsupported method %q", job.Method)
	}
	if err != nil {
		return meta, PayloadResult{}, err
	}
	proc, err := Start(ctx, e.Getenv, inv, nil, e.Log)
	if err != nil {
		return meta, PayloadResult{}, err
	}
	defer proc.Close()
	tap := &headTap{r: proc, max: 4 << 20}
	res, err := repo.StorePayload(ctx, key, tap, enc, job.Retention.LockDays, job.BandwidthKBps)
	if err != nil {
		return meta, res, err
	}
	if err := proc.Wait(); err != nil {
		return meta, res, err
	}
	if meta.Details == nil {
		meta.Details = map[string]string{}
	}
	if len(job.Conn.Databases) > 0 {
		meta.Details["databases"] = strings.Join(job.Conn.Databases, ",")
	}
	switch job.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		meta.Position = ParseMySQLDumpHeader(tap.buf.Bytes())
		if meta.Position.GTIDSet == "" && meta.Position.BinlogFile == "" {
			e.logf("warning: the dump contains no binlog position (binary logging disabled?); it cannot anchor point-in-time recovery")
		}
	case backup.MethodPGBaseBackup:
		start, end, tl := ParsePGBaseBackupLog(proc.Stderr())
		meta.Position, meta.EndPosition = start, end
		if tl != "" {
			meta.Details["timeline"] = tl
		}
	case backup.MethodMongodump:
		if len(job.Conn.Databases) == 0 {
			if ts, err := LatestOplogTS(ctx, mongoURI); err == nil {
				meta.EndPosition.OplogTS = FormatOplogTS(ts)
			}
		}
	}
	if meta.Details["objects"] == "" && meta.Details["objects_truncated"] == "" {
		if objs := e.queryBackupObjects(ctx, job, mongoURI); len(objs) > 0 {
			storeBackupObjects(meta.Details, objs)
		}
	}
	return meta, res, nil
}

func storeBackupObjects(details map[string]string, objs []string) {
	if len(objs) == 0 {
		return
	}
	if len(objs) > 1000 {
		details["objects_truncated"] = "1"
		return
	}
	joined := strings.Join(objs, ",")
	if len(joined) > 64*1024 {
		details["objects_truncated"] = "1"
		return
	}
	details["objects"] = joined
}

func (e *Env) queryBackupObjects(ctx context.Context, job backup.Job, mongoURI string) []string {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	switch job.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		if len(job.Conn.Databases) != 1 {
			return nil
		}
		db, err := sql.Open("mysql", mysqlDSN(e.Getenv, job.Conn))
		if err != nil {
			return nil
		}
		defer db.Close()
		rows, err := db.QueryContext(cctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME", job.Conn.Databases[0])
		if err != nil {
			return nil
		}
		defer rows.Close()
		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err == nil && t != "" {
				tables = append(tables, t)
			}
		}
		return tables

	case backup.MethodPGDump:
		dbName := "postgres"
		if len(job.Conn.Databases) > 0 && job.Conn.Databases[0] != "" {
			dbName = job.Conn.Databases[0]
		}
		c, err := pgx.Connect(cctx, PGConnString(e.Getenv, job.Conn, dbName, false))
		if err != nil {
			return nil
		}
		defer c.Close(context.Background())
		rows, err := c.Query(cctx, "SELECT CASE WHEN schemaname = 'public' THEN tablename ELSE schemaname || '.' || tablename END FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname, tablename")
		if err != nil {
			return nil
		}
		defer rows.Close()
		tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil
		}
		return tables

	case backup.MethodMongodump:
		if len(job.Conn.Databases) != 1 {
			return nil
		}
		if mongoURI == "" {
			var err error
			mongoURI, err = MongoURI(e.Getenv, job.Conn)
			if err != nil {
				return nil
			}
		}
		client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
		if err != nil {
			return nil
		}
		defer client.Disconnect(context.Background())
		colls, err := client.Database(job.Conn.Databases[0]).ListCollectionNames(cctx, bson.M{})
		if err != nil {
			return nil
		}
		var filtered []string
		for _, c := range colls {
			if !strings.HasPrefix(c, "system.") {
				filtered = append(filtered, c)
			}
		}
		sort.Strings(filtered)
		return filtered
	}
	return nil
}

func (e *Env) streamMydumper(ctx context.Context, job backup.Job, repo *Repository, key string, enc backup.EncodeOptions) (backup.Meta, PayloadResult, error) {
	var meta backup.Meta
	proc, err := Start(ctx, e.Getenv, MydumperInvocation(e.Getenv, job.Conn), nil, e.Log)
	if err != nil {
		return meta, PayloadResult{}, err
	}
	defer proc.Close()
	_, _ = io.Copy(io.Discard, proc)
	if err := proc.Wait(); err != nil {
		return meta, PayloadResult{}, err
	}
	out := filepath.Join(proc.Dir(), "out")
	md, _ := os.ReadFile(filepath.Join(out, "metadata"))
	meta.Position = ParseMydumperMetadata(string(md))
	tarStream, _, err := backup.OpenArtifact(ctx, out)
	if err != nil {
		return meta, PayloadResult{}, err
	}
	defer tarStream.Close()
	res, err := repo.StorePayload(ctx, key, tarStream, enc, job.Retention.LockDays, job.BandwidthKBps)
	return meta, res, err
}

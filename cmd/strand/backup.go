package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/seed"
	"strand/internal/web"
	"strand/internal/web/store"
)

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Manage centralized database backups"}
	cmd.AddCommand(newBackupPushCmd(), newBackupListCmd(), newBackupFetchCmd())
	return cmd
}

func backupUsageErr(msg string) error {
	return exitcode.New("invalid_arguments", exitcode.InvalidSyntax, msg, nil)
}

// backupObjectStore builds the S3 client from STRAND_BACKUP_S3_* variables. Configuration errors never
// echo values (the endpoint may carry user info; the keys are secrets).
func backupObjectStore(bucket string, partSizeMiB, concurrency int) (*objectstore.S3, error) {
	insecure := strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL"))
	obj, err := objectstore.New(objectstore.Config{
		Endpoint: strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_ENDPOINT")), Bucket: bucket,
		Region:             strings.TrimSpace(os.Getenv("STRAND_BACKUP_S3_REGION")),
		AccessKey:          os.Getenv("STRAND_BACKUP_S3_ACCESS_KEY"),
		SecretKey:          os.Getenv("STRAND_BACKUP_S3_SECRET_KEY"),
		SessionToken:       os.Getenv("STRAND_BACKUP_S3_SESSION_TOKEN"),
		AllowInsecureLocal: strings.EqualFold(insecure, "true") || insecure == "1",
		PartSize:           int64(partSizeMiB) << 20,
		Concurrency:        concurrency,
		TempDir:            os.Getenv("STRAND_BACKUP_TMPDIR"),
	})
	if err != nil {
		return nil, exitcode.New("invalid_arguments", exitcode.InvalidSyntax,
			"configure backup object storage: invalid settings; check STRAND_BACKUP_S3_ENDPOINT (https, or loopback http with STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL=true), STRAND_BACKUP_S3_REGION, credential variables, --part-size-mib (5-1024) and --upload-concurrency (1-32)", nil)
	}
	return obj, nil
}

// openBackupCatalog opens the Web metadata database (required: the catalog lives there).
func openBackupCatalog(ctx context.Context) (store.Store, *backup.Catalog, error) {
	catalogCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := web.CLIStore(catalogCtx, cfgPath, nil)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("open backup metadata store: %s", store.Redact(err.Error()))
	}
	if st == nil {
		return nil, nil, backupUsageErr("backup metadata database is required; provide --config web.yaml with database.dsn or set STRAND_WEB_DATABASE_DSN")
	}
	catalog, err := backup.NewCatalog(st)
	if err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("initialize backup catalog: %w", err)
	}
	return st, catalog, nil
}

// auditBackup appends a row to the Web audit log (same table the Web UI shows). Backup operations are
// fail-closed on audit: a caller that cannot record the operation reports failure.
func auditBackup(ctx context.Context, st store.Store, action, result string, detail map[string]any) error {
	e := web.AuditEntry{Time: time.Now().UTC(), IP: "cli", Action: action, Result: result, Detail: detail}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := st.AppendAudit(actx, store.AuditRow{Time: e.Time, IP: e.IP, Action: e.Action, Result: e.Result, Data: b}); err != nil {
		return fmt.Errorf("record audit entry %s: %s", action, store.Redact(err.Error()))
	}
	return nil
}

func newBackupPushCmd() *cobra.Command {
	var manifestPath, artifactPath, bucket, prefix, id string
	var partSizeMiB, concurrency int
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Upload a backup artifact and register it in the central catalog",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()

			if manifestPath == "" || artifactPath == "" || bucket == "" {
				return backupUsageErr("--manifest, --artifact, and --bucket are required")
			}
			manifestBytes, manifest, err := readBackupManifest(manifestPath)
			if err != nil {
				return backupUsageErr(err.Error())
			}
			position, engine, err := backupPosition(manifest)
			if err != nil {
				return backupUsageErr(err.Error())
			}
			if id == "" {
				id, err = newBackupID()
				if err != nil {
					return fmt.Errorf("generate backup ID: %w", err)
				}
			}
			cleanPrefix, err := sanitizeBackupPrefix(prefix)
			if err != nil {
				return backupUsageErr(err.Error())
			}
			artifactRoot, err := os.Lstat(artifactPath)
			if err != nil {
				return backupUsageErr(fmt.Sprintf("stat backup artifact: %v", err))
			}
			if artifactRoot.Mode()&os.ModeSymlink != 0 || (!artifactRoot.IsDir() && !artifactRoot.Mode().IsRegular()) {
				return backupUsageErr("backup artifact must be a regular file or directory, not a symlink or special file")
			}
			artifactKey, manifestKey := backupObjectKeys(cleanPrefix, id, artifactRoot.IsDir())
			manifestURI := "s3://" + bucket + "/" + manifestKey
			artifactURI := "s3://" + bucket + "/" + artifactKey

			obj, err := backupObjectStore(bucket, partSizeMiB, concurrency)
			if err != nil {
				return err
			}
			st, catalog, err := openBackupCatalog(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			record := backup.Record{
				ID: id, Source: manifest.SourceIdentity, Engine: engine,
				Tool: manifest.Kind, ToolVersion: manifest.ToolVersion,
				Bucket: bucket, ObjectKey: artifactKey, Position: position,
				ManifestURI: manifestURI, Status: backup.StatusQueued, CreatedAt: time.Now().UTC(),
				LegalHold: manifest.LegalHoldRef != nil && strings.TrimSpace(*manifest.LegalHoldRef) != "",
			}
			if manifest.BackupExpiresAt != nil && strings.TrimSpace(*manifest.BackupExpiresAt) != "" {
				expiresAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(*manifest.BackupExpiresAt))
				if parseErr != nil {
					return backupUsageErr("backup manifest backup_expires_at must be RFC3339")
				}
				expiresAt = expiresAt.UTC()
				record.ExpiresAt = &expiresAt
			}
			// Print the ID first: an interrupted push is resumed by re-running with --id <this ID>.
			fmt.Fprintf(cmd.ErrOrStderr(), "backup %s: uploading to s3://%s/%s (re-run with --id %s to retry)\n", id, bucket, artifactKey, id)
			pushErr := st.Locked(ctx, "backup-upload:"+record.ID, func(lockCtx context.Context) error {
				return pushBackupArtifact(lockCtx, cmd, obj, catalog, record, artifactPath, artifactKey, manifestKey, artifactURI, manifestURI, manifestBytes)
			})
			result, detail := "ok", map[string]any{"backup_id": id, "source": record.Source, "object": artifactURI}
			if pushErr != nil {
				result, detail["error"] = "error", store.Redact(pushErr.Error())
			}
			if err := auditBackup(ctx, st, "backup.push", result, detail); err != nil {
				return errors.Join(pushErr, err)
			}
			return pushErr
		},
	}
	cmd.Flags().IntVar(&partSizeMiB, "part-size-mib", 64, "multipart part size in MiB (5-1024); temporary disk use is part size × concurrency")
	cmd.Flags().IntVar(&concurrency, "upload-concurrency", 4, "parts uploaded in parallel (1-32)")
	cmd.Flags().StringVar(&manifestPath, "manifest", "", "seed manifest JSON path")
	cmd.Flags().StringVar(&artifactPath, "artifact", "", "backup artifact file or directory")
	cmd.Flags().StringVar(&bucket, "bucket", "", "destination S3 bucket")
	cmd.Flags().StringVar(&prefix, "prefix", "", "optional object key prefix")
	cmd.Flags().StringVar(&id, "id", "", "optional stable backup ID")
	return cmd
}

func pushBackupArtifact(ctx context.Context, cmd *cobra.Command, obj *objectstore.S3, catalog *backup.Catalog, requested backup.Record, artifactPath, artifactKey, manifestKey, artifactURI, manifestURI string, manifestBytes []byte) error {
	record, inserted, err := catalog.Create(ctx, requested)
	if err != nil {
		return fmt.Errorf("create backup catalog record: %w", err)
	}
	if !inserted && record.Status == backup.StatusComplete {
		fmt.Fprintf(cmd.OutOrStdout(), "backup %s is already complete\n", record.ID)
		return nil
	}
	if record.Status == backup.StatusUploading || record.Status == backup.StatusVerifying {
		// The advisory lock proves no uploader for this ID is still active. Recover a
		// stale in-progress record left by a crash before replacing the same S3 keys.
		_, _, err = catalog.UpdateStatus(ctx, record.ID, backup.StatusFailed, nil)
		if err != nil {
			return fmt.Errorf("recover interrupted backup upload: %w", err)
		}
		record.Status = backup.StatusFailed
	}
	if record.Status != backup.StatusQueued && record.Status != backup.StatusFailed {
		return fmt.Errorf("backup %s has unsupported status %q", record.ID, record.Status)
	}
	_, changed, err := catalog.UpdateStatus(ctx, record.ID, backup.StatusUploading, nil)
	if err != nil {
		return fmt.Errorf("start backup upload: %w", err)
	}
	if !changed {
		return fmt.Errorf("backup %s is already being processed", record.ID)
	}
	fail := func(cause error) error {
		failedCtx, failCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer failCancel()
		_, _, _ = catalog.UpdateStatus(failedCtx, record.ID, backup.StatusFailed, nil)
		return cause
	}
	// We hold the per-backup lock, so any unfinished multipart upload for these keys belongs to a
	// previous attempt that was killed before it could abort: reclaim its parts.
	for _, k := range []string{artifactKey, manifestKey} {
		if n, err := obj.AbortStaleUploads(ctx, k); err != nil {
			return fail(fmt.Errorf("abort stale multipart uploads: %w", err))
		} else if n > 0 {
			fmt.Fprintf(cmd.ErrOrStderr(), "backup %s: aborted %d unfinished multipart upload(s) from an earlier attempt\n", record.ID, n)
		}
	}
	reader, size, err := backup.OpenArtifact(ctx, artifactPath)
	if err != nil {
		return fail(fmt.Errorf("open backup artifact: %w", err))
	}
	artifactInfo, uploadErr := obj.Upload(ctx, artifactKey, reader, size, "")
	closeErr := reader.Close()
	if uploadErr != nil {
		return fail(fmt.Errorf("upload backup artifact: %w", uploadErr))
	}
	if closeErr != nil {
		return fail(fmt.Errorf("close backup artifact stream: %w", closeErr))
	}
	_, changed, err = catalog.UpdateStatus(ctx, record.ID, backup.StatusVerifying, nil)
	if err != nil {
		return fail(fmt.Errorf("start backup verification: %w", err))
	}
	if !changed {
		return fail(fmt.Errorf("backup %s left uploading state before verification", record.ID))
	}
	head, err := obj.Head(ctx, artifactKey)
	if err != nil {
		return fail(fmt.Errorf("verify uploaded backup artifact: %w", err))
	}
	if head.Size != artifactInfo.Size {
		return fail(fmt.Errorf("verify uploaded backup artifact: object size %d differs from streamed size %d", head.Size, artifactInfo.Size))
	}
	if artifactInfo.SHA256 == "" {
		return fail(errors.New("verify uploaded backup artifact: upload did not return a streamed SHA-256 digest"))
	}
	// Replace the machine-local backup path in the sidecar with the managed object
	// URI so catalog consumers never mistake the producer's local path for storage.
	managedManifest, err := setManifestBackupRef(manifestBytes, artifactURI)
	if err != nil {
		return fail(fmt.Errorf("encode managed backup manifest: %w", err))
	}
	manifestInfo, err := obj.Upload(ctx, manifestKey, bytes.NewReader(managedManifest), int64(len(managedManifest)), "")
	if err != nil {
		return fail(fmt.Errorf("upload backup manifest sidecar: %w", err))
	}
	if manifestInfo.Size != int64(len(managedManifest)) {
		return fail(errors.New("upload backup manifest sidecar: uploaded size does not match manifest size"))
	}
	verifiedAt := time.Now().UTC()
	_, _, err = catalog.Finalize(ctx, record.ID, backup.StatusVerifying, artifactInfo.Size, artifactInfo.SHA256, manifestURI, verifiedAt)
	if err != nil {
		return fail(fmt.Errorf("finalize backup catalog record: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "backup %s complete (%d bytes)\n", record.ID, artifactInfo.Size)
	return nil
}

func setManifestBackupRef(raw []byte, artifactURI string) ([]byte, error) {
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest object: %w", err)
	}
	ref, err := json.Marshal(artifactURI)
	if err != nil {
		return nil, err
	}
	manifest["backup_ref"] = ref
	return json.MarshalIndent(manifest, "", "  ")
}

func readBackupManifest(path string) ([]byte, seed.SeedManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, seed.SeedManifest{}, fmt.Errorf("read backup manifest: %w", err)
	}
	var manifest seed.SeedManifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&manifest); err != nil {
		return nil, seed.SeedManifest{}, fmt.Errorf("decode backup manifest: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, seed.SeedManifest{}, errors.New("decode backup manifest: expected exactly one JSON value")
	}
	if manifest.Version != 1 || strings.TrimSpace(manifest.SourceIdentity) == "" || strings.TrimSpace(manifest.Kind) == "" {
		return nil, seed.SeedManifest{}, errors.New("backup manifest must be v1 and include kind and source_identity")
	}
	return raw, manifest, nil
}

func backupPosition(manifest seed.SeedManifest) (backup.Position, string, error) {
	engine := strings.ToLower(strings.TrimSpace(manifest.Engine))
	if engine == "pg" {
		engine = "postgres"
	}
	switch engine {
	case "mysql":
		if manifest.StartPosition.GTIDSet == "" && (manifest.StartPosition.BinlogFile == "" || manifest.StartPosition.BinlogPos == 0) {
			return backup.Position{}, "", errors.New("MySQL backup manifest must contain a GTID set or a binlog file and positive binlog position")
		}
		if manifest.StartPosition.BinlogPos > math.MaxInt64 {
			return backup.Position{}, "", errors.New("backup manifest binlog position exceeds supported range")
		}
		return backup.Position{
			GTIDSet:    manifest.StartPosition.GTIDSet,
			BinlogFile: manifest.StartPosition.BinlogFile,
			BinlogPos:  int64(manifest.StartPosition.BinlogPos),
		}, engine, nil
	case "postgres":
		if strings.TrimSpace(manifest.StartPosition.LSN) == "" {
			return backup.Position{}, "", errors.New("PostgreSQL backup manifest must contain a start_position.lsn")
		}
		return backup.Position{LSN: manifest.StartPosition.LSN}, engine, nil
	default:
		return backup.Position{}, "", errors.New("backup manifest engine must be mysql, pg, or postgres")
	}
}

func newBackupID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "bkp-" + hex.EncodeToString(b[:]), nil
}

func sanitizeBackupPrefix(prefix string) (string, error) {
	if len(prefix) > 512 {
		return "", errors.New("--prefix must be no longer than 512 bytes")
	}
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", nil
	}
	parts := strings.Split(prefix, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		var b strings.Builder
		for _, r := range part {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
				b.WriteRune(r)
			} else {
				b.WriteByte('-')
			}
		}
		segment := strings.Trim(b.String(), ".-_ ")
		if segment != "" {
			clean = append(clean, segment)
		}
	}
	return strings.Join(clean, "/"), nil
}

func backupObjectKeys(prefix, id string, directory bool) (string, string) {
	base := id
	if prefix != "" {
		base = prefix + "/" + id
	}
	artifact := base + "/artifact"
	if directory {
		artifact += ".tar"
	}
	return artifact, base + "/manifest.json"
}

func newBackupListCmd() *cobra.Command {
	var source, engine, status, cursor string
	var limit int
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List backups in the central catalog (newest first)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit < 1 || limit > backup.MaxListLimit {
				return backupUsageErr(fmt.Sprintf("--limit must be between 1 and %d", backup.MaxListLimit))
			}
			f := backup.Filter{Source: source, Engine: engine, Status: backup.Status(status), Limit: limit}
			if cursor != "" {
				c, err := backup.ParseCursor(cursor)
				if err != nil {
					return backupUsageErr("--cursor is malformed")
				}
				f.After = &c
			}
			st, catalog, err := openBackupCatalog(cmd.Context())
			if err != nil {
				return err
			}
			defer st.Close()
			rows, err := catalog.List(cmd.Context(), f)
			if err != nil {
				if errors.Is(err, backup.ErrInvalid) {
					return backupUsageErr(err.Error())
				}
				return fmt.Errorf("list backups: %s", store.Redact(err.Error()))
			}
			next := backup.NextCursor(rows, limit)
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(map[string]any{"backups": rows, "next_cursor": next})
			}
			fmt.Fprintf(out, "%-36s %-24s %-8s %-12s %-10s %14s  %s\n", "ID", "SOURCE", "ENGINE", "TOOL", "STATUS", "BYTES", "CREATED")
			for _, r := range rows {
				fmt.Fprintf(out, "%-36s %-24s %-8s %-12s %-10s %14d  %s\n", r.ID, r.Source, r.Engine, r.Tool, r.Status, r.Bytes, r.CreatedAt.UTC().Format(time.RFC3339))
			}
			if next != "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "more backups: re-run with --cursor %s\n", next)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&source, "source", "", "filter by source identity")
	cmd.Flags().StringVar(&engine, "engine", "", "filter by engine (mysql|postgres)")
	cmd.Flags().StringVar(&status, "status", "", "filter by status (queued|uploading|verifying|complete|failed|expired)")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum rows")
	cmd.Flags().StringVar(&cursor, "cursor", "", "continue after a previous page")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

// newBackupFetchCmd downloads a complete backup to a local path, verifying its size and SHA-256 against
// the catalog before the file appears under its final name. It never overwrites an existing path and
// never touches a database: restoring is done with the native tool on the verified file.
func newBackupFetchCmd() *cobra.Command {
	var id, outPath string
	var withManifest bool
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Download a cataloged backup artifact and verify it against the catalog SHA-256",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			if id == "" || outPath == "" {
				return backupUsageErr("--id and --out are required")
			}
			if _, err := os.Lstat(outPath); err == nil {
				return backupUsageErr("--out " + outPath + " already exists; refusing to overwrite")
			}
			st, catalog, err := openBackupCatalog(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			rec, ok, err := catalog.Get(ctx, id)
			if err != nil {
				if errors.Is(err, backup.ErrInvalid) {
					return backupUsageErr(err.Error())
				}
				return fmt.Errorf("read backup catalog: %s", store.Redact(err.Error()))
			}
			if !ok {
				return backupUsageErr("backup " + id + " not found")
			}
			if rec.Status != backup.StatusComplete {
				return exitcode.New("backup_not_complete", exitcode.Rejected, fmt.Sprintf("backup %s is %s, only complete backups can be fetched", id, rec.Status), nil)
			}
			obj, err := backupObjectStore(rec.Bucket, 64, 4)
			if err != nil {
				return err
			}
			fetchErr := fetchVerified(ctx, obj, rec.ObjectKey, rec.Bytes, rec.SHA256, outPath)
			if fetchErr == nil && withManifest {
				fetchErr = fetchManifest(ctx, obj, rec, outPath+".manifest.json")
			}
			result, detail := "ok", map[string]any{"backup_id": id, "out": outPath}
			if fetchErr != nil {
				result, detail["error"] = "error", store.Redact(fetchErr.Error())
			}
			if err := auditBackup(ctx, st, "backup.fetch", result, detail); err != nil {
				// Fail closed: do not leave an unaudited copy of the backup behind.
				_ = os.Remove(outPath)
				_ = os.Remove(outPath + ".manifest.json")
				return errors.Join(fetchErr, err)
			}
			if fetchErr != nil {
				return fetchErr
			}
			fmt.Fprintf(cmd.OutOrStdout(), "backup %s fetched to %s (%d bytes, sha256 %s verified)\n", id, outPath, rec.Bytes, rec.SHA256)
			return nil
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "backup ID")
	cmd.Flags().StringVar(&outPath, "out", "", "destination file (must not exist)")
	cmd.Flags().BoolVar(&withManifest, "with-manifest", false, "also fetch the manifest sidecar to <out>.manifest.json")
	return cmd
}

func fetchVerified(ctx context.Context, obj *objectstore.S3, key string, size int64, sha string, outPath string) error {
	body, _, err := obj.DownloadVerified(ctx, key, size, sha)
	if err != nil {
		return verifyErr(fmt.Errorf("download backup artifact: %w", err))
	}
	defer body.Close()
	tmp, err := os.CreateTemp(filepathDir(outPath), ".strand-fetch-*")
	if err != nil {
		return fmt.Errorf("create temporary download file: %w", err)
	}
	tmpName := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := io.Copy(tmp, body); err != nil {
		return verifyErr(fmt.Errorf("download backup artifact: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Link (not rename) so an --out created meanwhile is never replaced.
	if err := os.Link(tmpName, outPath); err != nil {
		return fmt.Errorf("publish downloaded backup: %w", err)
	}
	_ = os.Remove(tmpName)
	keep = true
	return nil
}

func fetchManifest(ctx context.Context, obj *objectstore.S3, rec backup.Record, outPath string) error {
	prefix := "s3://" + rec.Bucket + "/"
	if !strings.HasPrefix(rec.ManifestURI, prefix) {
		return fmt.Errorf("manifest %s is not in bucket %s", rec.ManifestURI, rec.Bucket)
	}
	body, _, err := obj.Download(ctx, strings.TrimPrefix(rec.ManifestURI, prefix))
	if err != nil {
		return fmt.Errorf("download manifest sidecar: %w", err)
	}
	defer body.Close()
	b, err := io.ReadAll(io.LimitReader(body, 16<<20))
	if err != nil {
		return verifyErr(fmt.Errorf("download manifest sidecar: %w", err))
	}
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func verifyErr(err error) error {
	if strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "does not match") {
		return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, "backup artifact failed integrity verification", err)
	}
	return err
}

func filepathDir(p string) string {
	d := filepath.Dir(p)
	if d == "" {
		return "."
	}
	return d
}

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/web/store"
)

// backupRunnerEnv wires the managed-backup runner to the metadata store (catalog, repositories, audit)
// and the process environment (S3 credentials, encryption keys, native tools).
func backupRunnerEnv(st store.Store, out io.Writer) (*runner.Env, error) {
	cat, err := backup.NewCatalog(st)
	if err != nil {
		return nil, err
	}
	keys, err := backup.LoadKeyring(os.Getenv)
	if err != nil {
		return nil, backupUsageErr("backup encryption keys: " + err.Error())
	}
	return &runner.Env{
		Getenv: os.Getenv, Catalog: cat, Keys: keys, Log: out,
		Repo: st.GetBackupRepo,
		Audit: func(ctx context.Context, action, result string, detail map[string]any) error {
			if e, ok := detail["error"].(string); ok {
				detail["error"] = store.Redact(e)
			}
			return auditBackup(ctx, st, action, result, detail)
		},
		NoReadBack: os.Getenv("STRAND_BACKUP_VERIFY_READBACK") == "false" || os.Getenv("STRAND_BACKUP_VERIFY_READBACK") == "0",
	}, nil
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// runManagedBuiltinJob runs one built-in-method backup for a managed job (tracked run, execution lock),
// then applies retention and replication.
func runManagedBuiltinJob(ctx context.Context, cmd *cobra.Command, st store.Store, job backup.Job) error {
	if backup.IsUnsupportedMethod(job.Method) {
		return fmt.Errorf("backup job %s: method %q is unsupported (removed: runs on the database host)", job.ID, job.Method)
	}
	env, err := backupRunnerEnv(st, cmd.OutOrStdout())
	if err != nil {
		return err
	}
	return st.Locked(ctx, "backup-job-execution:"+job.ID, func(lockCtx context.Context) error {
		current, found, err := st.GetBackupJob(lockCtx, job.ID)
		if err != nil {
			return err
		}
		if !found || !current.UpdatedAt.Equal(job.UpdatedAt) {
			return errors.New("backup job is missing or its configuration changed")
		}
		if current, err = resolveJobConnection(lockCtx, st, current, os.Getenv); err != nil {
			return err
		}
		_, err = runTrackedBackupJob(lockCtx, st, current, func(runCtx context.Context) error {
			rec, err := env.RunJob(runCtx, current)
			if err != nil {
				return err
			}
			if current.ReplicateTo != "" {
				if _, err := env.Copy(runCtx, rec, current.ReplicateTo); err != nil {
					return fmt.Errorf("backup %s completed but replication to %s failed: %w", rec.ID, current.ReplicateTo, err)
				}
			}
			if current.Retention.Enabled() {
				res, err := env.PruneJob(runCtx, current, false)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "retention: %v\n", err)
				} else if len(res.Expired) > 0 || len(res.Errors) > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), "retention: expired %d backup(s), %d error(s)\n", len(res.Expired), len(res.Errors))
				}
			}
			return nil
		})
		// cron/daily/weekly/monthly jobs: the store only knows every_seconds, so move the due time to the
		// schedule's next occurrence (also after Run now).
		if aerr := advanceSchedule(context.WithoutCancel(lockCtx), st, current.ID, time.Now()); aerr != nil && err == nil {
			err = aerr
		}
		return err
	})
}

// runManagedBuiltinSchedule is the scheduler loop of a built-in-method job: it runs while the job keeps
// the revision it started with (an edit restarts the scheduler from the Web).
func runManagedBuiltinSchedule(ctx context.Context, cmd *cobra.Command, st store.Store, jobID string, once bool) error {
	job, found, err := st.GetBackupJob(ctx, jobID)
	if err != nil {
		return err
	}
	if !found || job.IsCustom() {
		return errors.New("backup job is missing or uses a custom wrapper (pass the wrapper command)")
	}
	if backup.IsUnsupportedMethod(job.Method) {
		return fmt.Errorf("backup job %s: method %q is unsupported (removed: runs on the database host)", jobID, job.Method)
	}
	if once {
		return runManagedBuiltinJob(ctx, cmd, st, job)
	}
	return runScheduleLoop(ctx, cmd.ErrOrStderr(), cmd.OutOrStdout(), st, jobID, schedulerOptions{
		limits: concurrencyFromEnv(os.Getenv),
		run: func(c context.Context, j backup.Job) error {
			return runManagedBuiltinJob(c, cmd, st, j)
		},
	})
}

func openBackupStoreOnly(ctx context.Context) (store.Store, error) {
	st, _, err := openBackupCatalog(ctx)
	return st, err
}

func newBackupPruneCmd() *cobra.Command {
	var jobID, repoID string
	var dryRun, asJSON bool
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Apply retention policies: delete expired backups from S3 and mark them expired",
		Long: `Apply the retention policy of a managed job (--job-id) or the default policy of a repository
(--repository, for backups that belong to no job). --dry-run shows the decision for every backup
without deleting anything. Legal hold and S3 Object Lock retention always win.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if (jobID == "") == (repoID == "") {
				return backupUsageErr("exactly one of --job-id and --repository is required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			st, err := openBackupStoreOnly(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			env, err := backupRunnerEnv(st, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			var results []runner.PruneResult
			if jobID != "" {
				job, ok, err := st.GetBackupJob(ctx, jobID)
				if err != nil || !ok {
					return backupUsageErr("backup job not found")
				}
				r, err := env.PruneJob(ctx, job, dryRun)
				if err != nil {
					return err
				}
				results = append(results, r)
			} else {
				repo, ok, err := st.GetBackupRepo(ctx, repoID)
				if err != nil || !ok {
					return backupUsageErr("backup repository not found")
				}
				if results, err = env.PruneRepository(ctx, repo, dryRun); err != nil {
					return err
				}
			}
			if asJSON {
				return printJSON(cmd.OutOrStdout(), results)
			}
			failures := 0
			for _, r := range results {
				for _, d := range r.Decisions {
					verdict := "keep  "
					if !d.Keep {
						verdict = "expire"
					}
					fmt.Fprintf(cmd.OutOrStdout(), "%s %-40s %-11s %s  %v\n", verdict, d.ID, d.Kind, d.Created, d.Reasons)
				}
				for _, e := range r.Errors {
					fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n", e)
				}
				failures += len(r.Errors)
			}
			if failures > 0 {
				return fmt.Errorf("%d backup(s) could not be expired", failures)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&jobID, "job-id", "", "managed backup job")
	cmd.Flags().StringVar(&repoID, "repository", "", "repository (backups without a job)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "only show what would be expired")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

// pushEncoding holds the --compress/--encrypt flags of push and schedule (one command per process).
var pushEncoding struct {
	compress, encrypt bool
	level             int
}

func addPushEncodingFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&pushEncoding.compress, "compress", false, "compress the payload with zstd before upload")
	cmd.Flags().IntVar(&pushEncoding.level, "compress-level", 0, "zstd level 1 (fastest) - 4 (best); 0 = default")
	cmd.Flags().BoolVar(&pushEncoding.encrypt, "encrypt", false, "encrypt the payload (AES-256-GCM) with the active key of STRAND_BACKUP_ENCRYPTION_KEYS")
}

// uploadPushPayload uploads an artifact stream, compressed/encrypted when requested; meta is nil for raw.
func uploadPushPayload(ctx context.Context, obj interface {
	Upload(context.Context, string, io.Reader, int64, string) (objectstore.ObjectInfo, error)
}, key string, src io.Reader, size int64) (objectstore.ObjectInfo, *backup.Meta, error) {
	if !pushEncoding.compress && !pushEncoding.encrypt {
		info, err := obj.Upload(ctx, key, src, size, "")
		return info, nil, err
	}
	opt := backup.EncodeOptions{Compress: pushEncoding.compress, Level: pushEncoding.level}
	if pushEncoding.encrypt {
		keys, err := backup.LoadKeyring(os.Getenv)
		if err != nil {
			return objectstore.ObjectInfo{}, nil, err
		}
		if keys == nil {
			return objectstore.ObjectInfo{}, nil, errors.New("--encrypt needs STRAND_BACKUP_ENCRYPTION_KEYS (or _KEY)")
		}
		k := *keys.Active
		opt.Key = &k
	}
	pr, pw := io.Pipe()
	var enc *backup.Encoder
	done := make(chan error, 1)
	go func() {
		var err error
		if enc, err = backup.NewEncoder(pw, opt); err != nil {
			_ = pw.CloseWithError(err)
			done <- err
			return
		}
		_, err = io.Copy(enc, src)
		if err == nil {
			err = enc.Close()
		}
		_ = pw.CloseWithError(err)
		done <- err
	}()
	info, upErr := obj.Upload(ctx, key, pr, -1, "")
	_ = pr.CloseWithError(errors.New("upload ended"))
	if err := <-done; err != nil {
		return info, nil, err
	}
	if upErr != nil {
		return info, nil, upErr
	}
	st := enc.Stats()
	m := backup.Meta{Format: backup.FormatSBK1, PlainBytes: st.PlainBytes, PlainSHA256: st.PlainSHA256}
	if opt.Compress {
		m.Compression = backup.CompressionZstd
	}
	if opt.Key != nil {
		m.KeyID = opt.Key.ID
	}
	return info, &m, nil
}

func addBackupProCommands(cmd *cobra.Command) {
	cmd.AddCommand(newBackupPruneCmd(), newBackupKeygenCmd(), newBackupCopyCmd(), newBackupVerifyCmd(),
		newBackupRestoreCmd(), newBackupLogArchiveCmd())
}

func newBackupRestoreCmd() *cobra.Command {
	var id, source, toTime, targetDB, targetDir string
	var target backup.ConnSpec
	var yes, dryRun bool
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a backup (or a source to a point in time) into a NEW database or directory",
		Long: `Restore --id <backup> as is, or --source <identity> --to-time <RFC3339> (point-in-time: the base
backup is chosen automatically and archived logs are replayed up to the target time). The target is
always new: --target-database must not exist on the target server (logical backups), --target-dir must
not exist or be empty (physical backups: pg_basebackup). The source is never written.
Passwords come from the environment variable named by --target-password-env.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if (id == "") == (source == "") {
				return backupUsageErr("exactly one of --id and --source is required")
			}
			if source != "" && toTime == "" {
				return backupUsageErr("--source needs --to-time")
			}
			if !dryRun && (targetDB == "") == (targetDir == "") {
				return backupUsageErr("exactly one of --target-database and --target-dir is required")
			}
			if target.PasswordEnv != "" && !backup.ValidEnvName(target.PasswordEnv) || target.URIEnv != "" && !backup.ValidEnvName(target.URIEnv) {
				return backupUsageErr("invalid or reserved environment variable name for the target secret")
			}
			var at *time.Time
			if toTime != "" {
				t, err := time.Parse(time.RFC3339, toTime)
				if err != nil {
					return backupUsageErr("--to-time must be RFC3339")
				}
				at = &t
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			st, err := openBackupStoreOnly(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			env, err := backupRunnerEnv(st, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			var chosen *backup.Record
			if id != "" {
				rec, ok, err := env.Catalog.Get(ctx, id)
				if err != nil || !ok {
					return backupUsageErr("backup not found")
				}
				if rec.Status != backup.StatusComplete {
					return backupUsageErr("only complete backups can be restored")
				}
				chosen, source = &rec, rec.Source
			}
			all, err := env.Catalog.ListAll(ctx, backup.Filter{Source: source, Status: backup.StatusComplete}, 0)
			if err != nil {
				return err
			}
			plan, err := runner.PlanRestore(all, chosen, at)
			if err != nil {
				return exitcode.New("restore_plan", exitcode.Rejected, err.Error(), err)
			}
			if dryRun {
				return printJSON(cmd.OutOrStdout(), plan)
			}
			if !yes {
				return backupUsageErr("restores need --yes (the target must be new; nothing is overwritten)")
			}
			return env.Restore(ctx, plan, runner.RestoreTarget{Conn: target, Database: targetDB, Dir: targetDir})
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "backup to restore")
	cmd.Flags().StringVar(&source, "source", "", "source identity (with --to-time)")
	cmd.Flags().StringVar(&toTime, "to-time", "", "point-in-time target (RFC3339)")
	cmd.Flags().StringVar(&target.Host, "target-host", "", "target server host")
	cmd.Flags().IntVar(&target.Port, "target-port", 0, "target server port")
	cmd.Flags().StringVar(&target.User, "target-user", "", "target server user")
	cmd.Flags().StringVar(&target.PasswordEnv, "target-password-env", "", "environment variable holding the target password")
	cmd.Flags().StringVar(&target.URIEnv, "target-uri-env", "", "MongoDB: environment variable holding the target URI")
	cmd.Flags().StringVar(&targetDB, "target-database", "", "new database to create (logical restores)")
	cmd.Flags().StringVar(&targetDir, "target-dir", "", "new data directory (physical restores)")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm the restore")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the restore plan (base, chain, logs) only")
	return cmd
}

func newBackupLogArchiveCmd() *cobra.Command {
	var jobID, staging string
	cmd := &cobra.Command{
		Use:   "log-archive",
		Short: "Continuously archive binlog/WAL/oplog of a job's source for point-in-time recovery",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !backup.ValidID(jobID) {
				return backupUsageErr("--job-id is required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			st, err := openBackupStoreOnly(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			job, ok, err := st.GetBackupJob(ctx, jobID)
			if err != nil || !ok || job.IsCustom() || !job.LogArchive {
				return backupUsageErr("backup job not found or log archiving is off")
			}
			if err := backup.ValidatePITR(job); err != nil {
				return fmt.Errorf("cannot archive logs for job %s: %w", jobID, err)
			}
			if job, err = resolveJobConnection(ctx, st, job, os.Getenv); err != nil {
				return err
			}
			env, err := backupRunnerEnv(st, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			revision := job.UpdatedAt
			// One archiver per job across instances; stop when the job definition changes.
			return st.Locked(ctx, "backup-log-archive:"+jobID, func(lctx context.Context) error {
				actx, cancel := context.WithCancel(lctx)
				defer cancel()
				go func() {
					for actx.Err() == nil {
						time.Sleep(5 * time.Second)
						j, ok, err := st.GetBackupJob(actx, jobID)
						if err == nil && (!ok || !j.UpdatedAt.Equal(revision) || !j.LogArchive) {
							cancel()
						}
					}
				}()
				switch job.Engine() {
				case backup.EnginePostgres:
					return env.ArchiveWAL(actx, job)
				case backup.EngineMongoDB:
					return env.ArchiveOplog(actx, job)
				default:
					dir := staging
					if dir == "" {
						base := os.Getenv("STRAND_BACKUP_TMPDIR")
						if base == "" {
							base = os.TempDir()
						}
						dir = filepath.Join(base, "strand-binlog-"+jobID)
					}
					return env.ArchiveBinlog(actx, job, dir)
				}
			})
		},
	}
	cmd.Flags().StringVar(&jobID, "job-id", "", "managed backup job with log_archive enabled")
	cmd.Flags().StringVar(&staging, "staging-dir", "", "MySQL: local staging directory (default $STRAND_BACKUP_TMPDIR/strand-binlog-<job>)")
	return cmd
}

func newBackupVerifyCmd() *cobra.Command {
	var id, jobID string
	var sample int
	var head, asJSON, testRestore bool
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check backup integrity (re-read and decrypt payloads, chains, log continuity)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if (id == "") == (jobID == "") {
				return backupUsageErr("exactly one of --id and --job-id is required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			st, err := openBackupStoreOnly(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			env, err := backupRunnerEnv(st, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			if id != "" {
				rec, ok, err := env.Catalog.Get(ctx, id)
				if err != nil || !ok {
					return backupUsageErr("backup not found")
				}
				verr := env.VerifyBackup(ctx, rec, !head)
				if err := env.Audit(ctx, "backup.verify", map[bool]string{true: "ok", false: "error"}[verr == nil], map[string]any{"backup_id": id}); err != nil {
					return errors.Join(verr, err)
				}
				if verr != nil {
					return verifyErr(verr)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "backup %s verified\n", id)
				return nil
			}
			job, ok, err := st.GetBackupJob(ctx, jobID)
			if err != nil || !ok {
				return backupUsageErr("backup job not found")
			}
			if sample == 0 {
				sample = job.VerifySamplePercent
			}
			rep, err := env.VerifyJob(ctx, job, runner.VerifyOptions{SamplePercent: sample})
			if err != nil {
				return err
			}
			if testRestore {
				res, terr := env.TestRestore(ctx, job)
				if terr != nil {
					rep.Problems = append(rep.Problems, "test restore: "+terr.Error())
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "test restore: %s\n", res.Detail)
				}
			}
			result := "ok"
			if !rep.OK() {
				result = "error"
			}
			if err := env.Audit(ctx, "backup.verify", result, map[string]any{"job": jobID, "checked": rep.Checked,
				"failed": len(rep.Failed), "problems": rep.Problems}); err != nil {
				return err
			}
			if asJSON {
				if err := printJSON(cmd.OutOrStdout(), rep); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "checked %d backup(s), %d fully re-read (%d bytes); %d failed, %d problem(s)\n",
					rep.Checked, rep.FullReads, rep.Bytes, len(rep.Failed), len(rep.Problems))
				for _, f := range append(rep.Failed, rep.Problems...) {
					fmt.Fprintln(cmd.OutOrStdout(), "  "+f)
				}
			}
			if !rep.OK() {
				return fmt.Errorf("verification found %d failure(s) and %d problem(s)", len(rep.Failed), len(rep.Problems))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "verify one backup")
	cmd.Flags().StringVar(&jobID, "job-id", "", "verify every backup of a job")
	cmd.Flags().IntVar(&sample, "sample", 0, "percent of payloads fully re-read (default: job setting, else 100)")
	cmd.Flags().BoolVar(&head, "head-only", false, "with --id: only check existence, size and digest metadata")
	cmd.Flags().BoolVar(&testRestore, "test-restore", false, "with --job-id: also restore the newest backup into a scratch database and count rows")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

// fetchManaged downloads a backup through its repository: stored bytes verified against the catalog,
// then (unless raw) decrypted/decompressed with the plaintext SHA-256 verified.
func fetchManaged(ctx context.Context, st store.Store, rec backup.Record, raw, withManifest bool, outPath string) error {
	env, err := backupRunnerEnv(st, io.Discard)
	if err != nil {
		return err
	}
	repo, err := env.OpenRecordRepository(ctx, rec)
	if err != nil {
		return err
	}
	var body io.ReadCloser
	if raw {
		body, _, err = repo.Store.DownloadVerified(ctx, rec.ObjectKey, rec.Bytes, rec.SHA256)
	} else {
		body, err = repo.OpenPayload(ctx, rec, env.Keys)
	}
	if err != nil {
		return verifyErr(fmt.Errorf("download backup artifact: %w", err))
	}
	defer body.Close()
	if err := writeVerifiedFile(body, outPath); err != nil {
		return verifyErr(err)
	}
	if withManifest {
		return fetchManifest(ctx, repo.Store, rec, outPath+".manifest.json")
	}
	return nil
}

func newBackupKeygenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "keygen",
		Short: "Print a new random 256-bit backup encryption key (base64)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			k, err := backup.GenerateKeyBase64(rand.Read)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), k)
			return nil
		},
	}
}

func newBackupCopyCmd() *cobra.Command {
	var id, to string
	cmd := &cobra.Command{
		Use:   "copy",
		Short: "Copy a backup to another repository (3-2-1) and catalog the replica",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if id == "" || to == "" {
				return backupUsageErr("--id and --to-repository are required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			st, err := openBackupStoreOnly(ctx)
			if err != nil {
				return err
			}
			defer st.Close()
			env, err := backupRunnerEnv(st, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			rec, ok, err := env.Catalog.Get(ctx, id)
			if err != nil || !ok {
				return backupUsageErr("backup not found")
			}
			cp, err := env.Copy(ctx, rec, to)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "backup %s copied to repository %s as %s\n", id, to, cp.ID)
			return nil
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "backup ID")
	cmd.Flags().StringVar(&to, "to-repository", "", "destination repository ID")
	return cmd
}

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/backup/sshtun"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/tunnel"
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
			if current.ConnectionID != "" {
				c, ok, err := st.GetBackupConnection(runCtx, current.ConnectionID)
				if err != nil {
					return err
				}
				if ok && c.SSHTunnel != nil {
					addrHost, addrPort, closeTunnel, err := sshtun.OpenForConnection(runCtx, c)
					if err != nil {
						return fmt.Errorf("open ssh tunnel: %w", err)
					}
					defer closeTunnel()
					current.Conn.Host = addrHost
					current.Conn.Port = addrPort
				}
			}
			rec, err := env.RunJob(runCtx, current)
			if err != nil {
				return err
			}
			backup.SetRunReport(runCtx, &backup.RunReport{
				BackupID:     rec.ID,
				BytesRead:    rec.PlainBytes,
				BytesWritten: rec.Bytes,
			})
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
	// Preflight: without its native tool every run would fail; refuse before a run, a catalog record or a
	// restore point is created.
	if err := runner.CheckTools(os.Getenv, runner.BackupToolNeeds(job.Method)); err != nil {
		return fmt.Errorf("backup job %s: %w", jobID, err)
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
		newBackupRestoreCmd(), newBackupLogArchiveCmd(), newBackupDeleteCmd())
}

// backupRejected is the exit-3 error of an operation refused because of the record's state.
func backupRejected(err error) error {
	return exitcode.New("backup_rejected", exitcode.Rejected, err.Error(), err)
}

func newBackupDeleteCmd() *cobra.Command {
	var id string
	var abandoned, asJSON bool
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a restore point that is not complete (failed, expired) and the objects it uploaded",
		Long: `Delete a failed or expired restore point: its partial objects in the repository (payload, an
unfinished multipart upload, the manifest) and then its catalog record. Complete restore points are never
deleted by hand: the retention policy (strand backup prune) removes them. Legal hold and S3 Object Lock
retention refuse the delete. A record still queued/uploading/verifying is refused unless --abandoned says its
backup process is gone (the Web checks that before it passes the flag); it is marked failed first.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !backup.ValidID(id) {
				return backupUsageErr("--id is required")
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
			if backup.InProgress(rec.Status) {
				if !abandoned {
					return backupRejected(fmt.Errorf("restore point %s is %s: it is still in progress (pass --abandoned only when its backup process is gone)", id, rec.Status))
				}
				if rec.JobID != "" {
					if j, ok, err := st.GetBackupJob(ctx, rec.JobID); err == nil && ok && j.LastRunStatus == backup.RunStatusRunning {
						return backupRejected(fmt.Errorf("restore point %s is %s and job %s is running: wait for the run to finish", id, rec.Status, rec.JobID))
					}
				}
				if err := backup.DeletableErr(rec, time.Now()); err != nil {
					return backupRejected(err)
				}
				if rec, _, err = env.Catalog.UpdateStatus(ctx, id, backup.StatusFailed, nil); err != nil {
					return fmt.Errorf("mark %s failed: %w", id, err)
				}
			}
			res, err := env.DeleteRecord(ctx, rec) // audited, also when refused
			if errors.Is(err, backup.ErrInvalid) {
				return backupRejected(err)
			}
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(cmd.OutOrStdout(), res)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restore point %s (%s) deleted; %d object(s) removed\n", id, res.Status, len(res.DeletedObjects))
			return nil
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "restore point (backup) ID")
	cmd.Flags().BoolVar(&abandoned, "abandoned", false, "the record is queued/uploading/verifying but its backup process is gone: mark it failed, then delete it")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func newBackupRestoreCmd() *cobra.Command {
	var id, source, toTime, targetDB, targetDir string
	var tunnelSSH, tunnelUser, tunnelIdentity, tunnelKnownHosts, targetCAEnv string
	var target backup.ConnSpec
	var yes, dryRun, targetDeployment bool
	var confirmTarget string
	var threads int
	var objects []string
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
			if !dryRun {
				if targetDeployment {
					if targetDB != "" || targetDir != "" {
						return backupUsageErr("--target-deployment cannot be combined with a database or directory")
					}
				} else if (targetDB == "") == (targetDir == "") {
					return backupUsageErr("specify one of --target-database, --target-dir or --target-deployment")
				}
			}
			if target.PasswordEnv != "" && !backup.ValidEnvName(target.PasswordEnv) || target.URIEnv != "" && !backup.ValidEnvName(target.URIEnv) {
				return backupUsageErr("invalid or reserved environment variable name for the target secret")
			}
			if err := runner.ValidateRestoreThreads(threads); err != nil {
				return backupUsageErr(err.Error())
			}
			if err := runner.ValidateRestoreObjects(objects); err != nil {
				return backupUsageErr(err.Error())
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
				if err := backup.RequireComplete(rec, "restored"); err != nil {
					return backupRejected(err)
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
			if targetCAEnv != "" {
				if !backup.ValidEnvName(targetCAEnv) || os.Getenv(targetCAEnv) == "" {
					return backupUsageErr("--target-ca-env must name a variable containing the target CA")
				}
				target.CACertPEM = os.Getenv(targetCAEnv)
			}
			base := plan.Base()
			if base.Engine == backup.EngineMongoDB {
				uri, err := runner.MongoURI(os.Getenv, target)
				if err != nil {
					return backupUsageErr(err.Error())
				}
				destination, err := runner.MongoRestoreDestination(uri, targetDB)
				if err != nil {
					return backupUsageErr(err.Error())
				}
				if confirmTarget != destination {
					return backupUsageErr("confirm the MongoDB restore with --confirm-target " + destination)
				}
			} else if targetDeployment {
				return backupUsageErr("--target-deployment applies only to MongoDB")
			}
			if tunnelSSH != "" {
				if target.URIEnv != "" {
					return backupUsageErr("ssh tunnel cannot be used with a connection URI; use host/port/user")
				}
				if err := tunnel.ValidateSecurity(tunnelIdentity, tunnelKnownHosts); err != nil {
					return fmt.Errorf("tunnel security: %w", err)
				}
				remotePort := target.Port
				if remotePort == 0 && chosen != nil {
					remotePort = backup.DefaultPort(chosen.Engine)
				}
				if remotePort == 0 {
					return backupUsageErr("restore with tunnel needs a target port or recognizable backup engine")
				}
				remoteAddr := net.JoinHostPort(target.Host, strconv.Itoa(remotePort))
				forward, err := tunnel.Start(ctx, tunnel.Config{
					SSHAddress:   tunnelSSH,
					SSHUser:      tunnelUser,
					IdentityFile: tunnelIdentity,
					HostKeysFile: tunnelKnownHosts,
					Remote:       remoteAddr,
					LocalAddress: "127.0.0.1:0",
				})
				if err != nil {
					return fmt.Errorf("start restore ssh tunnel: %w", err)
				}
				defer forward.Close()
				slog.Info("restore ssh tunnel opened", "ssh", tunnelSSH, "remote", remoteAddr, "local", forward.Address())
				h, pStr, err := net.SplitHostPort(forward.Address())
				if err != nil {
					return fmt.Errorf("parse restore tunnel address: %w", err)
				}
				p, _ := strconv.Atoi(pStr)
				target.TLSServerName = target.Host
				target.Host = h
				target.Port = p
			}
			baseRec := plan.Base()
			baseMethod := baseRec.Method
			if baseMethod == "" {
				baseMethod = baseRec.Tool
			}
			switch baseMethod {
			case backup.MethodMySQLDump, backup.MethodMariaDBDump:
				if threads > 1 {
					return backupUsageErr("parallel restore is not available for mysqldump backups")
				}
				if len(objects) > 0 && at != nil {
					return backupUsageErr("partial restore with point-in-time recovery is not supported for MySQL")
				}
			case backup.MethodPGBaseBackup:
				if threads > 0 {
					return backupUsageErr("parallel restore is not available for physical backups")
				}
				if len(objects) > 0 {
					return backupUsageErr("partial restore is not available for physical backups")
				}
			case backup.MethodMongodump:
				if len(objects) > 0 && (baseRec.Details == nil || baseRec.Details["databases"] == "") {
					return backupUsageErr("partial restore is not available for whole-deployment MongoDB backups")
				}
			case backup.MethodPGDump:
				// supported
			default:
				if threads > 0 {
					return backupUsageErr("parallel restore is not available for " + baseMethod + " backups")
				}
				if len(objects) > 0 {
					return backupUsageErr("partial restore is not available for " + baseMethod + " backups")
				}
			}
			return env.Restore(ctx, plan, runner.RestoreTarget{
				Conn:       target,
				Database:   targetDB,
				Deployment: targetDeployment,
				Dir:        targetDir,
				Threads:    threads,
				Objects:    objects,
			})
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
	cmd.Flags().StringVar(&target.TLSMode, "target-tls-mode", "", "target TLS policy (disable, require, verify-ca or verify-full)")
	cmd.Flags().StringVar(&targetCAEnv, "target-ca-env", "", "environment variable containing the target CA PEM")
	cmd.Flags().StringVar(&targetDB, "target-database", "", "new database to create (logical restores)")
	cmd.Flags().BoolVar(&targetDeployment, "target-deployment", false, "restore a MongoDB deployment backup into an empty deployment")
	cmd.Flags().StringVar(&confirmTarget, "confirm-target", "", "MongoDB endpoint and database or /* for a deployment restore")
	cmd.Flags().StringVar(&targetDir, "target-dir", "", "new data directory (physical restores)")
	cmd.Flags().IntVar(&threads, "threads", 0, "parallel restore workers (1..16, pg_dump/mongodump only)")
	cmd.Flags().StringSliceVar(&objects, "objects", nil, "comma-separated or repeatable list of tables/collections to restore")
	cmd.Flags().StringVar(&tunnelSSH, "tunnel-ssh", "", "optional SSH bastion host[:port]")
	cmd.Flags().StringVar(&tunnelUser, "tunnel-user", "", "SSH username")
	cmd.Flags().StringVar(&tunnelIdentity, "tunnel-identity-file", "", "SSH private key (or SSH_AUTH_SOCK)")
	cmd.Flags().StringVar(&tunnelKnownHosts, "tunnel-known-hosts", "", "SSH known_hosts file")
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
				if job.ConnectionID != "" {
					c, ok, err := st.GetBackupConnection(actx, job.ConnectionID)
					if err != nil {
						return err
					}
					if ok && c.SSHTunnel != nil {
						addrHost, addrPort, closeTunnel, err := sshtun.OpenForConnection(actx, c)
						if err != nil {
							return fmt.Errorf("open ssh tunnel: %w", err)
						}
						defer closeTunnel()
						job.Conn.Host = addrHost
						job.Conn.Port = addrPort
					}
				}
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
				if err := backup.RequireComplete(rec, "verified"); err != nil {
					return backupRejected(err)
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
				if job, err = resolveScratchConnection(ctx, st, job); err != nil {
					return err
				}
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
		body, _, err = repo.Store.DownloadVerifiedVersion(ctx, rec.ObjectKey, rec.VersionID, rec.Bytes, rec.SHA256)
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
			if err := backup.RequireComplete(rec, "copied"); err != nil {
				return backupRejected(err)
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

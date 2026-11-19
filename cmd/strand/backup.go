package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/seed"
	"strand/internal/tunnel"
	"strand/internal/web"
	"strand/internal/web/store"
)

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Manage centralized database backups"}
	cmd.AddCommand(newBackupPushCmd(), newBackupListCmd(), newBackupFetchCmd(), newBackupScheduleCmd())
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

// newBackupScheduleCmd runs an operator-provided native backup wrapper on a
// recurring interval, then publishes its artifact and seed manifest centrally.
func newBackupScheduleCmd() *cobra.Command {
	var every time.Duration
	var artifactTemplate, manifestTemplate, bucket, prefix string
	var jobID string
	var sshAddress, sshUser, identityFile, knownHosts, remoteAddress string
	var once, cleanupLocal bool
	cmd := &cobra.Command{
		Use:   "schedule -- command [args...]",
		Short: "Run a native backup command repeatedly and publish each result",
		Long: `Run a trusted native backup wrapper on a fixed interval. The wrapper
must create the artifact and v1 seed manifest at the paths supplied in the
STRAND_BACKUP_* environment variables. Use {id} in --artifact and --manifest
paths for unique files. This process stays in the foreground; supervise it with
systemd or another process manager.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if every < time.Minute {
				return errors.New("--every must be at least 1m")
			}
			if artifactTemplate == "" || manifestTemplate == "" || bucket == "" {
				return errors.New("--artifact, --manifest, and --bucket are required")
			}
			if !strings.Contains(artifactTemplate, "{id}") || !strings.Contains(manifestTemplate, "{id}") {
				return errors.New("--artifact and --manifest templates must each contain {id} to prevent overwriting prior backups")
			}
			if jobID != "" && !backup.ValidID(jobID) {
				return errors.New("--job-id is invalid")
			}
			// FileJobRunner gives this process a direct file descriptor for its log.
			// Cap this long-lived scheduler and the wrapped backup tool's output here.
			limitedOutput := &cappedBackupLog{dst: cmd.OutOrStdout(), limit: 32 << 20}
			cmd.SetOut(limitedOutput)
			cmd.SetErr(limitedOutput)
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			run := func(runCtx context.Context) error {
				id, err := newBackupID()
				if err != nil {
					return fmt.Errorf("generate backup ID: %w", err)
				}
				return executeScheduledBackup(runCtx, cmd, args, id, artifactTemplate, manifestTemplate, bucket, prefix,
					sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal)
			}
			if jobID == "" {
				return runBackupSchedule(ctx, cmd, every, once, run)
			}
			st, err := web.CLIStore(ctx, cfgPath, nil)
			if err != nil {
				return fmt.Errorf("open backup job metadata store: %w", err)
			}
			if st == nil {
				return errors.New("managed backup jobs require the metadata database")
			}
			defer st.Close()
			if once {
				return st.Locked(ctx, "backup-job-execution:"+jobID, func(lockCtx context.Context) error {
					job, found, err := st.GetBackupJob(lockCtx, jobID)
					if err != nil {
						return err
					}
					if !found || !managedBackupJobMatches(job, args[0], every, artifactTemplate, manifestTemplate,
						bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal) {
						return errors.New("backup job is missing or its configuration changed")
					}
					_, err = runTrackedBackupJob(lockCtx, st, job, run)
					return err
				})
			}
			return st.Locked(ctx, "backup-job-scheduler:"+jobID, func(lockCtx context.Context) error {
				return runManagedBackupSchedule(lockCtx, cmd, st, jobID, every, args[0], artifactTemplate,
					manifestTemplate, bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress,
					cleanupLocal, run)
			})
		},
	}
	cmd.Flags().DurationVar(&every, "every", 24*time.Hour, "backup interval (minimum 1m)")
	cmd.Flags().BoolVar(&once, "once", false, "run exactly one backup and exit (used for Run now)")
	cmd.Flags().StringVar(&jobID, "job-id", "", "managed Web backup job ID (enables distributed scheduling locks)")
	cmd.Flags().BoolVar(&cleanupLocal, "cleanup-local", false, "remove generated local artifact and manifest after successful upload")
	cmd.Flags().StringVar(&artifactTemplate, "artifact", "", "artifact path template; {id} is replaced with a unique backup ID")
	cmd.Flags().StringVar(&manifestTemplate, "manifest", "", "seed manifest path template; {id} is replaced with a unique backup ID")
	cmd.Flags().StringVar(&bucket, "bucket", "", "destination S3 bucket")
	cmd.Flags().StringVar(&prefix, "prefix", "", "optional S3 key prefix")
	cmd.Flags().StringVar(&sshAddress, "tunnel-ssh", "", "optional SSH bastion host[:port]")
	cmd.Flags().StringVar(&sshUser, "tunnel-user", "", "SSH username")
	cmd.Flags().StringVar(&identityFile, "tunnel-identity-file", "", "SSH private key (or SSH_AUTH_SOCK)")
	cmd.Flags().StringVar(&knownHosts, "tunnel-known-hosts", "", "SSH known_hosts file")
	cmd.Flags().StringVar(&remoteAddress, "tunnel-remote", "", "database host:port as seen from bastion")
	return cmd
}

type cappedBackupLog struct {
	mu        sync.Mutex
	dst       io.Writer
	limit     int64
	written   int64
	truncated bool
}

func (w *cappedBackupLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	original := len(p)
	const marker = "\n[strand] backup job log limit reached; further output discarded\n"
	remaining := w.limit - int64(len(marker)) - w.written
	if remaining > 0 {
		n := len(p)
		if int64(n) > remaining {
			n = int(remaining)
		}
		written, err := w.dst.Write(p[:n])
		w.written += int64(written)
		if err != nil {
			return written, err
		}
		if written != n {
			return written, io.ErrShortWrite
		}
	}
	if int64(original) > remaining && !w.truncated {
		w.truncated = true
		_, _ = io.WriteString(w.dst, marker)
	}
	return original, nil
}

func runBackupSchedule(ctx context.Context, cmd *cobra.Command, every time.Duration, once bool, run func(context.Context) error) error {
	for {
		err := run(ctx)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "backup run failed: %v\n", err)
			if once {
				return err
			}
		} else if once {
			return nil
		}
		timer := time.NewTimer(every)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func runManagedBackupSchedule(ctx context.Context, cmd *cobra.Command, st interface {
	GetBackupJob(context.Context, string) (backup.Job, bool, error)
	SetBackupJobNextRun(context.Context, string, time.Time, time.Time) (bool, error)
	BeginBackupJobRun(context.Context, string, time.Time) (time.Time, bool, error)
	FinishBackupJobRun(context.Context, string, time.Time, backup.RunStatus) (bool, error)
	RecoverBackupJobRun(context.Context, string, time.Time) (bool, error)
	Locked(context.Context, string, func(context.Context) error) error
}, jobID string, every time.Duration, command, artifactTemplate, manifestTemplate, bucket, prefix,
	sshAddress, sshUser, identityFile, knownHosts, remoteAddress string, cleanupLocal bool,
	run func(context.Context) error) error {
	for {
		job, found, err := st.GetBackupJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("read managed backup job: %w", err)
		}
		if !found || !job.Enabled || !managedBackupJobMatches(job, command, every, artifactTemplate, manifestTemplate,
			bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal) {
			return nil // stopped, deleted, or replaced by a newer configuration
		}
		// Recover only while holding the same execution lock used by Run now and
		// scheduled invocations. An active one-off run must never look interrupted.
		err = st.Locked(ctx, "backup-job-execution:"+jobID, func(lockCtx context.Context) error {
			current, found, err := st.GetBackupJob(lockCtx, jobID)
			if err != nil || !found || !current.UpdatedAt.Equal(job.UpdatedAt) {
				return err
			}
			_, err = st.RecoverBackupJobRun(lockCtx, jobID, job.UpdatedAt)
			return err
		})
		if err != nil {
			return fmt.Errorf("recover interrupted backup run: %w", err)
		}
		job, found, err = st.GetBackupJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("reload managed backup job: %w", err)
		}
		if !found || !job.Enabled || !managedBackupJobMatches(job, command, every, artifactTemplate, manifestTemplate,
			bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal) {
			return nil
		}
		next := time.Now().UTC()
		if job.NextRunAt != nil {
			next = *job.NextRunAt
		} else {
			ok, err := st.SetBackupJobNextRun(ctx, job.ID, job.UpdatedAt, next)
			if err != nil {
				return fmt.Errorf("initialize backup schedule: %w", err)
			}
			if !ok {
				continue
			}
			job.NextRunAt = &next
		}
		keepRunning, err := waitUntilBackupDue(ctx, next, func() (bool, error) {
			current, found, err := st.GetBackupJob(ctx, jobID)
			if err != nil {
				return false, err
			}
			return found && current.Enabled && current.UpdatedAt.Equal(job.UpdatedAt) &&
				managedBackupJobMatches(current, command, every, artifactTemplate, manifestTemplate, bucket,
					prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal), nil
		})
		if err != nil {
			return fmt.Errorf("check managed backup schedule: %w", err)
		}
		if !keepRunning {
			return nil
		}

		ran := false
		err = st.Locked(ctx, "backup-job-execution:"+jobID, func(runCtx context.Context) error {
			current, found, err := st.GetBackupJob(runCtx, jobID)
			if err != nil {
				return err
			}
			if !found || !current.Enabled || !managedBackupJobMatches(current, command, every, artifactTemplate,
				manifestTemplate, bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal) {
				return nil
			}
			if current.NextRunAt != nil && current.NextRunAt.After(time.Now()) {
				return nil // Run now or another completed run moved the due time.
			}
			ran, err = runTrackedBackupJob(runCtx, st, current, run)
			return err
		})
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "backup run failed: %v\n", err)
		} else if ran {
			fmt.Fprintln(cmd.OutOrStdout(), "backup run completed")
		}
	}
}

func runTrackedBackupJob(ctx context.Context, st interface {
	BeginBackupJobRun(context.Context, string, time.Time) (time.Time, bool, error)
	FinishBackupJobRun(context.Context, string, time.Time, backup.RunStatus) (bool, error)
}, job backup.Job, run func(context.Context) error) (bool, error) {
	startedAt, began, err := st.BeginBackupJobRun(ctx, job.ID, job.UpdatedAt)
	if err != nil {
		return false, err
	}
	if !began {
		return false, errors.New("backup job changed before this run could start")
	}
	runErr := run(ctx)
	status := backup.RunStatusSucceeded
	if runErr != nil {
		status = backup.RunStatusFailed
		if ctx.Err() != nil {
			status = backup.RunStatusCancelled
		}
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, finishErr := st.FinishBackupJobRun(finishCtx, job.ID, startedAt, status)
	return true, errors.Join(runErr, finishErr)
}

func waitUntilBackupDue(ctx context.Context, due time.Time, keepRunning func() (bool, error)) (bool, error) {
	for {
		keep, err := keepRunning()
		if err != nil || !keep {
			return keep, err
		}
		remaining := time.Until(due)
		if remaining <= 0 {
			return true, nil
		}
		step := time.Second
		if remaining < step {
			step = remaining
		}
		timer := time.NewTimer(step)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

func managedBackupJobMatches(j backup.Job, command string, every time.Duration, artifactTemplate, manifestTemplate,
	bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress string, cleanupLocal bool) bool {
	return j.Command == command && time.Duration(j.EverySeconds)*time.Second == every &&
		j.ArtifactTemplate == artifactTemplate && j.ManifestTemplate == manifestTemplate && j.Bucket == bucket &&
		j.Prefix == prefix && j.TunnelSSH == sshAddress && j.TunnelUser == sshUser &&
		j.TunnelIdentity == identityFile && j.TunnelKnownHosts == knownHosts && j.TunnelRemote == remoteAddress &&
		j.CleanupLocal == cleanupLocal
}

func executeScheduledBackup(ctx context.Context, cmd *cobra.Command, args []string, id, artifactTemplate,
	manifestTemplate, bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress string,
	cleanupLocal bool) error {
	artifactPath := strings.ReplaceAll(artifactTemplate, "{id}", id)
	manifestPath := strings.ReplaceAll(manifestTemplate, "{id}", id)
	var forward *tunnel.Forward
	var err error
	if sshAddress != "" {
		if knownHosts == "" {
			knownHosts, err = tunnel.DefaultKnownHosts()
			if err != nil {
				return fmt.Errorf("find default known_hosts file: %w", err)
			}
		}
		forward, err = tunnel.Start(ctx, tunnel.Config{SSHAddress: sshAddress, SSHUser: sshUser,
			IdentityFile: identityFile, HostKeysFile: knownHosts, Remote: remoteAddress})
		if err != nil {
			return err
		}
	}
	env := append(wrapperEnv(os.Environ()), "STRAND_BACKUP_ID="+id, "STRAND_BACKUP_ARTIFACT="+artifactPath,
		"STRAND_BACKUP_MANIFEST="+manifestPath)
	if forward != nil {
		env = append(env, "STRAND_BACKUP_SOURCE_ADDR="+forward.Address())
	}
	err = runBackupCommand(ctx, args, env, cmd.OutOrStdout(), cmd.ErrOrStderr())
	if forward != nil {
		_ = forward.Close()
	}
	if err != nil {
		return err
	}
	if err := publishScheduledBackup(ctx, cmd, id, manifestPath, artifactPath, bucket, prefix); err != nil {
		return err
	}
	if cleanupLocal {
		cleanupArtifact(cmd, artifactPath)
		cleanupArtifact(cmd, manifestPath)
	}
	return nil
}

// wrapperEnv is the environment handed to the native backup wrapper: Strand's own secrets (metadata DSN,
// Web secret key, admin/API credentials, S3 credentials) are removed. The wrapper needs only the source
// database credentials it is configured with; it must not be able to read or forge the catalog, decrypt
// stored pipeline secrets, or write to the backup bucket directly.
func wrapperEnv(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "STRAND_WEB_") || strings.HasPrefix(name, "STRAND_BACKUP_S3_") ||
			strings.HasPrefix(name, "STRAND_SECRET") || name == "AWS_SECRET_ACCESS_KEY" || name == "AWS_SESSION_TOKEN" {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func runBackupCommand(ctx context.Context, args, env []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("backup command is empty")
	}
	command := exec.Command(args[0], args[1:]...)
	command.Env, command.Stdout, command.Stderr = env, stdout, stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		pid := command.Process.Pid
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case err := <-done:
			// The wrapper may exit on TERM while grandchildren remain in its
			// process group; kill those too before releasing the distributed lock.
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return errors.Join(ctx.Err(), <-done)
		}
	}
}

func cleanupArtifact(cmd *cobra.Command, path string) {
	if _, err := os.Lstat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: local backup cleanup failed for %q: %v\n", path, err)
		}
		return
	}
	if err := os.RemoveAll(path); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: local backup cleanup failed for %q: %v\n", path, err)
	}
}

func validateManagedBackupJob(ctx context.Context, st interface {
	GetBackupJob(context.Context, string) (backup.Job, bool, error)
}, jobID, command string, every time.Duration, artifactTemplate, manifestTemplate, bucket, prefix,
	sshAddress, sshUser, identityFile, knownHosts, remoteAddress string, cleanupLocal, requireEnabled bool) error {
	j, found, err := st.GetBackupJob(ctx, jobID)
	if err != nil {
		return err
	}
	if !found || (requireEnabled && !j.Enabled) || !managedBackupJobMatches(j, command, every, artifactTemplate,
		manifestTemplate, bucket, prefix, sshAddress, sshUser, identityFile, knownHosts, remoteAddress, cleanupLocal) {
		return errors.New("backup job is missing, disabled, or its configuration changed")
	}
	return nil
}

func publishScheduledBackup(ctx context.Context, cmd *cobra.Command, id, manifestPath, artifactPath, bucket, prefix string) error {
	manifestBytes, manifest, err := readBackupManifest(manifestPath)
	if err != nil {
		return err
	}
	position, engine, err := backupPosition(manifest)
	if err != nil {
		return err
	}
	cleanPrefix, err := sanitizeBackupPrefix(prefix)
	if err != nil {
		return err
	}
	root, err := os.Lstat(artifactPath)
	if err != nil {
		return fmt.Errorf("stat backup artifact: %w", err)
	}
	if root.Mode()&os.ModeSymlink != 0 || (!root.IsDir() && !root.Mode().IsRegular()) {
		return errors.New("backup artifact must be a regular file or directory")
	}
	artifactKey, manifestKey := backupObjectKeys(cleanPrefix, id, root.IsDir())
	manifestURI, artifactURI := "s3://"+bucket+"/"+manifestKey, "s3://"+bucket+"/"+artifactKey
	obj, err := backupObjectStore(bucket, 64, 4)
	if err != nil {
		return err
	}
	st, err := web.CLIStore(ctx, cfgPath, nil)
	if err != nil {
		return fmt.Errorf("open backup metadata store: %s", store.Redact(err.Error()))
	}
	if st == nil {
		return errors.New("backup metadata database is required")
	}
	defer st.Close()
	catalog, err := backup.NewCatalog(st)
	if err != nil {
		return err
	}
	pushErr := st.Locked(ctx, "backup-upload:"+id, func(lockCtx context.Context) error {
		r := backup.Record{ID: id, Source: manifest.SourceIdentity, Engine: engine, Tool: manifest.Kind,
			ToolVersion: manifest.ToolVersion, Bucket: bucket, ObjectKey: artifactKey, Position: position,
			ManifestURI: manifestURI, Status: backup.StatusQueued, CreatedAt: time.Now().UTC(),
			LegalHold: manifest.LegalHoldRef != nil && strings.TrimSpace(*manifest.LegalHoldRef) != ""}
		if manifest.BackupExpiresAt != nil && strings.TrimSpace(*manifest.BackupExpiresAt) != "" {
			t, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(*manifest.BackupExpiresAt))
			if parseErr != nil {
				return errors.New("backup manifest backup_expires_at must be RFC3339")
			}
			r.ExpiresAt = &t
		}
		return pushBackupArtifact(lockCtx, cmd, obj, catalog, r, artifactPath, artifactKey, manifestKey, artifactURI, manifestURI, manifestBytes)
	})
	result, detail := "ok", map[string]any{"backup_id": id, "source": manifest.SourceIdentity, "object": artifactURI, "scheduled": true}
	if pushErr != nil {
		result, detail["error"] = "error", store.Redact(pushErr.Error())
	}
	if err := auditBackup(ctx, st, "backup.push", result, detail); err != nil {
		return errors.Join(pushErr, err)
	}
	return pushErr
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
		// A catalog record is not proof that its remote objects still exist. Check
		// both objects before reporting an idempotent retry as successful; this
		// catches manual bucket cleanup and partial external lifecycle policies.
		if err := verifyUploadedObject(ctx, obj, record.ObjectKey, record.Bytes, record.SHA256); err != nil {
			return fmt.Errorf("backup %s is cataloged complete but its artifact cannot be verified: %w", record.ID, err)
		}
		managedManifest, err := setManifestBackupRef(manifestBytes, artifactURI)
		if err != nil {
			return fmt.Errorf("encode managed backup manifest: %w", err)
		}
		manifestHash := sha256.Sum256(managedManifest)
		if err := verifyUploadedObject(ctx, obj, manifestKey, int64(len(managedManifest)), hex.EncodeToString(manifestHash[:])); err != nil {
			return fmt.Errorf("backup %s is cataloged complete but its manifest sidecar cannot be verified: %w", record.ID, err)
		}
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
	if artifactInfo.SHA256 == "" {
		return fail(errors.New("verify uploaded backup artifact: upload did not return a streamed SHA-256 digest"))
	}
	if err := verifyUploadedObject(ctx, obj, artifactKey, artifactInfo.Size, artifactInfo.SHA256); err != nil {
		return fail(fmt.Errorf("verify uploaded backup artifact: %w", err))
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
	manifestHash := sha256.Sum256(managedManifest)
	if err := verifyUploadedObject(ctx, obj, manifestKey, int64(len(managedManifest)), hex.EncodeToString(manifestHash[:])); err != nil {
		return fail(fmt.Errorf("verify uploaded backup manifest sidecar: %w", err))
	}
	verifiedAt := time.Now().UTC()
	_, _, err = catalog.Finalize(ctx, record.ID, backup.StatusVerifying, artifactInfo.Size, artifactInfo.SHA256, manifestURI, verifiedAt)
	if err != nil {
		return fail(fmt.Errorf("finalize backup catalog record: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "backup %s complete (%d bytes)\n", record.ID, artifactInfo.Size)
	return nil
}

func verifyUploadedObject(ctx context.Context, obj *objectstore.S3, key string, expectedSize int64, expectedSHA string) error {
	head, err := obj.Head(ctx, key)
	if err != nil {
		return err
	}
	if head.Size != expectedSize {
		return fmt.Errorf("object size %d differs from expected size %d", head.Size, expectedSize)
	}
	if head.SHA256 != "" && !strings.EqualFold(head.SHA256, expectedSHA) {
		return errors.New("object SHA-256 metadata differs from expected value")
	}
	verify := strings.TrimSpace(os.Getenv("STRAND_BACKUP_VERIFY_READBACK"))
	if strings.EqualFold(verify, "false") || verify == "0" {
		return nil
	}
	body, info, err := obj.Download(ctx, key)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, body)
	closeErr := body.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if info.Size >= 0 && info.Size != expectedSize {
		return fmt.Errorf("downloaded object size %d differs from expected size %d", info.Size, expectedSize)
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), expectedSHA) {
		return errors.New("downloaded object SHA-256 differs from uploaded data")
	}
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

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// Concurrency limits of the schedulers (all jobs of one Strand deployment share them through the metadata
// database, so they hold across the separate scheduler processes of the active Web instance).
const (
	EnvBackupMaxConcurrent = "STRAND_BACKUP_MAX_CONCURRENT"
	EnvBackupMaxPerHost    = "STRAND_BACKUP_MAX_PER_HOST"
	defaultMaxConcurrent   = 2
	defaultMaxPerHost      = 1
)

type concurrencyLimits struct{ maxTotal, maxPerHost int }

func concurrencyFromEnv(getenv func(string) string) concurrencyLimits {
	read := func(name string, def int) int {
		v := strings.TrimSpace(getenv(name))
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return def
		}
		return n // 0 = unlimited
	}
	return concurrencyLimits{read(EnvBackupMaxConcurrent, defaultMaxConcurrent), read(EnvBackupMaxPerHost, defaultMaxPerHost)}
}

// Timings of the slot protocol; variables so tests can shrink them.
var (
	slotPoll = 3 * time.Second
	slotTTL  = 90 * time.Second
)

type schedulerOptions struct {
	limits concurrencyLimits
	run    func(ctx context.Context, job backup.Job) error
	now    func() time.Time
}

func (o schedulerOptions) clock() time.Time {
	if o.now != nil {
		return o.now()
	}
	return time.Now()
}

// sourceHost is the identity of the database host a job reads, for the per-host limit.
func sourceHost(j backup.Job) string {
	switch {
	case j.Conn.Host != "":
		port := j.Conn.Port
		if port == 0 {
			port = backup.DefaultPort(map[string]string{backup.EngineMySQL: backup.ConnMySQL,
				backup.EnginePostgres: backup.ConnPostgres, backup.EngineMongoDB: backup.ConnMongoDB}[j.Engine()])
		}
		return strings.ToLower(j.Conn.Host) + ":" + strconv.Itoa(port)
	case j.ConnectionID != "":
		return "connection:" + j.ConnectionID
	}
	return "source:" + j.Source
}

// resolveJobConnection refreshes a job that references a stored connection from the connection row
// (host, port, user, secret variable names), so connection edits apply to the next run. A stored secret
// is handed to this process by the Web process in EnvConnPassword/EnvConnURI.
func resolveJobConnection(ctx context.Context, st store.Store, j backup.Job, getenv func(string) string) (backup.Job, error) {
	j, err := resolveScratchConnection(ctx, st, j)
	if err != nil || j.ConnectionID == "" {
		return j, err
	}
	c, ok, err := st.GetBackupConnection(ctx, j.ConnectionID)
	if err != nil {
		return j, fmt.Errorf("read backup connection: %w", err)
	}
	if !ok {
		return j, fmt.Errorf("backup connection %q of job %s no longer exists", j.ConnectionID, j.ID)
	}
	out, err := backup.ApplyConnection(j, c)
	if err != nil {
		return j, err
	}
	if c.SecretMode == backup.SecretStored &&
		((c.PasswordSet && getenv(backup.EnvConnPassword) == "") || (c.URISet && getenv(backup.EnvConnURI) == "")) {
		return j, fmt.Errorf("connection %q keeps its secret in the Web database: run this job from the Web console "+
			"(the Web process passes the decrypted secret to the backup process)", c.Name)
	}
	return out, nil
}

// resolveScratchConnection refreshes the scratch server of a job with test restores from its saved connection.
// Its stored password reaches this process in EnvScratchPassword (set by the Web process for verify runs only),
// so a missing value is not an error here: the test restore reports the failed login.
func resolveScratchConnection(ctx context.Context, st store.Store, j backup.Job) (backup.Job, error) {
	if j.ScratchConnectionID == "" || !j.TestRestore {
		return j, nil
	}
	c, ok, err := st.GetBackupConnection(ctx, j.ScratchConnectionID)
	if err != nil {
		return j, fmt.Errorf("read scratch connection: %w", err)
	}
	if !ok {
		return j, fmt.Errorf("scratch connection %q of job %s no longer exists", j.ScratchConnectionID, j.ID)
	}
	return backup.ApplyScratchConnection(j, c)
}

// nextAfterRun computes the due time that follows a run (or a missed run) for schedule kinds other than
// interval; interval jobs keep the store's every_seconds bookkeeping. ok=false: leave next_run_at alone.
func nextAfterRun(j backup.Job, now time.Time) (time.Time, bool) {
	if j.Schedule == nil || j.Schedule.Kind == backup.ScheduleInterval {
		return time.Time{}, false
	}
	// A failed run that still has retries keeps the retry back-off the store computed.
	if j.LastRunStatus == backup.RunStatusFailed && j.ConsecutiveFailures <= j.RetryMax && j.ConsecutiveFailures > 0 && j.RetryMax > 0 {
		return time.Time{}, false
	}
	n, err := j.Schedule.Next(now, now)
	if err != nil {
		return time.Time{}, false
	}
	return n, true
}

// advanceSchedule moves next_run_at of a cron/daily/weekly/monthly job to its following due time.
func advanceSchedule(ctx context.Context, st store.Store, jobID string, now time.Time) error {
	j, ok, err := st.GetBackupJob(ctx, jobID)
	if err != nil || !ok || !j.Enabled {
		return err
	}
	if n, ok := nextAfterRun(j, now); ok {
		_, err = st.SetBackupJobNextRun(ctx, jobID, j.UpdatedAt, n)
	}
	return err
}

// runScheduleLoop runs a job on its schedule: it waits for the due time (plus jitter), starts the run only
// inside the job's window (otherwise records it as missed and moves on), and takes a concurrency slot first.
func runScheduleLoop(ctx context.Context, errw, outw io.Writer, st store.Store, jobID string, o schedulerOptions) error {
	job, found, err := st.GetBackupJob(ctx, jobID)
	if err != nil || !found {
		return err
	}
	revision := job.UpdatedAt
	same := func(j backup.Job, ok bool) bool { return ok && j.Enabled && j.UpdatedAt.Equal(revision) }
	return st.Locked(ctx, "backup-job-scheduler:"+jobID, func(lockCtx context.Context) error {
		for {
			job, found, err := st.GetBackupJob(lockCtx, jobID)
			if err != nil {
				return err
			}
			if !same(job, found) {
				return nil
			}
			err = st.Locked(lockCtx, "backup-job-execution:"+jobID, func(c context.Context) error {
				_, err := st.RecoverBackupJobRun(c, jobID, revision)
				return err
			})
			if err != nil {
				return err
			}
			job, found, err = st.GetBackupJob(lockCtx, jobID)
			if err != nil || !same(job, found) {
				return err
			}
			next := o.clock().UTC()
			if job.NextRunAt != nil {
				next = *job.NextRunAt
			} else {
				if n, ok := nextAfterRun(job, next); ok {
					next = n
				}
				if ok, err := st.SetBackupJobNextRun(lockCtx, jobID, revision, next); err != nil || !ok {
					return err
				}
			}
			stillSame := func() (bool, error) {
				j, ok, err := st.GetBackupJob(lockCtx, jobID)
				return same(j, ok), err
			}
			keep, err := waitUntilBackupDue(lockCtx, next, stillSame)
			if err != nil || !keep {
				if lockCtx.Err() != nil {
					return nil
				}
				return err
			}
			cur, ok, err := st.GetBackupJob(lockCtx, jobID)
			if err != nil || !same(cur, ok) {
				return err
			}
			if cur.NextRunAt != nil && cur.NextRunAt.After(o.clock()) {
				continue // Run now moved the due time
			}
			due := next
			if cur.NextRunAt != nil {
				due = *cur.NextRunAt
			}
			deadline := time.Time{} // zero: no window, wait as long as it takes
			if cur.WindowMinutes > 0 {
				deadline = due.Add(time.Duration(cur.WindowMinutes) * time.Minute)
			}
			// Jitter spreads simultaneous due times; it never pushes the start past the window.
			if cur.JitterSeconds > 0 {
				start := due.Add(time.Duration(rand.Int64N(int64(cur.JitterSeconds)*int64(time.Second) + 1)))
				if !deadline.IsZero() && start.After(deadline) {
					start = deadline
				}
				if keep, err := waitUntilBackupDue(lockCtx, start, stillSame); err != nil || !keep {
					if lockCtx.Err() != nil {
						return nil
					}
					return err
				}
			}
			outcome, release, err := o.startInWindow(lockCtx, st, cur, deadline, stillSame)
			if err != nil {
				if lockCtx.Err() != nil {
					return nil
				}
				return err
			}
			switch outcome {
			case outcomeChanged:
				return nil
			case outcomeMissed:
				n, ok := nextAfterRun(cur, o.clock())
				if !ok {
					n = o.clock().UTC().Add(time.Duration(cur.EverySeconds) * time.Second)
				}
				if rec, err := st.RecordBackupJobMissed(lockCtx, jobID, revision, n); err != nil {
					return err
				} else if rec {
					fmt.Fprintf(errw, "backup run missed: not started within %d minute(s) of its due time %s (next %s)\n",
						cur.WindowMinutes, due.UTC().Format(time.RFC3339), n.UTC().Format(time.RFC3339))
				}
				continue
			}
			runErr := o.run(lockCtx, cur)
			release()
			if runErr != nil {
				if lockCtx.Err() != nil {
					return nil
				}
				fmt.Fprintf(errw, "backup run failed: %v\n", runErr)
			} else {
				fmt.Fprintln(outw, "backup run completed")
			}
		}
	})
}

type startOutcome int

const (
	outcomeRun startOutcome = iota
	outcomeMissed
	outcomeChanged
)

// startInWindow acquires the job's run slot. It keeps retrying until the slot is free, the window (deadline,
// zero = never) has passed (missed) or the job revision changed. On success the slot is held and renewed in
// the background until release is called.
func (o schedulerOptions) startInWindow(ctx context.Context, st store.Store, j backup.Job, deadline time.Time,
	stillSame func() (bool, error)) (startOutcome, func(), error) {
	ttl, poll := slotTTL, slotPoll
	req := store.SlotRequest{JobID: j.ID, Host: sourceHost(j), MaxTotal: o.limits.maxTotal, MaxPerHost: o.limits.maxPerHost, TTL: ttl}
	for {
		if !deadline.IsZero() && o.clock().After(deadline) {
			return outcomeMissed, nil, nil
		}
		ok, err := st.AcquireBackupSlot(ctx, req, o.clock())
		if err != nil {
			return outcomeRun, nil, err
		}
		if ok {
			stop := make(chan struct{})
			go func() {
				t := time.NewTicker(ttl / 3)
				defer t.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ctx.Done():
						return
					case <-t.C:
						_ = st.RenewBackupSlot(ctx, j.ID, o.clock())
					}
				}
			}()
			return outcomeRun, func() {
				close(stop)
				_ = st.ReleaseBackupSlot(context.WithoutCancel(ctx), j.ID) // expires by TTL if this fails
			}, nil
		}
		select {
		case <-ctx.Done():
			return outcomeRun, nil, ctx.Err()
		case <-time.After(poll):
		}
		if same, err := stillSame(); err != nil {
			return outcomeRun, nil, err
		} else if !same {
			return outcomeChanged, nil, nil
		}
	}
}

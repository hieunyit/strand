package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"strand/internal/backup"
)

// TestRestore restores the newest restore point of a logical-method job into a scratch database on the
// job's scratch server, counts the rows of every table, records the result (kind test_restore) and drops
// the scratch database. It proves the backup is restorable, not just intact.
func (e *Env) TestRestore(ctx context.Context, job backup.Job) (backup.CheckResult, error) {
	if err := checkRuntimeTLS(job.Engine(), job.Scratch); err != nil {
		return backup.CheckResult{}, err
	}
	// A missing restore tool is a setup problem, not a failed backup: refuse before recording a failed check.
	if err := CheckTools(e.Getenv, TestRestoreToolNeeds(job.Method)); err != nil {
		return backup.CheckResult{}, err
	}
	rs, err := e.Catalog.List(ctx, backup.Filter{JobID: job.ID, Status: backup.StatusComplete, NotKind: backup.KindLog, Limit: 1})
	if err != nil {
		return backup.CheckResult{}, err
	}
	if len(rs) == 0 {
		return backup.CheckResult{}, errors.New("no complete backup to test")
	}
	rec := rs[0]
	scratch, err := scratchDatabaseName(e.now())
	if err != nil {
		return backup.CheckResult{}, err
	}
	res := backup.CheckResult{BackupID: rec.ID, Kind: "test_restore", StartedAt: e.now(), Metrics: map[string]string{"scratch_database": scratch}}
	counts, err := e.testRestore(ctx, job, rec, scratch)
	res.FinishedAt = e.now()
	res.Metrics["duration_ms"] = strconv.FormatInt(res.FinishedAt.Sub(res.StartedAt).Milliseconds(), 10)
	if err != nil {
		res.Status, res.Detail = backup.CheckFailed, err.Error()
	} else {
		res.Status = backup.CheckOK
		total := int64(0)
		var names []string
		for t, n := range counts {
			total += n
			names = append(names, t)
		}
		sort.Strings(names)
		for i, t := range names {
			if i < 40 {
				res.Metrics["rows."+sanitizeMetric(t)] = strconv.FormatInt(counts[t], 10)
			}
		}
		res.Metrics["tables"], res.Metrics["rows"] = strconv.Itoa(len(counts)), strconv.FormatInt(total, 10)
		res.Detail = fmt.Sprintf("restored into %s: %d tables, %d rows", scratch, len(counts), total)
	}
	if _, aerr := e.Catalog.AddCheck(ctx, res); aerr != nil {
		return res, errors.Join(err, aerr)
	}
	return res, err
}

// scratchDatabaseName names a test-restore database. The random suffix keeps two runs started in the same
// second (another job on the same scratch server, or another Strand) from sharing a database.
func scratchDatabaseName(now time.Time) (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("scratch database name: %w", err)
	}
	return fmt.Sprintf("strand_verify_%d_%s", now.Unix(), hex.EncodeToString(b[:])), nil
}

func sanitizeMetric(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + 32
		}
		return '_'
	}, s)
}

func (e *Env) testRestore(ctx context.Context, job backup.Job, rec backup.Record, scratch string) (_ map[string]int64, retErr error) {
	plan := RestorePlan{Chain: []backup.Record{rec}}
	target := RestoreTarget{Conn: job.Scratch, Database: scratch}
	counts := map[string]int64{}
	switch rec.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		db, err := openBackupMySQL(e.Getenv, job.Scratch)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		// Only a database this run created is dropped: a failed restore drops what it created itself, and
		// a name that already existed (restores never overwrite) belongs to someone else.
		if err := e.restoreMySQL(ctx, plan, target); err != nil {
			return nil, err
		}
		defer func() {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			if _, derr := db.ExecContext(cctx, "DROP DATABASE IF EXISTS `"+strings.ReplaceAll(scratch, "`", "``")+"`"); derr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("drop scratch database %s: %w", scratch, derr))
			}
		}()
		rows, err := db.QueryContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", scratch)
		if err != nil {
			return nil, err
		}
		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				rows.Close()
				return nil, err
			}
			tables = append(tables, t)
		}
		rows.Close()
		for _, t := range tables {
			var n int64
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM `"+scratch+"`.`"+strings.ReplaceAll(t, "`", "``")+"`").Scan(&n); err != nil {
				return nil, err
			}
			counts[t] = n
		}
		return counts, nil
	case backup.MethodPGDump:
		if err := e.restorePGDump(ctx, rec, target); err != nil {
			return nil, err
		}
		defer func() {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			c, cerr := connectBackupPG(cctx, e.Getenv, job.Scratch, "postgres", false)
			if cerr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("drop scratch database %s: %v", scratch,
					redactAll(cerr.Error(), []string{password(e.Getenv, job.Scratch)})))
				return
			}
			defer c.Close(context.Background())
			if derr := dropPGDatabase(cctx, c, scratch); derr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("drop scratch database %s: %w", scratch, derr))
			}
		}()
		c, err := connectBackupPGX(ctx, e.Getenv, job.Scratch, scratch)
		if err != nil {
			return nil, err
		}
		defer c.Close(context.Background())
		rows, err := c.Query(ctx, `SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) FROM pg_tables
			WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`)
		if err != nil {
			return nil, err
		}
		tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			var n int64
			if err := c.QueryRow(ctx, "SELECT count(*) FROM "+t).Scan(&n); err != nil {
				return nil, err
			}
			counts[t] = n
		}
		return counts, nil
	}
	return nil, fmt.Errorf("test restore of %s is not supported", rec.Method)
}

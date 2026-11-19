package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"strand/internal/web/store"
)

// AuditArchiveCLIOptions configure AuditArchiveCLI (`strand web db audit-archive`).
type AuditArchiveCLIOptions struct {
	ConfigPath    string    // web.yaml (optional: state_dir, database.dsn, audit_* settings)
	DSN           string    // "" = STRAND_WEB_DATABASE_DSN, then database.dsn
	Before        time.Time // explicit cutoff; zero = now - OlderThanDays
	OlderThanDays int       // 0 = the audit_retention_days setting (database > web.yaml > 365)
	Out           string    // output file; "" = <audit_archive_dir or state_dir/audit-archive>/audit-<from>-<to>.jsonl.gz
	DryRun        bool
	W             io.Writer
}

// AuditArchiveCLI opens the metadata database and archives the old audit rows.
func AuditArchiveCLI(ctx context.Context, o AuditArchiveCLIOptions) (*AuditArchiveResult, error) {
	if o.W == nil {
		o.W = io.Discard
	}
	cfg := &Config{StateDir: DefaultStateDir}
	if o.ConfigPath != "" {
		c, err := LoadConfig(o.ConfigPath)
		if err != nil {
			return nil, err
		}
		cfg = c
	}
	dsn := resolveDSN(o.DSN, cfg)
	if dsn == "" {
		return nil, errors.New("no database: pass --dsn, set " + EnvDatabaseDSN + " or database.dsn in web.yaml (file mode rotates audit.log instead: audit_rotate_mb / audit_rotate_days)")
	}
	pg, err := store.Open(ctx, dsn, store.OpenOptions{MaxWait: 15 * time.Second})
	if err != nil {
		return nil, err
	}
	defer pg.Close()
	return auditArchiveWith(ctx, pg, cfg, o)
}

func auditArchiveWith(ctx context.Context, st store.Store, cfg *Config, o AuditArchiveCLIOptions) (*AuditArchiveResult, error) {
	dbs, err := st.Settings(ctx)
	if err != nil {
		return nil, err
	}
	vals, _ := resolveSettings(cfg, dbs)
	before := o.Before
	if before.IsZero() {
		days := o.OlderThanDays
		if days <= 0 {
			days, _ = vals["audit_retention_days"].(int)
		}
		if days <= 0 {
			return nil, errors.New("audit_retention_days is 0 (keep forever): pass --older-than <days> or --before <date>")
		}
		before = time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	}
	dir, _ := vals["audit_archive_dir"].(string)
	if dir == "" {
		dir = filepath.Join(cfg.StateDir, "audit-archive")
	}
	res, err := ArchiveAuditRows(ctx, st, AuditArchiveOptions{Before: before, Out: o.Out, Dir: dir, DryRun: o.DryRun, By: "cli"})
	if err != nil {
		return nil, err
	}
	p, ts := res.Plan, func(t time.Time) string { return t.UTC().Format(time.RFC3339) }
	switch {
	case p.Count == 0:
		fmt.Fprintf(o.W, "no audit rows older than %s: nothing to archive\n", ts(before))
	case o.DryRun:
		fmt.Fprintf(o.W, "dry run: %d audit rows older than %s (ids %d..%d, %s .. %s) would be archived into %s\n",
			p.Count, ts(before), p.MinID, p.MaxID, ts(p.MinTS), ts(p.MaxTS), dir)
	default:
		fmt.Fprintf(o.W, "archived %d audit rows (ids %d..%d, %s .. %s)\n  file:     %s (%d bytes)\n  sha256:   %s\n  manifest: %s\nan audit.archive entry with the count, range and sha256 stays in the database\n",
			res.Deleted, p.MinID, p.MaxID, ts(p.MinTS), ts(p.MaxTS), res.File, res.Bytes, res.SHA256, res.Manifest)
	}
	return res, nil
}

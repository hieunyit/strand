package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/metrics"
	"strand/internal/mongocdc"
	"strand/internal/tunnel"
)

// mongoConfig maps the normal pipeline configuration to a MongoDB replication:
//
//	source/target.uri | dsn | uri_env MongoDB URIs (user + password | password_env optional)
//	source.database / target.database
//	tables.include / exclude         "db.collection" globs
//	bootstrap.logical.max_parallel   collections copied concurrently
//	mongodb.snapshot_batch           documents per snapshot batch (or bootstrap.logical.chunk_rows)
//	mongodb.stream_batch             change events per apply transaction (or target.batch.max_rows)
//	checkpoint.flush_every           idle checkpoint interval
//	mongodb.*                        overwrite_target, updates, indexes, lease_ttl
func mongoConfig(ctx context.Context, cfg *config.Config, logger *slog.Logger) (mongocdc.Config, func()) {
	mc := mongocdc.Config{
		Pipeline: cfg.Name, SourceURI: cfg.Source.DSN, TargetURI: cfg.Target.DSN,
		SourceDB: cfg.Source.Database, TargetDB: cfg.Target.Database,
		Include: cfg.Tables.Include, Exclude: cfg.Tables.Exclude,
		OverwriteTarget:  cfg.MongoDB.OverwriteTarget,
		Logger:           logger,
		SnapshotParallel: cfg.Bootstrap.Logical.MaxParallel,
		StreamBatch:      cfg.Target.Batch.MaxRows,
		UpdateMode:       cfg.MongoDB.Updates,
		ReplicateIndexes: cfg.MongoDB.Indexes == "replicate",
	}
	if n, err := strconv.Atoi(cfg.Bootstrap.Logical.ChunkRows); err == nil {
		mc.SnapshotBatch = n
	}
	if cfg.MongoDB.SnapshotBatch > 0 {
		mc.SnapshotBatch = cfg.MongoDB.SnapshotBatch
	}
	if cfg.MongoDB.StreamBatch > 0 {
		mc.StreamBatch = cfg.MongoDB.StreamBatch
	}
	if cfg.MongoDB.MaxTransactionMiB > 0 {
		mc.MaxTransactionBytes = cfg.MongoDB.MaxTransactionMiB << 20
	}
	if d, err := time.ParseDuration(cfg.Checkpoint.FlushEvery); err == nil {
		mc.CheckpointEvery = d
	}
	if d, err := time.ParseDuration(cfg.MongoDB.LeaseTTL); err == nil {
		mc.LeaseDuration = d
	}
	lifetime, closeTunnels := context.WithCancel(context.WithoutCancel(ctx))
	if ssh := cfg.Source.SSHTunnel; ssh != nil {
		mc.SourceDialer = tunnel.NewDialer(lifetime, func(ctx context.Context) (tunnel.Config, error) {
			return ssh.TunnelConfig(ctx, config.KindMongoDB)
		})
	}
	if ssh := cfg.Target.SSHTunnel; ssh != nil {
		mc.TargetDialer = tunnel.NewDialer(lifetime, func(ctx context.Context) (tunnel.Config, error) {
			return ssh.TunnelConfig(ctx, config.KindMongoDB)
		})
	}
	return mc, closeTunnels
}

// runMongoSync runs a MongoDB pipeline with the same admin socket and /metrics as SQL
// pipelines.
func runMongoSync(ctx context.Context, cfg *config.Config, logger *slog.Logger, events *admin.EventRing) error {
	for _, k := range cfg.MongoDBIgnoredKeys() {
		logger.Warn("configuration key has no effect on a MongoDB pipeline", "key", k)
	}
	mc, closeTunnels := mongoConfig(ctx, cfg, logger)
	defer closeTunnels()
	obs := mongocdc.NewObserver(cfg.Name)
	mc.Observer = obs
	m := metrics.NewMetrics(prometheus.NewRegistry())
	health := metrics.NewHealthChecker(2 * time.Minute)
	exporter := mongocdc.NewMetricsExporter(obs, m, health)
	if addr := cfg.Observability.Metrics; addr != "" {
		ms := metrics.NewServer(addr, m, health, logger)
		if err := ms.Start(); err != nil {
			return fmt.Errorf("start metrics server: %w", err)
		}
		defer ms.Close()
	}
	if sock := cfg.Observability.AdminSocket; sock != "" {
		h := &mongocdc.AdminHandler{Obs: obs, EventRing: events, SourceKind: "mongodb", TargetKind: "mongodb", SourceDB: cfg.Source.Database,
			Workers:    max(1, cfg.Bootstrap.Logical.MaxParallel),
			ListSource: func(ctx context.Context) ([]string, error) { return mongocdc.ListCollections(ctx, mc) }}
		as := admin.NewServer(sock, h, logger)
		if err := as.Start(); err != nil {
			return fmt.Errorf("start admin socket: %w", err)
		}
		defer as.Close()
	}
	expCtx, stopExp := context.WithCancel(ctx)
	defer stopExp()
	go exporter.Run(expCtx, time.Second)
	err := mongocdc.Run(ctx, mc)
	exporter.Export()
	switch {
	case err == nil, errors.Is(err, context.Canceled) && ctx.Err() != nil:
		logger.Info("MongoDB pipeline stopped", "pipeline", cfg.Name)
		return nil
	case errors.Is(err, mongocdc.ErrLeaseLost), errors.Is(err, mongocdc.ErrLeaseHeld):
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, err.Error(), err)
	case errors.Is(err, mongocdc.ErrResyncRequired):
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, err.Error(), err)
	}
	return err
}

// mongoValidate implements `strand validate` and `strand plan` for MongoDB pipelines.
func mongoValidate(ctx context.Context, out io.Writer, cfg *config.Config, planOnly, force bool) error {
	for _, k := range cfg.MongoDBIgnoredKeys() {
		fmt.Fprintf(out, "warning: %s has no effect on a MongoDB pipeline\n", k)
	}
	mc, closeTunnels := mongoConfig(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer closeTunnels()
	checks, plan, err := mongocdc.Preflight(ctx, mc)
	if err != nil {
		return err
	}
	if planOnly {
		fmt.Fprintf(out, "MongoDB snapshot plan for %s (%s -> %s):\n", cfg.Name, cfg.Source.Database, cfg.Target.Database)
		for _, c := range plan.Collections {
			fmt.Fprintf(out, "  %-40s %12d docs %10d MiB\n", c.Name, c.Documents, c.Bytes>>20)
		}
		fmt.Fprintf(out, "  total: %d documents, %d MiB; estimated copy %s; oplog window %s\n", plan.Documents, plan.Bytes>>20,
			plan.EstimatedCopy.Round(time.Second), fmtWindow(plan.OplogWindow))
	}
	var failed []string
	for _, c := range checks {
		if planOnly && c.Name != "oplog window" && c.Status != mongocdc.CheckFail {
			continue
		}
		if planOnly && c.Name == "oplog window" && c.Status == mongocdc.CheckFail && force {
			fmt.Fprintf(out, "[%s] %s: %s (--force)\n", mongocdc.CheckWarn, c.Name, c.Detail)
			continue
		}
		fmt.Fprintf(out, "[%s] %s: %s\n", c.Status, c.Name, c.Detail)
		if c.Status == mongocdc.CheckFail {
			failed = append(failed, c.Name)
		}
	}
	if len(failed) > 0 {
		return exitcode.New("validate_failed", exitcode.Rejected, "MongoDB preflight failed: "+strings.Join(failed, ", "), nil)
	}
	if !planOnly {
		fmt.Fprintln(out, "OK")
	}
	return nil
}

func fmtWindow(d time.Duration) string {
	if d <= 0 {
		return "unknown"
	}
	return d.Round(time.Second).String()
}

// mongoVerify implements `strand verify` for MongoDB pipelines: per collection count,
// sha256 over documents in _id order and a byte comparison by _id (full) or of a random
// sample; differences are re-read to absorb lag. --repair fix re-copies each differing
// collection through the running pipeline (admin socket).
func mongoVerify(ctx context.Context, out io.Writer, cfg *config.Config, table, mode, repair string, sample int) error {
	opts := mongocdc.VerifyOptions{}
	if table != "" {
		coll := table
		if db, c, ok := strings.Cut(table, "."); ok && db == cfg.Source.Database {
			coll = c
		}
		opts.Collections = []string{coll}
	}
	switch mode {
	case "sample":
		opts.Sample = sample
	case "full":
	default:
		return exitcode.New("invalid_mode", exitcode.InvalidSyntax, "--mode must be sample|full for MongoDB pipelines", nil)
	}
	mc, closeTunnels := mongoConfig(ctx, cfg, slog.Default())
	defer closeTunnels()
	reports, err := mongocdc.Verify(ctx, mc, opts)
	if err != nil {
		return err
	}
	var bad []string
	for _, r := range reports {
		status := "OK"
		if !r.OK() {
			status = "MISMATCH"
			bad = append(bad, r.Collection)
		}
		fmt.Fprintf(out, "%-8s %s.%s: source %d, target %d, compared %d, missing %d, extra %d, different %d",
			status, cfg.Source.Database, r.Collection, r.SourceCount, r.TargetCount, r.Compared, r.Missing, r.Extra, r.Different)
		if r.SourceHash != "" {
			fmt.Fprintf(out, ", sha256 %s/%s", r.SourceHash[:12], r.TargetHash[:12])
		}
		fmt.Fprintln(out)
		if len(r.DifferingIDs) > 0 {
			fmt.Fprintf(out, "         _id: %s\n", strings.Join(r.DifferingIDs, ", "))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	if repair == "fix" {
		cli := admin.NewClient(getAdminSocketPath())
		for _, c := range bad {
			if err := cli.SnapshotAdd(ctx, cfg.Source.Database+"."+c); err != nil {
				return fmt.Errorf("request re-copy of %s: %w", c, err)
			}
			fmt.Fprintf(out, "re-copy of %s.%s started in the running pipeline\n", cfg.Source.Database, c)
		}
		return nil
	}
	return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, "MongoDB verify found differences in: "+strings.Join(bad, ", "), nil)
}

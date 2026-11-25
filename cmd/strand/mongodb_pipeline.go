package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
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
//	source.database / target.database one database
//	source.databases / target.databases several databases (one change stream each)
//	tables.include / exclude         "db.collection" globs
//	bootstrap.logical.max_parallel   copy workers (collections, or _id ranges of a large one)
//	mongodb.split_min_docs           smallest _id range of a split collection
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
		SplitMinDocs:     cfg.MongoDB.SplitMinDocs,
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

// mongoDatabaseConfigs is mongoConfig split per database (mongocdc.DatabaseConfigs).
// The error is set when the checkpoint keys could not be read from the target; the
// configs are still usable for checks that report the connection failure themselves.
func mongoDatabaseConfigs(ctx context.Context, cfg *config.Config, logger *slog.Logger) ([]mongocdc.Config, func(), error) {
	mc, closeTunnels := mongoConfig(ctx, cfg, logger)
	var dbs []mongocdc.Database
	for _, d := range cfg.MongoDatabases() {
		dbs = append(dbs, mongocdc.Database{Source: d.Source, Target: d.Target})
	}
	cfgs, err := mongocdc.DatabaseConfigs(ctx, mc, dbs)
	if cfgs == nil {
		closeTunnels()
		return nil, func() {}, err
	}
	return cfgs, closeTunnels, err
}

// mongoDatabaseLabel names the databases of a pipeline in messages: "app -> app_copy".
func mongoDatabaseLabel(cfg *config.Config) string {
	var parts []string
	for _, d := range cfg.MongoDatabases() {
		if d.Source == d.Target {
			parts = append(parts, d.Source)
		} else {
			parts = append(parts, d.Source+" -> "+d.Target)
		}
	}
	return strings.Join(parts, ", ")
}

// runMongoSync runs a MongoDB pipeline with the same admin socket and /metrics as SQL
// pipelines.
func runMongoSync(ctx context.Context, cfg *config.Config, logger *slog.Logger, events *admin.EventRing) error {
	for _, k := range cfg.MongoDBIgnoredKeys() {
		logger.Warn("configuration key has no effect on a MongoDB pipeline", "key", k)
	}
	cfgs, closeTunnels, err := mongoDatabaseConfigs(ctx, cfg, logger)
	defer closeTunnels()
	if err != nil {
		return err
	}
	m := metrics.NewMetrics(prometheus.NewRegistry())
	health := metrics.NewHealthChecker(2 * time.Minute)
	var exporters []*mongocdc.MetricsExporter
	var parts []mongocdc.AdminPart
	for i := range cfgs {
		obs := mongocdc.NewObserver(cfgs[i].Pipeline)
		cfgs[i].Observer = obs
		exporters = append(exporters, mongocdc.NewMetricsExporter(obs, m, health))
		mc := cfgs[i]
		parts = append(parts, mongocdc.AdminPart{SourceDB: mc.SourceDB, Obs: obs,
			ListSource: func(ctx context.Context) ([]string, error) { return mongocdc.ListCollections(ctx, mc) }})
	}
	if addr := cfg.Observability.Metrics; addr != "" {
		ms := metrics.NewServer(addr, m, health, logger)
		if err := ms.Start(); err != nil {
			return fmt.Errorf("start metrics server: %w", err)
		}
		defer ms.Close()
	}
	if sock := cfg.Observability.AdminSocket; sock != "" {
		h := &mongocdc.AdminHandler{Parts: parts, Pipeline: cfg.Name, EventRing: events, SourceKind: "mongodb", TargetKind: "mongodb",
			Workers: max(1, cfg.Bootstrap.Logical.MaxParallel)}
		as := admin.NewServer(sock, h, logger)
		if err := as.Start(); err != nil {
			return fmt.Errorf("start admin socket: %w", err)
		}
		defer as.Close()
	}
	expCtx, stopExp := context.WithCancel(ctx)
	defer stopExp()
	for _, e := range exporters {
		go e.Run(expCtx, time.Second)
	}
	if len(cfgs) > 1 {
		logger.Info("MongoDB pipeline replicates several databases, one change stream each", "pipeline", cfg.Name, "databases", mongoDatabaseLabel(cfg))
	}
	err = mongocdc.RunAll(ctx, cfgs)
	for _, e := range exporters {
		e.Export()
	}
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
	cfgs, closeTunnels, _ := mongoDatabaseConfigs(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer closeTunnels()
	checks, plan, err := mongoPreflight(ctx, cfgs)
	if err != nil {
		return err
	}
	if planOnly {
		fmt.Fprintf(out, "MongoDB snapshot plan for %s (%s):\n", cfg.Name, mongoDatabaseLabel(cfg))
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

// mongoPreflight runs the preflight of every database. With several databases the
// checks that differ per database name it ("app: ..."), identical ones (topology,
// update mode) are shown once, collection names are "database.collection", and the
// estimated copy is the longest one (the databases are copied at the same time).
func mongoPreflight(ctx context.Context, cfgs []mongocdc.Config) ([]mongocdc.Check, mongocdc.Plan, error) {
	if len(cfgs) == 1 {
		return mongocdc.Preflight(ctx, cfgs[0])
	}
	var all []mongocdc.Check
	var plan mongocdc.Plan
	seen := map[mongocdc.Check]bool{}
	for _, c := range cfgs {
		checks, p, err := mongocdc.Preflight(ctx, c)
		if err != nil {
			return nil, plan, fmt.Errorf("database %s: %w", c.SourceDB, err)
		}
		for _, ch := range checks {
			if seen[ch] {
				continue
			}
			seen[ch] = true
			switch ch.Name {
			case "source topology", "target topology", "source connection", "target connection", "update mode":
			default:
				ch.Detail = c.SourceDB + ": " + ch.Detail
			}
			all = append(all, ch)
		}
		for _, pc := range p.Collections {
			pc.Name = c.SourceDB + "." + pc.Name
			plan.Collections = append(plan.Collections, pc)
		}
		plan.Documents += p.Documents
		plan.Bytes += p.Bytes
		plan.EstimatedCopy = max(plan.EstimatedCopy, p.EstimatedCopy)
		if p.OplogWindow > 0 && (plan.OplogWindow <= 0 || p.OplogWindow < plan.OplogWindow) {
			plan.OplogWindow = p.OplogWindow
		}
		plan.CheckpointFound = plan.CheckpointFound || p.CheckpointFound
	}
	sort.SliceStable(plan.Collections, func(i, j int) bool { return plan.Collections[i].Documents > plan.Collections[j].Documents })
	return all, plan, nil
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
	switch mode {
	case "sample", "full":
	default:
		return exitcode.New("invalid_mode", exitcode.InvalidSyntax, "--mode must be sample|full for MongoDB pipelines", nil)
	}
	cfgs, closeTunnels, err := mongoDatabaseConfigs(ctx, cfg, slog.Default())
	defer closeTunnels()
	if err != nil {
		return err
	}
	if table != "" {
		db, coll, ok := strings.Cut(table, ".")
		var pick []mongocdc.Config
		for _, c := range cfgs {
			if ok && c.SourceDB == db {
				pick = []mongocdc.Config{c}
				break
			}
		}
		switch {
		case pick == nil && len(cfgs) == 1:
			pick, coll = cfgs, table
			if ok && db == cfgs[0].SourceDB {
				coll = table[len(db)+1:]
			}
		case pick == nil && ok:
			return exitcode.New("invalid_table", exitcode.InvalidSyntax, fmt.Sprintf("--table %s: database %q is not part of this pipeline (%s)", table, db, mongoDatabaseLabel(cfg)), nil)
		case pick == nil:
			return exitcode.New("invalid_table", exitcode.InvalidSyntax, fmt.Sprintf("--table %s: name it database.collection, this pipeline replicates several databases (%s)", table, mongoDatabaseLabel(cfg)), nil)
		}
		cfgs, table = pick, coll
	}
	var bad []string
	for _, mc := range cfgs {
		opts := mongocdc.VerifyOptions{}
		if table != "" {
			opts.Collections = []string{table}
		}
		if mode == "sample" {
			opts.Sample = sample
		}
		reports, err := mongocdc.Verify(ctx, mc, opts)
		if err != nil {
			if len(cfgs) > 1 {
				return fmt.Errorf("verify database %s: %w", mc.SourceDB, err)
			}
			return err
		}
		for _, r := range reports {
			status := "OK"
			if !r.OK() {
				status = "MISMATCH"
				bad = append(bad, mc.SourceDB+"."+r.Collection)
			}
			fmt.Fprintf(out, "%-8s %s.%s: source %d, target %d, compared %d, missing %d, extra %d, different %d",
				status, mc.SourceDB, r.Collection, r.SourceCount, r.TargetCount, r.Compared, r.Missing, r.Extra, r.Different)
			if r.SourceHash != "" {
				fmt.Fprintf(out, ", sha256 %s/%s", r.SourceHash[:12], r.TargetHash[:12])
			}
			fmt.Fprintln(out)
			if len(r.DifferingIDs) > 0 {
				fmt.Fprintf(out, "         _id: %s\n", strings.Join(r.DifferingIDs, ", "))
			}
		}
	}
	if len(bad) == 0 {
		return nil
	}
	if repair == "fix" {
		cli := admin.NewClient(getAdminSocketPath())
		for _, c := range bad {
			if err := cli.SnapshotAdd(ctx, c); err != nil {
				return fmt.Errorf("request re-copy of %s: %w", c, err)
			}
			fmt.Fprintf(out, "re-copy of %s started in the running pipeline\n", c)
		}
		return nil
	}
	return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, "MongoDB verify found differences in: "+strings.Join(bad, ", "), nil)
}

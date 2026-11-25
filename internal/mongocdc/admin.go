package mongocdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/admin"
	"strand/internal/metrics"
)

// AdminHandler serves the SQL pipelines' admin socket protocol (top, status, tables,
// events, checkpoint, pause/resume, snapshot add = collection re-copy) for a MongoDB
// pipeline, so `strand top/pause/resume/checkpoint` and the Web UI work unchanged.
// A pipeline with several databases has one part per database; the handler reports
// them as one pipeline with "database.collection" table names.
type AdminHandler struct {
	// Parts: one per source database. When empty, Obs/SourceDB/ListSource are the
	// only part.
	Parts      []AdminPart
	Pipeline   string // reported pipeline ID (default: the observer's)
	Obs        *Observer
	EventRing  *admin.EventRing
	SourceKind string
	TargetKind string
	SourceDB   string
	Workers    int
	ListSource func(ctx context.Context) ([]string, error) // optional
}

// AdminPart is the observer of one database of a pipeline.
type AdminPart struct {
	SourceDB   string
	Obs        *Observer
	ListSource func(ctx context.Context) ([]string, error) // collections of SourceDB (optional)
}

func (h *AdminHandler) parts() []AdminPart {
	if len(h.Parts) > 0 {
		return h.Parts
	}
	return []AdminPart{{SourceDB: h.SourceDB, Obs: h.Obs, ListSource: h.ListSource}}
}

// snapshot merges the parts' state: collection names are qualified with their
// database, counters and rates add up, lag is the largest, and the phase is the
// least advanced one (a failure or retry anywhere shows for the whole pipeline).
func (h *AdminHandler) snapshot() Snapshot {
	parts := h.parts()
	var m Snapshot
	var phases []Phase
	var positions, failures, retryErrs []string
	for i, p := range parts {
		s := p.Obs.Snapshot()
		if i == 0 {
			m = Snapshot{Pipeline: s.Pipeline, Started: s.Started, EventsByOp: map[string]int64{}, OplogWindow: s.OplogWindow}
		}
		phases = append(phases, s.Phase)
		prefix := func(v string) string {
			if len(parts) == 1 {
				return v
			}
			return p.SourceDB + ": " + v
		}
		if s.Failed != "" {
			failures = append(failures, prefix(s.Failed))
		}
		if s.LastRetryError != "" {
			retryErrs = append(retryErrs, prefix(s.LastRetryError))
		}
		if s.Position != "" {
			positions = append(positions, prefix(s.Position))
		}
		if s.Started.Before(m.Started) {
			m.Started = s.Started
		}
		m.Paused = m.Paused || s.Paused
		m.SnapshotPaused = m.SnapshotPaused || s.SnapshotPaused
		m.LagSeconds = max(m.LagSeconds, s.LagSeconds)
		m.CaughtUp = i == 0 && s.CaughtUp || i > 0 && m.CaughtUp && s.CaughtUp
		if s.LastEventTime.After(m.LastEventTime) {
			m.LastEventTime = s.LastEventTime
		}
		m.Events += s.Events
		m.DocWrites += s.DocWrites
		m.Txns += s.Txns
		m.SnapshotDocs += s.SnapshotDocs
		m.Lookups += s.Lookups
		m.Retries += s.Retries
		m.Fenced += s.Fenced
		m.EventRate += s.EventRate
		m.DocRate += s.DocRate
		m.SnapshotRate += s.SnapshotRate
		if s.OplogWindow > 0 && (m.OplogWindow <= 0 || s.OplogWindow < m.OplogWindow) {
			m.OplogWindow = s.OplogWindow
		}
		for op, n := range s.EventsByOp {
			m.EventsByOp[op] += n
		}
		for _, c := range s.Collections {
			c.Name = p.SourceDB + "." + c.Name
			m.Collections = append(m.Collections, c)
		}
	}
	if h.Pipeline != "" {
		m.Pipeline = h.Pipeline
	}
	m.Phase = mergePhases(phases)
	m.Failed = strings.Join(failures, "; ")
	m.LastRetryError = strings.Join(retryErrs, "; ")
	m.Position = strings.Join(positions, "; ")
	return m
}

// mergePhases is the phase shown for several databases: the first of retrying,
// starting, snapshot/repair, stopped (any database stopped means the pipeline is
// stopping) found, else streaming.
func mergePhases(phases []Phase) Phase {
	for _, want := range []Phase{PhaseRetrying, PhaseStarting, PhaseSnapshot, PhaseRepair, PhaseStopped} {
		for _, p := range phases {
			if p == want {
				return p
			}
		}
	}
	if len(phases) == 0 {
		return PhaseStarting
	}
	return phases[0]
}

// part finds the part of "db.collection" (or of a bare collection name when the
// pipeline has one database).
func (h *AdminHandler) part(table string) (AdminPart, string, error) {
	parts := h.parts()
	if db, c, ok := strings.Cut(table, "."); ok {
		for _, p := range parts {
			if p.SourceDB == db {
				return p, c, nil
			}
		}
		if len(parts) > 1 {
			return AdminPart{}, "", fmt.Errorf("database %q is not part of this pipeline", db)
		}
	}
	if len(parts) > 1 {
		return AdminPart{}, "", fmt.Errorf("name the collection as database.collection: this pipeline replicates %d databases", len(parts))
	}
	return parts[0], table, nil
}

var (
	_ admin.PipelineAdminHandler = (*AdminHandler)(nil)
	_ admin.StatusProvider       = (*AdminHandler)(nil)
	_ admin.TablesProvider       = (*AdminHandler)(nil)
	_ admin.EventsProvider       = (*AdminHandler)(nil)
	_ admin.SnapshotAdder        = (*AdminHandler)(nil)
	_ admin.SnapshotPauser       = (*AdminHandler)(nil)
	_ admin.SourceTablesProvider = (*AdminHandler)(nil)
)

// State maps the phase to the admin-socket state names used by SQL pipelines.
func (s Snapshot) State() string {
	switch {
	case s.Phase == PhaseStopped && s.Failed != "":
		return "failed"
	case s.Paused:
		return "paused"
	case s.Phase == PhaseStarting:
		return "starting"
	case s.Phase == PhaseSnapshot, s.Phase == PhaseRepair:
		return "bootstrapping"
	case s.Phase == PhaseRetrying:
		return "retrying"
	case s.Phase == PhaseStopped:
		return "stopped"
	default:
		return "synced"
	}
}

func (s Snapshot) snapshotTotals() (done, total, copied, estimate int64, running []string) {
	for _, c := range s.Collections {
		total++
		copied += c.Copied
		estimate += max(c.Estimate, c.Copied)
		switch c.State {
		case CollDone:
			done++
		case CollRunning:
			running = append(running, c.Name)
		}
	}
	return
}

// Top implements admin `top`.
func (h *AdminHandler) Top(context.Context) (*admin.TopInfo, error) {
	s := h.snapshot()
	info := &admin.TopInfo{PipelineID: s.Pipeline, State: s.State(), LagSeconds: s.LagSeconds, Throughput: s.EventRate, Zone: "green"}
	if s.OplogWindow > 0 && s.LagSeconds > s.OplogWindow.Seconds()/2 {
		info.Zone = "bootstrap_retention_risk"
	}
	done, total, copied, estimate, running := s.snapshotTotals()
	if total > 0 && (s.Phase == PhaseSnapshot || s.Phase == PhaseRepair || done < total) {
		info.TablesTotal, info.TablesDone = total, done
		info.SnapshotTables, info.SnapshotTable = running, strings.Join(running, ", ")
		info.SnapshotRows, info.SnapshotRowsTotal, info.SnapshotRate = copied, estimate, s.SnapshotRate
		if estimate > 0 {
			info.Progress = min(100, 100*float64(copied)/float64(estimate))
		}
		if s.SnapshotRate > 0 && estimate > copied {
			info.ETASeconds = float64(estimate-copied) / s.SnapshotRate
		}
	} else if s.Phase == PhaseStreaming {
		info.Progress = 100
	}
	return info, nil
}

// Pause implements admin `pause`: stop applying after the current batch.
func (h *AdminHandler) Pause(context.Context) (*admin.PauseResult, error) {
	for _, p := range h.parts() {
		p.Obs.Pause(false)
	}
	return &admin.PauseResult{State: "paused", Paused: true}, nil
}

// PauseSnapshot implements admin `pause_snapshot`: stop copying collections only.
func (h *AdminHandler) PauseSnapshot(context.Context) (*admin.PauseResult, error) {
	for _, p := range h.parts() {
		p.Obs.Pause(true)
	}
	return &admin.PauseResult{State: h.snapshot().State(), Paused: true}, nil
}

// Resume implements admin `resume`.
func (h *AdminHandler) Resume(_ context.Context, snapshotOnly bool) (*admin.ResumeResult, error) {
	for _, p := range h.parts() {
		p.Obs.Resume(snapshotOnly)
	}
	return &admin.ResumeResult{State: h.snapshot().State(), SnapshotOnly: snapshotOnly, Resumed: true}, nil
}

// ThrottleSet is not supported: MongoDB pipelines have no IOPS governor.
func (h *AdminHandler) ThrottleSet(context.Context, admin.Request) (*admin.ThrottleResult, error) {
	return nil, errors.New("throttle is not supported for MongoDB pipelines; bound the snapshot with bootstrap.logical.max_parallel and chunk_rows")
}

// CheckpointShow implements admin `checkpoint_show`: the committed resume token.
func (h *AdminHandler) CheckpointShow(context.Context) (*admin.CheckpointResult, error) {
	parts := h.parts()
	var applied []string
	for _, p := range parts {
		s := p.Obs.Snapshot()
		a := s.Position
		if !s.LastEventTime.IsZero() {
			a = fmt.Sprintf("resume_token=%s cluster_time=%s", s.Position, s.LastEventTime.UTC().Format(time.RFC3339))
		}
		if len(parts) > 1 {
			a = p.SourceDB + ": " + a
		}
		applied = append(applied, a)
	}
	return &admin.CheckpointResult{PipelineID: h.snapshot().Pipeline, Applied: strings.Join(applied, "; "), UpdatedAt: time.Now().UTC()}, nil
}

// CheckpointRewind is refused: a MongoDB checkpoint is a change-stream resume token.
func (h *AdminHandler) CheckpointRewind(context.Context, string) (*admin.CheckpointResult, error) {
	return nil, errors.New("checkpoint rewind is not supported for MongoDB pipelines; re-copy a collection with `strand snapshot add --table db.collection` instead")
}

// SnapshotAdd re-copies one collection ("db.collection" or "collection").
func (h *AdminHandler) SnapshotAdd(ctx context.Context, table string) error {
	p, coll, err := h.part(table)
	if err != nil {
		return err
	}
	return p.Obs.RequestRecopy(ctx, coll)
}

// Status implements admin `status`.
func (h *AdminHandler) Status(context.Context) (*admin.StatusInfo, error) {
	s := h.snapshot()
	return &admin.StatusInfo{
		PipelineID: s.Pipeline, State: s.State(), StartedAt: s.Started, UptimeSeconds: time.Since(s.Started).Seconds(),
		StartPosition: s.Position, SourceKind: h.SourceKind, TargetKind: h.TargetKind, CommitOrder: "source-transaction",
		Workers: h.Workers, Paused: s.Paused, SnapshotPaused: s.SnapshotPaused, AppliedTxns: s.Txns, AppliedRows: s.DocWrites,
		Governor: admin.GovernorInfo{Algorithm: "none", Zone: "green"},
	}, nil
}

// Tables implements admin `tables`: per-collection copy progress.
func (h *AdminHandler) Tables(context.Context) ([]admin.TableInfo, error) {
	s := h.snapshot()
	out := make([]admin.TableInfo, 0, len(s.Collections))
	for _, c := range s.Collections {
		ti := admin.TableInfo{Table: c.Name, State: c.State, RowsEstimate: c.Estimate, RowsCopied: c.Copied,
			PK: []string{"_id"}, Key: "_id", GenericPK: true}
		switch {
		case c.State == CollDone:
			ti.ProgressPct = 100
		case c.Estimate > 0:
			ti.ProgressPct = min(99.9, 100*float64(c.Copied)/float64(c.Estimate))
			if el, n := time.Since(c.Started).Seconds(), c.Copied-c.StartCopied; c.State == CollRunning && n > 0 && el > 0 && c.Estimate > c.Copied {
				ti.ETASeconds = float64(c.Estimate-c.Copied) / (float64(n) / el)
			}
		}
		out = append(out, ti)
	}
	return out, nil
}

// Events implements admin `events` (recent WARN/ERROR log records).
func (h *AdminHandler) Events(context.Context) ([]admin.EventRecord, error) {
	return h.EventRing.Snapshot(), nil
}

// SourceTables lists the source collections ("db.collection").
func (h *AdminHandler) SourceTables(ctx context.Context) ([]admin.SourceTable, error) {
	in := map[string]bool{}
	for _, c := range h.snapshot().Collections {
		in[c.Name] = true
	}
	var out []admin.SourceTable
	for _, p := range h.parts() {
		if p.ListSource == nil {
			return nil, errors.New("source_tables is not available")
		}
		names, err := p.ListSource(ctx)
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			t := p.SourceDB + "." + n
			out = append(out, admin.SourceTable{Table: t, InPipeline: in[t]})
		}
	}
	return out, nil
}

// MetricsExporter publishes Observer state as the SQL pipelines' Prometheus metrics
// (strand_lag_seconds, strand_apply_txn_total, strand_apply_rows_total,
// strand_seed_progress_ratio, strand_snapshot_ranges, strand_retention_headroom_seconds,
// strand_fenced_total) plus MongoDB counters:
//
//	strand_mongodb_events_total{pipeline,op}     change events applied, by operation type
//	strand_mongodb_snapshot_docs_total{pipeline} documents copied by snapshot/re-copy
//	strand_mongodb_lookups_total{pipeline}       update events resolved by a source lookup
//	strand_mongodb_retries_total{pipeline}       automatic restarts after transient errors
//	strand_mongodb_up{pipeline}                  1 while streaming or copying, 0 otherwise
type MetricsExporter struct {
	obs                                 *Observer
	m                                   *metrics.Metrics
	health                              *metrics.HealthChecker
	events                              *prometheus.CounterVec
	snapDocs, lookups, retries          prometheus.Counter
	up                                  prometheus.Gauge
	lastTxns, lastDocs, lastSnap        int64
	lastLookups, lastRetries, lastFence int64
	lastOps                             map[string]int64
}

// NewMetricsExporter registers the MongoDB counters on m.Registry.
func NewMetricsExporter(obs *Observer, m *metrics.Metrics, health *metrics.HealthChecker) *MetricsExporter {
	p := prometheus.Labels{"pipeline": obs.pipeline}
	e := &MetricsExporter{obs: obs, m: m, health: health, lastOps: map[string]int64{},
		events:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "strand_mongodb_events_total", Help: "MongoDB change events applied, by operation type.", ConstLabels: p}, []string{"op"}),
		snapDocs: prometheus.NewCounter(prometheus.CounterOpts{Name: "strand_mongodb_snapshot_docs_total", Help: "Documents copied by the MongoDB snapshot and collection re-copies.", ConstLabels: p}),
		lookups:  prometheus.NewCounter(prometheus.CounterOpts{Name: "strand_mongodb_lookups_total", Help: "Update events resolved by reading the current document from the source.", ConstLabels: p}),
		retries:  prometheus.NewCounter(prometheus.CounterOpts{Name: "strand_mongodb_retries_total", Help: "Automatic restarts from the last checkpoint after transient errors.", ConstLabels: p}),
		up:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "strand_mongodb_up", Help: "1 while the MongoDB pipeline is copying or streaming.", ConstLabels: p}),
	}
	m.Registry.MustRegister(e.events, e.snapDocs, e.lookups, e.retries, e.up)
	return e
}

// Run updates the metrics every interval until ctx ends.
func (e *MetricsExporter) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		e.Export()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Export publishes the current state once.
func (e *MetricsExporter) Export() {
	s := e.obs.Snapshot()
	p := s.Pipeline
	phase := "synced"
	if s.Phase == PhaseSnapshot || s.Phase == PhaseRepair || s.Phase == PhaseStarting {
		phase = "bootstrap"
	}
	e.m.LagSeconds.WithLabelValues(p, "bootstrap").Set(0)
	e.m.LagSeconds.WithLabelValues(p, "synced").Set(0)
	e.m.LagSeconds.WithLabelValues(p, phase).Set(s.LagSeconds)
	add := func(c prometheus.Counter, now int64, last *int64) {
		if now > *last {
			c.Add(float64(now - *last))
		}
		*last = now
	}
	add(e.m.ApplyTxnTotal.WithLabelValues(p), s.Txns, &e.lastTxns)
	add(e.m.ApplyRowsTotal.WithLabelValues(p), s.DocWrites, &e.lastDocs)
	add(e.m.FencedTotal.WithLabelValues(p), s.Fenced, &e.lastFence)
	add(e.snapDocs, s.SnapshotDocs, &e.lastSnap)
	add(e.lookups, s.Lookups, &e.lastLookups)
	add(e.retries, s.Retries, &e.lastRetries)
	for op, n := range s.EventsByOp {
		last := e.lastOps[op]
		add(e.events.WithLabelValues(op), n, &last)
		e.lastOps[op] = last
	}
	for _, c := range s.Collections {
		ratio := 0.0
		switch {
		case c.State == CollDone:
			ratio = 1
		case c.Estimate > 0:
			ratio = min(0.999, float64(c.Copied)/float64(c.Estimate))
		}
		e.m.SeedProgressRatio.WithLabelValues(p, c.Name).Set(ratio)
		for _, st := range []string{CollPending, CollRunning, CollDone} {
			v := 0.0
			if c.State == st {
				v = 1
			}
			e.m.SnapshotRanges.WithLabelValues(p, c.Name, st).Set(v)
		}
	}
	if s.OplogWindow > 0 {
		e.m.RetentionHeadroomSeconds.WithLabelValues(p).Set(s.OplogWindow.Seconds() - s.LagSeconds)
	}
	up := 0.0
	if s.Phase == PhaseSnapshot || s.Phase == PhaseStreaming || s.Phase == PhaseRepair {
		up = 1
		if e.health != nil {
			e.health.RecordProgress()
		}
	}
	e.up.Set(up)
	if e.health != nil && s.Phase == PhaseStopped && s.Failed != "" {
		e.health.SetFailed(s.Failed)
	}
}

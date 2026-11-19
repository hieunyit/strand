package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// MetricSample is one sampler point (raw) or one bucket (aggregated; LagMax = max lag in the bucket).
type MetricSample struct {
	Pipeline   string
	T          time.Time
	State      string
	Lag        float64
	LagMax     float64
	Throughput float64
	ApplyRate  float64
	Rows       float64 // rows/s đã làm mượt (số hiển thị ở UI)
	SnapRows   float64 // snapshot rows/s đã làm mượt
	Holes      float64
	Writeset   float64
	SpoolBytes float64
}

// Metrics history retention.
const (
	MetricsRawRetention    = 24 * time.Hour
	MetricsRollupRetention = 7 * 24 * time.Hour
)

// MetricsStore is the metrics-history part of Store.
type MetricsStore interface {
	// AppendMetrics inserts raw samples (duplicates of (pipeline, ts) are ignored).
	AppendMetrics(ctx context.Context, rows []MetricSample) error
	// QueryMetrics returns buckets of width step in [from, to], oldest first; rollup=true reads the 1-minute
	// table (data older than the raw retention), otherwise raw samples.
	QueryMetrics(ctx context.Context, pipeline string, from, to time.Time, step time.Duration, rollup bool) ([]MetricSample, error)
	// MaintainMetrics recomputes 1-minute rollups of raw samples older than rollupBefore (idempotent), then
	// deletes raw samples older than rawBefore and rollups older than rollupKeep.
	MaintainMetrics(ctx context.Context, rollupBefore, rawBefore, rollupKeep time.Time) error
}

// ---- PG ----

func (p *PG) AppendMetrics(ctx context.Context, rows []MetricSample) error {
	if len(rows) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO metric_samples (pipeline, ts, state, lag, throughput, apply_rate, rows_rate, snap_rate, holes, writeset, spool_bytes) VALUES `)
	args := make([]any, 0, len(rows)*11)
	for i, r := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		b := i * 11
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9, b+10, b+11)
		args = append(args, r.Pipeline, r.T, r.State, r.Lag, r.Throughput, r.ApplyRate, r.Rows, r.SnapRows, r.Holes, r.Writeset, r.SpoolBytes)
	}
	sb.WriteString(` ON CONFLICT (pipeline, ts) DO NOTHING`)
	_, err := p.db.ExecContext(ctx, sb.String(), args...)
	return p.wrap(err)
}

func (p *PG) QueryMetrics(ctx context.Context, pipeline string, from, to time.Time, step time.Duration, rollup bool) ([]MetricSample, error) {
	sec := step.Seconds()
	if sec < 1 {
		sec = 1
	}
	table, lagMax := "metric_samples", "max(lag)"
	if rollup {
		table, lagMax = "metric_samples_1m", "max(lag_max)"
	}
	q := `SELECT to_timestamp(floor(extract(epoch FROM ts) / $4) * $4) AS b,
		(array_agg(state ORDER BY ts DESC))[1], avg(lag), ` + lagMax + `, avg(throughput), avg(apply_rate), avg(rows_rate), avg(snap_rate), avg(holes), avg(writeset), avg(spool_bytes)
		FROM ` + table + ` WHERE pipeline = $1 AND ts >= $2 AND ts <= $3 GROUP BY b ORDER BY b`
	rows, err := p.db.QueryContext(ctx, q, pipeline, from, to, sec)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []MetricSample
	for rows.Next() {
		m := MetricSample{Pipeline: pipeline}
		if err := rows.Scan(&m.T, &m.State, &m.Lag, &m.LagMax, &m.Throughput, &m.ApplyRate, &m.Rows, &m.SnapRows, &m.Holes, &m.Writeset, &m.SpoolBytes); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, m)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) MaintainMetrics(ctx context.Context, rollupBefore, rawBefore, rollupKeep time.Time) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return p.wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck
	// Chỉ các phút đã đóng (trước rollupBefore) và còn raw; tính lại là idempotent.
	if _, err := tx.ExecContext(ctx, `INSERT INTO metric_samples_1m
		(pipeline, ts, state, lag, lag_max, throughput, apply_rate, rows_rate, snap_rate, holes, writeset, spool_bytes)
		SELECT pipeline, date_trunc('minute', ts) AS m, (array_agg(state ORDER BY ts DESC))[1], avg(lag), max(lag),
		       avg(throughput), avg(apply_rate), avg(rows_rate), avg(snap_rate), avg(holes), avg(writeset), avg(spool_bytes)
		FROM metric_samples WHERE ts < date_trunc('minute', $1::timestamptz) GROUP BY pipeline, m
		ON CONFLICT (pipeline, ts) DO UPDATE SET state = EXCLUDED.state, lag = EXCLUDED.lag, lag_max = EXCLUDED.lag_max,
		  throughput = EXCLUDED.throughput, apply_rate = EXCLUDED.apply_rate, rows_rate = EXCLUDED.rows_rate, snap_rate = EXCLUDED.snap_rate, holes = EXCLUDED.holes,
		  writeset = EXCLUDED.writeset, spool_bytes = EXCLUDED.spool_bytes`, rollupBefore); err != nil {
		return p.wrap(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM metric_samples WHERE ts < $1`, rawBefore); err != nil {
		return p.wrap(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM metric_samples_1m WHERE ts < $1`, rollupKeep); err != nil {
		return p.wrap(err)
	}
	return p.wrap(tx.Commit())
}

// ---- Mem ----

func (m *Mem) AppendMetrics(_ context.Context, rows []MetricSample) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.metrics == nil {
		m.metrics = map[string]map[int64]MetricSample{}
	}
	for _, r := range rows {
		if m.metrics[r.Pipeline] == nil {
			m.metrics[r.Pipeline] = map[int64]MetricSample{}
		}
		k := r.T.UnixNano()
		if _, ok := m.metrics[r.Pipeline][k]; !ok {
			r.LagMax = r.Lag
			m.metrics[r.Pipeline][k] = r
		}
	}
	return nil
}

func bucketize(src []MetricSample, pipeline string, from, to time.Time, step time.Duration) []MetricSample {
	if step < time.Second {
		step = time.Second
	}
	type acc struct {
		n                               float64
		s                               MetricSample
		last                            time.Time
		lag, thr, app, holes, ws, spool float64
		rows, snap                      float64
		lagMax                          float64
	}
	bk := map[int64]*acc{}
	for _, r := range src {
		if r.T.Before(from) || r.T.After(to) {
			continue
		}
		b := r.T.Unix() / int64(step.Seconds()) * int64(step.Seconds())
		a := bk[b]
		if a == nil {
			a = &acc{}
			bk[b] = a
		}
		a.n++
		a.lag += r.Lag
		a.thr += r.Throughput
		a.app += r.ApplyRate
		a.rows += r.Rows
		a.snap += r.SnapRows
		a.holes += r.Holes
		a.ws += r.Writeset
		a.spool += r.SpoolBytes
		if r.LagMax > a.lagMax {
			a.lagMax = r.LagMax
		}
		if !r.T.Before(a.last) {
			a.last, a.s.State = r.T, r.State
		}
	}
	var keys []int64
	for k := range bk {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]MetricSample, 0, len(keys))
	for _, k := range keys {
		a := bk[k]
		out = append(out, MetricSample{Pipeline: pipeline, T: time.Unix(k, 0).UTC(), State: a.s.State, Lag: a.lag / a.n, LagMax: a.lagMax,
			Throughput: a.thr / a.n, ApplyRate: a.app / a.n, Rows: a.rows / a.n, SnapRows: a.snap / a.n, Holes: a.holes / a.n, Writeset: a.ws / a.n, SpoolBytes: a.spool / a.n})
	}
	return out
}

func (m *Mem) QueryMetrics(_ context.Context, pipeline string, from, to time.Time, step time.Duration, rollup bool) ([]MetricSample, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	src := m.metrics[pipeline]
	if rollup {
		src = m.metrics1m[pipeline]
	}
	all := make([]MetricSample, 0, len(src))
	for _, r := range src {
		all = append(all, r)
	}
	return bucketize(all, pipeline, from, to, step), nil
}

func (m *Mem) MaintainMetrics(_ context.Context, rollupBefore, rawBefore, rollupKeep time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.metrics1m == nil {
		m.metrics1m = map[string]map[int64]MetricSample{}
	}
	cut := rollupBefore.Truncate(time.Minute)
	for p, rows := range m.metrics {
		var old []MetricSample
		for _, r := range rows {
			if r.T.Before(cut) {
				old = append(old, r)
			}
		}
		if len(old) > 0 {
			if m.metrics1m[p] == nil {
				m.metrics1m[p] = map[int64]MetricSample{}
			}
			for _, b := range bucketize(old, p, time.Time{}, cut, time.Minute) {
				m.metrics1m[p][b.T.UnixNano()] = b
			}
		}
		for k, r := range rows {
			if r.T.Before(rawBefore) {
				delete(rows, k)
			}
		}
	}
	for _, rows := range m.metrics1m {
		for k, r := range rows {
			if r.T.Before(rollupKeep) {
				delete(rows, k)
			}
		}
	}
	return nil
}

// MetricsCount: số mẫu raw và rollup (test).
func (m *Mem) MetricsCount() (raw, rollup int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.metrics {
		raw += len(r)
	}
	for _, r := range m.metrics1m {
		rollup += len(r)
	}
	return
}

package web

import (
	"math/bits"
	"sync"
	"time"

	"strand/internal/admin"
)

// Per-table CDC history kept by the Web process (not by the pipeline): the sampler feeds it once per sample with
// the table list it already read, and the table page draws "changes per minute" and "sync delay" from it. It is
// also what the quiet-table rule (below) looks at.
//
// Memory bound (per pipeline):
//   - a table that never had a CDC change recorded has NO entry;
//   - a table with a change recorded has one tblHist (~100 B plus its map entry, ~170 B in all);
//   - a table with a change in the last histBuckets minutes also has one tblSeries (2 × 60 × 4 B + 8 B = 488 B),
//     dropped again once its newest bucket falls out of the window;
//   - at most histMaxTables tables get an entry (the others still show their counters, without a chart).
//
// So 10,000 tables that all change constantly cost about 10,000 × 660 B ≈ 6.5 MiB, and a 10,000-table pipeline
// where a few hundred tables are busy costs well under 2 MiB. Entries of tables that left the list are dropped
// at the next sample; the whole history of a pipeline goes when the pipeline is removed. Nothing is persisted:
// a restart of the Web starts the history again (the counters of the pipeline are "since it started" anyway).
const (
	histBucket    = time.Minute
	histBuckets   = 60     // 1 hour of per-minute points
	histGaps      = 16     // gaps between changes kept for the usual gap (ring)
	histMaxTables = 10_000 // tables with an entry, per pipeline
)

// Quiet-table rule: a table is QUIET when all of these hold at the sample time:
//
//  1. it is not being copied (state done: a table in its initial copy is never quiet);
//  2. it changed REGULARLY before it stopped: CDC changes were seen in at least quietMinWindows of the
//     quietWindows ten-minute windows that end with its last change (that is, over the last two hours of its
//     activity it changed in at least one hour's worth of windows); a table that never changed, or changes in
//     short bursts (a nightly batch, an hourly job), never qualifies;
//  3. at least quietMinGaps gaps between changes were measured, and the time since its last change exceeds
//     max(quietGapFactor × usual gap, quietFloor), where the usual gap is the mean of the last histGaps gaps
//     between changes the Web saw (time between the "last change" times of consecutive samples with news).
//
// Example: a table changed every few seconds (usual gap 5 s) is quiet after 15 minutes without a change; a
// table changed every 10 minutes is quiet after 100 minutes. The history starts with the Web, so right after a
// restart of the Web no table is quiet until its rhythm is seen again.
const (
	quietWindow     = 10 * time.Minute
	quietWindows    = 12
	quietMinWindows = 6
	quietMinGaps    = 4
	quietGapFactor  = 10
	quietFloor      = 15 * time.Minute
)

// quietRuleText explains the rule in the UI (tooltips) and is kept next to the constants it describes.
const quietRuleText = "Quiet: the table changed regularly (in at least 6 of the 12 ten-minute windows up to its last change) " +
	"and has had no change for more than 10 times its usual gap between changes (at least 15 minutes). " +
	"Tables in their initial copy and tables that never changed are never quiet."

// tblSeries: per-minute buckets of one table, a ring indexed by unix minute % histBuckets. Slot i holds minute m
// when lastMin-histBuckets < m <= lastMin; slots of minutes skipped when lastMin advances are cleared.
type tblSeries struct {
	lastMin int64               // unix minute of the newest bucket
	chg     [histBuckets]uint32 // CDC changes applied in the minute (saturating)
	delay   [histBuckets]uint32 // 1 + largest sync delay (target apply - source commit) of the minute, ms; 0: none
}

// tblHist: what the Web keeps for one table that had a CDC change.
type tblHist struct {
	gen        uint32 // sample generation that last saw the table (to drop tables that left the list)
	lastTotal  int64  // inserts+updates+deletes at the previous sample
	lastChange int64  // newest LastChangeAt seen, unix ms (-1: none)
	gaps       [histGaps]uint32
	nGaps      uint8
	gapNext    uint8
	winMask    uint16 // bit i: a change in the ten-minute window winLast-i
	winLast    int64  // index (unix / 600) of the newest window with a change
	series     *tblSeries
}

// pipeHist: the history of one pipeline.
type pipeHist struct {
	mu     sync.Mutex
	seeded bool
	gen    uint32
	since  time.Time // first sample seen (the start of the charts)
	tables map[string]*tblHist
}

func newPipeHist() *pipeHist { return &pipeHist{tables: map[string]*tblHist{}} }

func unixMs(t *time.Time) int64 {
	if t == nil {
		return -1
	}
	return t.UnixMilli()
}

func sat32(v int64) uint32 {
	if v < 0 {
		return 0
	}
	if v > 0xFFFFFFFF {
		return 0xFFFFFFFF
	}
	return uint32(v)
}

// update takes the table list of one (non-stale) sample at now and returns the quiet tables (nil: none).
// Counters that went DOWN (the pipeline process restarted and counts again from 0) count as "changes since
// the restart": the delta is the new value, never negative.
func (h *pipeHist) update(tables []admin.TableInfo, now time.Time) map[string]bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.since.IsZero() {
		h.since = now
	}
	h.gen++
	seeding := !h.seeded
	h.seeded = true
	min := now.Unix() / 60
	var quiet map[string]bool
	for i := range tables {
		t := &tables[i]
		total, lc := t.Changes(), unixMs(t.LastChangeAt)
		e := h.tables[t.Table]
		if e == nil {
			if total <= 0 && lc < 0 {
				continue
			}
			if len(h.tables) >= histMaxTables {
				continue
			}
			e = &tblHist{lastChange: -1}
			h.tables[t.Table] = e
			if seeding {
				// The first sample after the Web started: these counts are older than the history, take them as
				// the baseline (no chart point, no gap).
				e.gen, e.lastTotal, e.lastChange = h.gen, total, lc
				continue
			}
		}
		e.gen = h.gen
		delta := total - e.lastTotal
		if total < e.lastTotal {
			delta = total // restarted counters
		}
		advanced := lc >= 0 && lc > e.lastChange
		if delta > 0 || advanced {
			if advanced && e.lastChange >= 0 {
				e.addGap(sat32((lc - e.lastChange) / 1000))
			}
			at := now
			if advanced {
				at = time.UnixMilli(lc)
			}
			e.markWindow(at.Unix() / int64(quietWindow/time.Second))
			var delay int64 = -1
			if advanced && t.LastSourceAt != nil {
				delay = lc - t.LastSourceAt.UnixMilli()
			}
			e.addPoint(min, delta, delay)
		}
		e.lastTotal = total
		if lc > e.lastChange {
			e.lastChange = lc
		}
		if e.series != nil && e.series.lastMin <= min-histBuckets {
			e.series = nil // no change for a whole window: free the buckets
		}
		if t.State == "done" && e.isQuiet(now) {
			if quiet == nil {
				quiet = map[string]bool{}
			}
			quiet[t.Table] = true
		}
	}
	for name, e := range h.tables {
		if e.gen != h.gen {
			delete(h.tables, name)
		}
	}
	return quiet
}

func (e *tblHist) addGap(sec uint32) {
	e.gaps[e.gapNext] = sec
	e.gapNext = (e.gapNext + 1) % histGaps
	if e.nGaps < histGaps {
		e.nGaps++
	}
}

func (e *tblHist) markWindow(w int64) {
	switch d := w - e.winLast; {
	case e.winMask == 0:
		e.winMask, e.winLast = 1, w
	case d <= 0: // same window (or an older one: keep the newest as the reference)
		if d > -quietWindows {
			e.winMask |= 1 << uint(-d)
		}
	case d >= quietWindows:
		e.winMask, e.winLast = 1, w
	default:
		e.winMask = e.winMask<<uint(d) | 1
		e.winLast = w
	}
	e.winMask &= 1<<quietWindows - 1
}

func slot(min int64) int { return int(((min % histBuckets) + histBuckets) % histBuckets) }

func (e *tblHist) addPoint(min, delta, delay int64) {
	s := e.series
	if s == nil {
		s = &tblSeries{lastMin: min}
		e.series = s
	}
	if min < s.lastMin-histBuckets+1 {
		return // older than the window (clock went back)
	}
	if min > s.lastMin {
		for m, n := s.lastMin+1, 0; m <= min && n < histBuckets; m, n = m+1, n+1 {
			s.chg[slot(m)], s.delay[slot(m)] = 0, 0
		}
		s.lastMin = min
	}
	i := slot(min)
	if delta > 0 {
		s.chg[i] = sat32(int64(s.chg[i]) + delta)
	}
	if delay >= 0 {
		if d := sat32(delay + 1); d > s.delay[i] {
			s.delay[i] = d
		}
	}
}

// usualGap: the mean of the measured gaps between changes (0 without any).
func (e *tblHist) usualGap() time.Duration {
	if e.nGaps == 0 {
		return 0
	}
	var sum uint64
	for i := 0; i < int(e.nGaps); i++ {
		sum += uint64(e.gaps[i])
	}
	return time.Duration(sum/uint64(e.nGaps)) * time.Second
}

// activeWindows: ten-minute windows with a change among the quietWindows up to the last change.
func (e *tblHist) activeWindows() int { return bits.OnesCount16(e.winMask) }

// quietAfter: how long without a change makes the table quiet (0: the table does not qualify).
func (e *tblHist) quietAfter() time.Duration {
	if e.lastChange < 0 || int(e.nGaps) < quietMinGaps || e.activeWindows() < quietMinWindows {
		return 0
	}
	th := quietGapFactor * e.usualGap()
	if th < quietFloor {
		th = quietFloor
	}
	return th
}

func (e *tblHist) isQuiet(now time.Time) bool {
	th := e.quietAfter()
	return th > 0 && now.Sub(time.UnixMilli(e.lastChange)) > th
}

// TableHistPoint is one minute of the table history.
type TableHistPoint struct {
	T       time.Time `json:"t"`
	Changes int64     `json:"changes"`
	// MaxDelay: the largest sync delay (target apply time - source commit time) of the minute, seconds; absent
	// when the minute had no change with a known source time.
	MaxDelay *float64 `json:"max_delay_seconds,omitempty"`
}

// TableQuietInfo explains the quiet rule for one table.
type TableQuietInfo struct {
	Quiet         bool    `json:"quiet"`
	Regular       bool    `json:"regular"`        // condition 2 and enough gaps (condition 3, first half)
	ActiveWindows int     `json:"active_windows"` // of quietWindows ten-minute windows up to the last change
	Gaps          int     `json:"gaps_measured"`
	UsualGapS     float64 `json:"usual_gap_seconds"`
	QuietAfterS   float64 `json:"quiet_after_seconds,omitempty"` // threshold (only when regular)
	IdleS         float64 `json:"idle_seconds,omitempty"`        // since the last change, at the sample time
	Rule          string  `json:"rule"`
}

// tableHistory returns the last histBuckets minutes up to now (only minutes since the Web watched the pipeline;
// minutes without a change are 0) and the quiet details of one table.
func (h *pipeHist) tableHistory(table string, now time.Time) ([]TableHistPoint, TableQuietInfo) {
	qi := TableQuietInfo{Rule: quietRuleText}
	if h == nil {
		return nil, qi
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	cur := now.Unix() / 60
	first := cur - histBuckets + 1
	if !h.since.IsZero() {
		if s := h.since.Unix() / 60; s > first {
			first = s
		}
	}
	e := h.tables[table]
	pts := make([]TableHistPoint, 0, cur-first+1)
	for m := first; m <= cur; m++ {
		p := TableHistPoint{T: time.Unix(m*60, 0).UTC()}
		if e != nil && e.series != nil {
			if s := e.series; m <= s.lastMin && m > s.lastMin-histBuckets {
				i := slot(m)
				p.Changes = int64(s.chg[i])
				if s.delay[i] > 0 {
					d := float64(s.delay[i]-1) / 1000
					p.MaxDelay = &d
				}
			}
		}
		pts = append(pts, p)
	}
	if e != nil {
		qi.ActiveWindows, qi.Gaps, qi.UsualGapS = e.activeWindows(), int(e.nGaps), e.usualGap().Seconds()
		if th := e.quietAfter(); th > 0 {
			qi.Regular, qi.QuietAfterS = true, th.Seconds()
		}
		if e.lastChange >= 0 {
			qi.IdleS = maxf(0, now.Sub(time.UnixMilli(e.lastChange)).Seconds())
		}
	}
	return pts, qi
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// tableHist returns the history of a pipeline, creating it (sampler side) when create is true.
func (s *Sampler) tableHist(name string, create bool) *pipeHist {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hist[name]
	if h == nil && create {
		h = newPipeHist()
		s.hist[name] = h
	}
	return h
}

// TableHistory: the per-minute history and quiet details of one table of a pipeline at now.
func (s *Sampler) TableHistory(pipeline, table string, now time.Time) ([]TableHistPoint, TableQuietInfo) {
	return s.tableHist(pipeline, false).tableHistory(table, now)
}

// histTracked: number of tables with an entry and with live buckets (tests, diagnostics).
func (h *pipeHist) histTracked() (entries, series int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.tables {
		entries++
		if e.series != nil {
			series++
		}
	}
	return
}

package mongocdc

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Phase is the coarse state of a MongoDB pipeline (admin socket "state").
type Phase string

// Pipeline phases.
const (
	PhaseStarting  Phase = "starting"
	PhaseSnapshot  Phase = "snapshot"
	PhaseStreaming Phase = "streaming"
	PhaseRepair    Phase = "repair"
	PhaseRetrying  Phase = "retrying"
	PhaseStopped   Phase = "stopped"
)

// Collection snapshot states (admin socket "tables").
const (
	CollPending = "pending"
	CollRunning = "running"
	CollDone    = "done"
)

// Observer collects progress of one pipeline for the admin socket and metrics, and
// carries operator controls (pause/resume, collection re-copy requests). All methods
// are safe for concurrent use; a nil *Observer is not valid.
type Observer struct {
	pipeline string
	started  time.Time

	mu            sync.Mutex
	phase         Phase
	failed        string
	colls         map[string]*CollectionProgress
	activity      map[string]*CollectionActivity // change events applied per collection (this process)
	lastEventTime time.Time                      // source cluster time of the last applied event
	lastApplied   time.Time                      // wall clock of the last applied batch
	caughtUp      bool                           // the last getMore returned nothing and nothing is pending
	position      string                         // checkpoint (resume token) description
	consistentAt  time.Time
	events        int64
	eventsByOp    map[string]int64
	docWrites     int64
	txns          int64
	snapshotDocs  int64
	lookups       int64
	retries       int64
	lastRetry     string
	fencedCount   int64
	oplogWindow   time.Duration
	eventRate     rateMeter
	docRate       rateMeter
	snapRate      rateMeter

	paused         bool
	snapshotPaused bool
	resumeCh       chan struct{} // closed on resume
	repairs        chan repairRequest
}

// CollectionProgress is the copy progress of one collection.
type CollectionProgress struct {
	Name     string
	State    string // pending | running | done
	Kind     string // snapshot | recopy
	Copied   int64
	Estimate int64
	Started  time.Time
	Finished time.Time
	// StartCopied is Copied when the collection started running in this process: the
	// copy rate (ETA) counts only what this process copied, not what was copied before
	// a restart.
	StartCopied int64
	// Activity: change-stream events applied to the collection by this process.
	Activity CollectionActivity
}

// CollectionActivity counts the change events applied to one collection: inserts, updates (with
// replaces) and deletes, when the last batch carrying one committed on the target, and the source
// cluster time of that batch.
type CollectionActivity struct {
	Inserts, Updates, Deletes int64
	LastChange                time.Time
	LastSource                time.Time
}

type repairRequest struct {
	coll string
	resp chan error
}

// NewObserver returns an Observer for pipeline.
func NewObserver(pipeline string) *Observer {
	return &Observer{
		pipeline: pipeline, started: time.Now(), phase: PhaseStarting,
		colls: map[string]*CollectionProgress{}, eventsByOp: map[string]int64{},
		resumeCh: make(chan struct{}), repairs: make(chan repairRequest),
	}
}

func (o *Observer) setPhase(p Phase) {
	o.mu.Lock()
	o.phase = p
	if p != PhaseStopped && p != PhaseRetrying {
		o.failed = ""
	}
	o.mu.Unlock()
}

func (o *Observer) setFailed(msg string) {
	o.mu.Lock()
	o.failed = msg
	o.mu.Unlock()
}

func (o *Observer) retried(err error) {
	o.mu.Lock()
	o.retries++
	o.lastRetry = err.Error()
	o.mu.Unlock()
}

func (o *Observer) fenced() {
	o.mu.Lock()
	o.fencedCount++
	o.mu.Unlock()
}

func (o *Observer) setCollection(name, state, kind string, copied, estimate int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	c := o.colls[name]
	if c == nil {
		c = &CollectionProgress{Name: name}
		o.colls[name] = c
	}
	if state == CollRunning && c.State != CollRunning {
		c.Started, c.StartCopied = time.Now(), copied
	}
	if state == CollDone && c.State != CollDone {
		c.Finished = time.Now()
	}
	c.State, c.Kind, c.Copied = state, kind, copied
	if estimate > 0 {
		c.Estimate = estimate
	}
}

// setCollectionState changes only the state of a known collection (ranges of a split
// copy report their documents through copied).
func (o *Observer) setCollectionState(name, state string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	c := o.colls[name]
	if c == nil || c.State == state {
		return
	}
	if state == CollRunning {
		c.Started, c.StartCopied = time.Now(), c.Copied
	}
	c.State = state
}

func (o *Observer) removeCollection(name string) {
	o.mu.Lock()
	delete(o.colls, name)
	o.mu.Unlock()
}

func (o *Observer) copied(name string, n int64) {
	o.mu.Lock()
	if c := o.colls[name]; c != nil {
		c.Copied += n
	}
	o.snapshotDocs += n
	o.snapRate.add(time.Now(), n)
	o.mu.Unlock()
}

func (o *Observer) applied(events int, byOp map[string]int, writes int, lookups int, last bson.Timestamp, position string) {
	now := time.Now()
	o.mu.Lock()
	o.events += int64(events)
	for op, n := range byOp {
		o.eventsByOp[op] += int64(n)
	}
	o.docWrites += int64(writes)
	o.lookups += int64(lookups)
	o.txns++
	if last.T != 0 {
		o.lastEventTime = time.Unix(int64(last.T), 0)
	}
	o.lastApplied = now
	o.position = position
	o.eventRate.add(now, int64(events))
	o.docRate.add(now, int64(writes))
	o.mu.Unlock()
}

// collApplied records the per-collection change events of a committed batch.
func (o *Observer) collApplied(ops map[string]*collOpCount, last bson.Timestamp) {
	if len(ops) == 0 {
		return
	}
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.activity == nil {
		o.activity = make(map[string]*CollectionActivity)
	}
	for coll, c := range ops {
		if c.inserts+c.updates+c.deletes == 0 {
			continue
		}
		a := o.activity[coll]
		if a == nil {
			a = &CollectionActivity{}
			o.activity[coll] = a
		}
		a.Inserts += c.inserts
		a.Updates += c.updates
		a.Deletes += c.deletes
		a.LastChange = now
		if last.T != 0 {
			a.LastSource = time.Unix(int64(last.T), 0)
		}
	}
}

func (o *Observer) setCaughtUp(v bool) {
	o.mu.Lock()
	o.caughtUp = v
	o.mu.Unlock()
}

func (o *Observer) setConsistentAt(ts bson.Timestamp) {
	o.mu.Lock()
	if ts.T != 0 {
		o.consistentAt = time.Unix(int64(ts.T), 0)
	}
	o.mu.Unlock()
}

func (o *Observer) setOplogWindow(d time.Duration) {
	o.mu.Lock()
	o.oplogWindow = d
	o.mu.Unlock()
}

// Snapshot is a consistent copy of the observed state.
type Snapshot struct {
	Pipeline       string
	Phase          Phase
	Failed         string
	Started        time.Time
	Paused         bool
	SnapshotPaused bool
	Collections    []CollectionProgress
	LagSeconds     float64
	CaughtUp       bool
	LastEventTime  time.Time
	Position       string
	Events         int64
	EventsByOp     map[string]int64
	DocWrites      int64
	Txns           int64
	SnapshotDocs   int64
	Lookups        int64
	Retries        int64
	LastRetryError string
	Fenced         int64
	EventRate      float64 // events/s over the last minute
	DocRate        float64 // target document writes/s over the last minute
	SnapshotRate   float64 // snapshot documents/s over the last minute
	OplogWindow    time.Duration
}

// Snapshot returns the current state.
func (o *Observer) Snapshot() Snapshot {
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	s := Snapshot{
		Pipeline: o.pipeline, Phase: o.phase, Failed: o.failed, Started: o.started, Paused: o.paused, SnapshotPaused: o.snapshotPaused,
		CaughtUp: o.caughtUp, LastEventTime: o.lastEventTime, Position: o.position, Events: o.events, DocWrites: o.docWrites,
		Txns: o.txns, SnapshotDocs: o.snapshotDocs, Lookups: o.lookups, Retries: o.retries, LastRetryError: o.lastRetry,
		Fenced: o.fencedCount, EventRate: o.eventRate.rate(now), DocRate: o.docRate.rate(now), SnapshotRate: o.snapRate.rate(now),
		OplogWindow: o.oplogWindow, EventsByOp: make(map[string]int64, len(o.eventsByOp)),
	}
	for k, v := range o.eventsByOp {
		s.EventsByOp[k] = v
	}
	for _, c := range o.colls {
		cp := *c
		if a := o.activity[c.Name]; a != nil {
			cp.Activity = *a
		}
		s.Collections = append(s.Collections, cp)
	}
	sort.Slice(s.Collections, func(i, j int) bool { return s.Collections[i].Name < s.Collections[j].Name })
	switch {
	case o.phase == PhaseStreaming && o.caughtUp:
		s.LagSeconds = 0
	case !o.lastEventTime.IsZero():
		s.LagSeconds = now.Sub(o.lastEventTime).Seconds()
	case o.phase == PhaseSnapshot || o.phase == PhaseStarting:
		s.LagSeconds = now.Sub(o.started).Seconds()
	}
	if s.LagSeconds < 0 {
		s.LagSeconds = 0
	}
	return s
}

// Pause stops applying after the current batch (snapshotOnly: stop only snapshot copying).
func (o *Observer) Pause(snapshotOnly bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if snapshotOnly {
		o.snapshotPaused = true
		return
	}
	o.paused = true
}

// Resume undoes Pause (snapshotOnly: resume only snapshot copying).
func (o *Observer) Resume(snapshotOnly bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshotPaused = false
	if !snapshotOnly {
		o.paused = false
	}
	close(o.resumeCh)
	o.resumeCh = make(chan struct{})
}

// waitIfPaused blocks while the pipeline (or, for snapshot work, the snapshot) is paused.
func (o *Observer) waitIfPaused(ctx context.Context, snapshot bool) error {
	for {
		o.mu.Lock()
		blocked := o.paused || (snapshot && o.snapshotPaused)
		ch := o.resumeCh
		o.mu.Unlock()
		if !blocked {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// ErrNotStreaming is returned for a re-copy request while the pipeline is not streaming.
var ErrNotStreaming = errors.New("mongodb cdc: the pipeline is not streaming (initial snapshot not finished or not running)")

// RequestRecopy asks the running pipeline to re-copy one collection from the source
// (repair). It returns once the re-copy has been checkpointed as started; the copy then
// runs in the stream loop (changes stop being applied until it finishes).
func (o *Observer) RequestRecopy(ctx context.Context, coll string) error {
	req := repairRequest{coll: coll, resp: make(chan error, 1)}
	o.mu.Lock()
	phase := o.phase
	o.mu.Unlock()
	if phase != PhaseStreaming {
		return ErrNotStreaming
	}
	select {
	case o.repairs <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// rateMeter is a one-minute sliding rate over one-second buckets.
type rateMeter struct {
	buckets [60]int64
	secs    [60]int64
}

func (m *rateMeter) add(now time.Time, n int64) {
	s := now.Unix()
	i := s % 60
	if m.secs[i] != s {
		m.secs[i], m.buckets[i] = s, 0
	}
	m.buckets[i] += n
}

func (m *rateMeter) rate(now time.Time) float64 {
	s := now.Unix()
	var sum, oldest int64 = 0, s
	for i := range m.buckets {
		if m.secs[i] > s-60 && m.secs[i] <= s && m.buckets[i] > 0 {
			sum += m.buckets[i]
			if m.secs[i] < oldest {
				oldest = m.secs[i]
			}
		}
	}
	if sum == 0 {
		return 0
	}
	span := float64(s-oldest) + 1
	return float64(sum) / span
}

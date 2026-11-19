package web

import (
	"sync"
	"sync/atomic"
	"time"

	"strand/internal/web/store"
)

// Pipelines stopped outside the web (`strand stop`, `kill -TERM` by an operator) must not be restarted by the
// web's autostart. The web records desired=running when it starts a background pipeline; before this check a
// pipeline stopped from the CLI kept desired=running and came back on the next web start.
//
// Rule (DB mode, background pipelines, active instance only):
//   - the process is not running AND its log ends with "pipeline stopped cleanly" (exit 0) AND that line is
//     newer than the end of the last stop the web itself performed (Stop/Restart buttons, wizards, shutdown
//     with shutdown_stop_pipelines) -> desired running -> stopped (reason observed_stop) + audit
//     `desired.observed_stop`;
//   - a crash (error exit, SIGKILL/OOM: no clean line) keeps desired=running: autostart is still wanted.
//
// Systemd pipelines have no desired state in the web (systemd's own enable/Restart policy decides), so the
// check does not apply to them.

const webStopMetaPrefix = "web_stop:"

// stopTracker remembers the stops the web performs, to tell them apart from stops done elsewhere.
type stopTracker struct {
	mu       sync.Mutex
	stopping map[string]int       // web stops in progress (the clean line appears before bgStop returns)
	webStop  map[string]time.Time // wall-clock end of the last web stop
	seen     map[string]time.Time // clean stop (StoppedAt) already handled
}

func newStopTracker() *stopTracker {
	return &stopTracker{stopping: map[string]int{}, webStop: map[string]time.Time{}, seen: map[string]time.Time{}}
}

// beginWebStop marks a web-initiated stop of name; the returned func records its end (call it after the stop
// returned, whatever the result: a stop that timed out may still finish later, and it was still the web's).
func (s *Server) beginWebStop(name string) func() {
	t := s.stops
	t.mu.Lock()
	t.stopping[name]++
	t.mu.Unlock()
	return func() {
		end := time.Now() // wall clock: compared with the time written in the pipeline log on this host
		t.mu.Lock()
		t.webStop[name] = end
		t.stopping[name]--
		if t.stopping[name] <= 0 {
			delete(t.stopping, name)
		}
		t.mu.Unlock()
		if s.db != nil {
			// Persisted so that the next web (restart, or a standby taking over) also knows this stop was ours.
			ctx, cancel := dbCtx()
			defer cancel()
			if err := s.db.st.MetaSet(ctx, webStopMetaPrefix+name, end.UTC().Format(time.RFC3339Nano)); err != nil {
				s.log.Warn("record web stop time failed (the stop may later be taken for an outside stop)", "pipeline", name, "err", err)
			}
		}
	}
}

// lastWebStop returns the end of the last web stop of name (memory, else the database).
func (s *Server) lastWebStop(name string) time.Time {
	t := s.stops
	t.mu.Lock()
	ws, ok := t.webStop[name]
	t.mu.Unlock()
	if ok || s.db == nil {
		return ws
	}
	ctx, cancel := dbCtx()
	defer cancel()
	v, have, err := s.db.st.MetaGet(ctx, webStopMetaPrefix+name)
	if err != nil || !have {
		return time.Time{}
	}
	ws, _ = time.Parse(time.RFC3339Nano, v)
	t.mu.Lock()
	if _, ok := t.webStop[name]; !ok {
		t.webStop[name] = ws
	}
	t.mu.Unlock()
	return ws
}

// observeStop applies the rule above to one sample of a pipeline. Reports whether the pipeline is known to
// have been stopped outside the web (desired is, or has just been set to, stopped for that reason).
func (s *Server) observeStop(p *Pipeline, rt Runtime) bool {
	if s.db == nil || !s.isActive() || p.Mode() != ModeBackground || rt.Running || !rt.StoppedCleanly || rt.StoppedAt == nil {
		return false
	}
	at := *rt.StoppedAt
	t := s.stops
	t.mu.Lock()
	busy := t.stopping[p.Name] > 0
	done := t.seen[p.Name].Equal(at)
	t.mu.Unlock()
	if busy || done {
		return done && s.desiredState(p.Name).Reason == store.ReasonObservedStop
	}
	if ws := s.lastWebStop(p.Name); !ws.IsZero() && !at.After(ws) {
		t.mu.Lock()
		t.seen[p.Name] = at
		t.mu.Unlock()
		return false // the web's own stop
	}
	ctx, cancel := dbCtx()
	changed, err := s.db.st.ObserveStop(ctx, p.Name)
	cancel()
	if err != nil {
		s.log.Warn("record observed stop failed", "pipeline", p.Name, "err", err)
		return false // retried on the next sample
	}
	t.mu.Lock()
	t.seen[p.Name] = at
	t.mu.Unlock()
	if !changed {
		return s.desiredState(p.Name).Reason == store.ReasonObservedStop
	}
	s.log.Info("pipeline was stopped outside the web: desired state set to stopped (no autostart)", "pipeline", p.Name, "stopped_at", at)
	_ = s.audit.Log(AuditEntry{Action: "desired.observed_stop", Pipeline: p.Name, Result: "ok",
		Detail: map[string]any{"desired": store.DesiredStopped, "stopped_at": at.UTC().Format(time.RFC3339Nano), "mode": ModeBackground,
			"reason": "the process exited cleanly (pipeline stopped cleanly, exit 0) without a web action"}})
	s.refreshDesired()
	return true
}

// ---- cache of the desired states (UI: "stopped outside the web") ----

type desiredCache struct {
	m atomic.Pointer[map[string]store.DesiredState]
}

// refreshDesired reloads the desired states (once per sampler round, and after a change).
func (s *Server) refreshDesired() {
	if s.db == nil {
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	m, err := s.db.st.DesiredStates(ctx)
	if err != nil {
		return // keep the previous copy
	}
	s.desired.m.Store(&m)
}

func (s *Server) desiredState(name string) store.DesiredState {
	if m := s.desired.m.Load(); m != nil {
		return (*m)[name]
	}
	return store.DesiredState{}
}

// stoppedOutside: the UI note "stopped outside the web".
func (s *Server) stoppedOutside(p *Pipeline) bool {
	if s.db == nil || p.Mode() != ModeBackground {
		return false
	}
	d := s.desiredState(p.Name)
	return d.Desired == store.DesiredStopped && d.Reason == store.ReasonObservedStop
}

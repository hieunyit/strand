package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"strand/internal/web/store"
)

// Two or more `strand web` processes on one metadata database (DB mode): exactly one is ACTIVE, the others are
// STANDBY. The active instance holds the lease row (web_lease, migration 008) and renews it every TTL/3; it
// alone runs the singleton work:
//
//   - autostart from the desired state (ResumeDesired) and the observed-stop rule (observed_stop.go);
//   - metrics history writes and the hourly maintenance;
//   - recovery of jobs left "running" (adopt / mark interrupted);
//   - pipeline discovery writes (web.yaml / scan -> registry rows, missing_since);
//   - the automatic audit archive (audit_archive.go);
//   - every write request (start/stop, config edits, settings, wizards...).
//
// A standby serves the UI and the read API (sessions are in the database, so a login works on any instance),
// answers write requests with 503 and a pointer to the active instance, and takes over when the lease expires:
// the holder stopped (clean shutdown releases the lease at once) or lost the database for longer than the TTL.
// An active instance that cannot renew stops acting as active when its local deadline (last successful renewal
// + remaining TTL, measured with the database clock) passes, before a standby can acquire the lease.
//
// Why standby rather than refusing to start: a second instance is useful (zero-downtime upgrades, a warm spare
// that takes over automatically) and harmless once only one of them writes. Background pipelines are child
// processes of the instance that started them; a standby on another host cannot see them (it shows them as not
// running) and, after a takeover, starts the pipelines whose desired state is running on its own host. If the
// old instance is still alive but cut off from the database, its pipelines keep running: the pipeline lock
// (exit code 5, pipeline_locked) then stops the second copy.

// Lease timing (defaults; STRAND_WEB_LEASE_TTL=<seconds> overrides the TTL, renewals run every TTL/3).
const (
	DefaultLeaseTTL = 30 * time.Second
	EnvLeaseTTL     = "STRAND_WEB_LEASE_TTL"
)

// Roles.
const (
	RoleActive  = "active"
	RoleStandby = "standby"
)

var errStandby = errors.New("this web instance is a standby (read-only): another instance holds the active lease")

type leaseMgr struct {
	id   string
	info string
	ttl  time.Duration

	active atomic.Bool
	until  atomic.Int64 // UnixNano (monotonic-based local deadline) up to which the lease is ours

	mu      sync.Mutex
	current store.Lease // last lease seen (holder may be another instance)
	seenAt  time.Time
}

func leaseTTL(o Options) time.Duration {
	if o.LeaseTTL > 0 {
		return o.LeaseTTL
	}
	if v := strings.TrimSpace(os.Getenv(EnvLeaseTTL)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 3 && n <= 600 {
			return time.Duration(n) * time.Second
		}
	}
	return DefaultLeaseTTL
}

func newLeaseMgr(listen, version string, ttl time.Duration) *leaseMgr {
	host, _ := os.Hostname()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	id := fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(b))
	info, _ := json.Marshal(map[string]any{"id": id, "host": host, "pid": os.Getpid(), "listen": listen, "version": version,
		"started": time.Now().UTC().Format(time.RFC3339)})
	return &leaseMgr{id: id, info: string(info), ttl: ttl}
}

// monoNow: nanoseconds of a monotonic clock (immune to wall-clock jumps).
var monoBase = time.Now()

func monoNow() int64 { return int64(time.Since(monoBase)) }

// isActive: this instance runs the singleton work. File mode: always (single instance by construction).
func (s *Server) isActive() bool {
	if s.lease == nil {
		return true
	}
	return s.lease.active.Load() && monoNow() < s.lease.until.Load()
}

// role: "active" | "standby".
func (s *Server) role() string {
	if s.lease == nil {
		return ""
	}
	if s.isActive() {
		return RoleActive
	}
	return RoleStandby
}

// leaseTick tries to acquire/renew the lease once. Returns (acquired now, lost now).
func (s *Server) leaseTick() (gained, lost bool) {
	l := s.lease
	was := s.isActive()
	timeout := l.ttl / 3
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}
	if timeout > l.ttl {
		timeout = l.ttl
	}
	baseCtx := context.Background()
	if s.closeCtx != nil {
		baseCtx = s.closeCtx
	}
	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	t0 := monoNow()
	cur, err := s.db.st.TryLease(ctx, l.id, l.info, l.ttl)
	cancel()
	if err != nil {
		// Keep acting until the local deadline; isActive() turns false by itself after it.
		if was {
			s.log.Warn("renew the active lease failed (still active until the lease would expire)", "err", err,
				"left", time.Duration(l.until.Load()-monoNow()).Round(time.Second).String())
		}
		return false, false
	}
	l.mu.Lock()
	prev := l.current
	l.current, l.seenAt = cur, time.Now()
	l.mu.Unlock()
	if cur.Held {
		// Deadline from the start of the request: the database may have granted it any time after t0.
		l.until.Store(t0 + int64(cur.Remaining))
		l.active.Store(true)
		if !was {
			s.log.Info("this instance is now the ACTIVE web instance (holds the lease)", "epoch", cur.Epoch, "instance", l.id)
			det := map[string]any{"epoch": cur.Epoch, "instance": l.id}
			if prev.Holder != "" && prev.Holder != l.id {
				det["previous_holder"] = prev.Holder
			}
			_ = s.audit.Log(AuditEntry{Action: "web.lease.acquired", Result: "ok", Detail: det})
			return true, false
		}
		return false, false
	}
	l.active.Store(false)
	if was {
		s.log.Error("lost the active lease to another web instance: switching to STANDBY (read-only)", "holder", cur.Holder, "epoch", cur.Epoch)
		return false, true
	}
	return false, false
}

// runLease renews (active) or watches (standby) the lease until ctx ends, takes over when it can, and keeps
// the heartbeat row of this instance.
func (s *Server) runLease(ctx context.Context) {
	if s.lease == nil {
		return
	}
	every := s.lease.ttl / 3
	wasActive := s.isActive()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		s.heartbeat()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		wasActive = s.leaseStep(ctx, wasActive)
	}
}

// leaseStep runs one lease round: renew/acquire, and react to a role change. wasActive is the role after the
// previous round; it returns the role now. Stepping down (lease lost to another holder, or the local
// deadline passed while the database was unreachable) stops the backup schedulers this instance started;
// gaining the lease runs takeover.
func (s *Server) leaseStep(ctx context.Context, wasActive bool) bool {
	gained, _ := s.leaseTick()
	nowActive := s.isActive()
	if wasActive && !nowActive {
		go s.stopBackupSchedulers()
	}
	if gained {
		s.takeover(ctx)
	} else if !nowActive {
		if jr, ok := s.jobs.(*FileJobRunner); ok {
			_ = jr.Reload() // job history written by the active instance
		}
	}
	return nowActive
}

// takeover: this standby just became active. Run what the previous holder no longer does.
func (s *Server) takeover(ctx context.Context) {
	if jr, ok := s.jobs.(*FileJobRunner); ok {
		if err := jr.Recover(); err != nil {
			s.log.Warn("takeover: recover jobs failed", "err", err)
		}
	}
	s.reg.Refresh(ctx) // discovery writes now allowed
	go s.ResumeDesired(ctx)
	go func() {
		if err := s.resumeBackupJobs(ctx); err != nil {
			s.log.Error("takeover: start backup schedulers", "error", err)
		}
	}()
}

func (s *Server) heartbeat() {
	ctx, cancel := dbCtx()
	defer cancel()
	_ = s.db.st.HeartbeatInstance(ctx, s.lease.id, s.lease.info, s.role())
}

// releaseLease (clean shutdown): give the lease up at once so a standby takes over without waiting for the TTL.
func (s *Server) releaseLease() {
	if s.lease == nil {
		return
	}
	s.lease.active.Store(false)
	ctx, cancel := dbCtx()
	defer cancel()
	if err := s.db.st.ReleaseLease(ctx, s.lease.id); err != nil {
		s.log.Warn("release the active lease failed (a standby takes over when it expires)", "err", err)
	}
	_ = s.db.st.RemoveInstance(ctx, s.lease.id)
}

// leaseView: lease information for /healthz and the Settings page.
type leaseView struct {
	Role      string         `json:"role"`
	Instance  string         `json:"instance"`
	Holder    string         `json:"active_instance,omitempty"`
	Active    map[string]any `json:"active,omitempty"` // info of the active instance (host, listen, version)
	Epoch     int64          `json:"epoch,omitempty"`
	ExpiresIn float64        `json:"expires_in_s,omitempty"`
}

func (s *Server) leaseInfo() *leaseView {
	if s.lease == nil {
		return nil
	}
	l := s.lease
	l.mu.Lock()
	cur, at := l.current, l.seenAt
	l.mu.Unlock()
	v := &leaseView{Role: s.role(), Instance: l.id, Holder: cur.Holder, Epoch: cur.Epoch}
	if cur.Info != "" {
		_ = json.Unmarshal([]byte(cur.Info), &v.Active)
	}
	if left := cur.Remaining - time.Since(at); left > 0 && cur.Holder != "" {
		v.ExpiresIn = left.Round(time.Second).Seconds()
	}
	return v
}

// standbyMessage: banner / 503 text naming the active instance.
func (s *Server) standbyMessage() string {
	v := s.leaseInfo()
	if v == nil || v.Role != RoleStandby {
		return ""
	}
	msg := "This web instance is a standby (read-only): another instance holds the active lease"
	if v.Active != nil {
		host, _ := v.Active["host"].(string)
		listen, _ := v.Active["listen"].(string)
		if host != "" || listen != "" {
			msg += " (" + strings.TrimSpace(host+" "+listen) + ")"
		}
	}
	return msg + ". Make changes on the active instance; this one takes over automatically if the active one stops."
}

// standbyGuard rejects write requests on a standby instance (503 + Retry-After). Login and logout stay allowed.
func (s *Server) standbyGuard(w http.ResponseWriter, r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	if r.URL.Path == "/logout" || r.URL.Path == "/api/v1/logout" || s.isActive() {
		return false
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(s.lease.ttl.Seconds())))
	w.Header().Set("X-Strand-Role", RoleStandby)
	writeErr(w, http.StatusServiceUnavailable, s.standbyMessage())
	return true
}

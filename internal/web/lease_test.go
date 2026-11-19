package web

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/web/store"
)

// Two web instances on one metadata database. Before the lease both ran the singleton work: both autostarted
// pipelines, wrote metrics history, rewrote the registry rows from their own web.yaml and marked the other's
// running jobs "interrupted". Now one is active, the other a read-only standby that takes over when the
// lease expires.
func TestTwoInstancesOneActiveStandbyTakesOver(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		ctx := context.Background()
		st := mk()
		ttl := 900 * time.Millisecond
		withTTL := func(o *Options) { o.LeaseTTL = ttl }
		e1 := dbEnv(t, st, nil, withTTL)
		if e1.srv.role() != RoleActive {
			t.Fatalf("first instance role = %q", e1.srv.role())
		}
		c1, csrf1 := e1.loginOK()
		if code, _ := e1.act(c1, csrf1, "orders", "start", nil); code != 200 {
			t.Fatal("start on the active instance failed")
		}
		// A job running on the active instance (held open by the fake binary).
		e1.hold(true)
		t.Cleanup(func() { e1.hold(false) })
		if code, body := e1.act(c1, csrf1, "orders", "verify", map[string]string{"mode": "full"}); code != 200 {
			t.Fatalf("verify: %d %v", code, body)
		}
		row1, _, _ := st.GetPipeline(ctx, "orders")

		e2 := dbEnv(t, st, nil, withTTL) // second instance: own web.yaml path for "orders", own binary
		if e2.srv.role() != RoleStandby {
			t.Fatalf("second instance role = %q, want standby", e2.srv.role())
		}
		// Standby did not touch the active instance's job, registry row or pipelines.
		for _, j := range e2.srv.jobs.List("orders") {
			if !j.Running || j.Interrupted {
				t.Fatalf("standby marked the active instance's job: %+v", j)
			}
		}
		if row, _, _ := st.GetPipeline(ctx, "orders"); row.ConfigPath != row1.ConfigPath {
			t.Fatalf("standby rewrote the registry row: %s -> %s", row1.ConfigPath, row.ConfigPath)
		}
		if got := e2.srv.ResumeDesired(ctx); got != nil {
			t.Fatalf("standby autostarted %v", got)
		}
		e2.srv.samp.SampleOnce(ctx)
		e2.srv.db.metrics.mu.Lock()
		pending := len(e2.srv.db.metrics.pending)
		e2.srv.db.metrics.mu.Unlock()
		if pending != 0 {
			t.Fatalf("standby buffered %d metrics rows", pending)
		}
		// Read-only: login, pages and reads work; writes get 503 naming the active instance.
		c2, csrf2 := e2.loginOK()
		if resp, _ := e2.do(c2, "GET", "/api/v1/pipelines", "", nil); resp.StatusCode != 200 {
			t.Fatalf("standby read: %d", resp.StatusCode)
		}
		code, body := e2.act(c2, csrf2, "orders", "stop", nil)
		if code != http.StatusServiceUnavailable || !strings.Contains(body["error"].(string), "standby") {
			t.Fatalf("write on standby: %d %v", code, body)
		}
		resp, _ := e2.do(c2, "POST", "/api/v1/settings", `{"secure_cookies": true}`, map[string]string{csrfHeader: csrf2, "Content-Type": "application/json"})
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("X-Strand-Role") != "standby" {
			t.Fatalf("settings save on standby: %d", resp.StatusCode)
		}
		if _, page := e2.do(c2, "GET", "/", "", nil); !strings.Contains(page, `id="standby-banner"`) {
			t.Fatal("standby banner missing")
		}
		_, hz := e2.do(e2.client(), "GET", "/healthz", "", nil)
		var h struct {
			Role  string    `json:"role"`
			Lease leaseView `json:"lease"`
		}
		_ = json.Unmarshal([]byte(hz), &h)
		if h.Role != RoleStandby || h.Lease.Holder != e1.srv.lease.id || h.Lease.Active["listen"] == nil {
			t.Fatalf("standby healthz = %s", hz)
		}

		// The active instance dies (no renewal): the standby takes over after the TTL, the old one steps down.
		time.Sleep(ttl + 200*time.Millisecond)
		if e1.srv.isActive() {
			t.Fatal("instance whose lease expired still acts as active")
		}
		gained, _ := e2.srv.leaseTick()
		if !gained || e2.srv.role() != RoleActive {
			t.Fatalf("standby did not take over: role %s", e2.srv.role())
		}
		e2.srv.takeover(ctx)
		waitFor(t, "autostart after takeover", func() bool { return len(e2.auditFor("autostart")) >= 2 })
		if a := e2.auditFor("web.lease.acquired"); len(a) == 0 {
			t.Fatal("takeover not audited")
		}
		if code, _ := e2.act(c2, csrf2, "orders", "stop", nil); code != 200 {
			t.Fatalf("write after takeover: %d", code)
		}
		if _, lost := e1.srv.leaseTick(); e1.srv.isActive() {
			t.Fatalf("old instance got the lease back (lost=%v)", lost)
		}
		if code, _ := e1.act(c1, csrf1, "orders", "start", nil); code != http.StatusServiceUnavailable {
			t.Fatalf("old active instance still accepts writes: %d", code)
		}
	})
}

// Clean shutdown of the active instance releases the lease: the standby's lease loop takes over within
// about TTL/3, without waiting for the TTL.
func TestActiveShutdownReleasesLease(t *testing.T) {
	st := store.NewMem()
	ttl := 3 * time.Second
	e1 := dbEnv(t, st, nil, func(o *Options) { o.LeaseTTL = ttl })
	e2 := dbEnv(t, st, nil, func(o *Options) { o.LeaseTTL = ttl })
	if e2.srv.role() != RoleStandby {
		t.Fatal("second instance not standby")
	}
	serve := func(e *testEnv) (context.CancelFunc, chan struct{}) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = e.srv.Serve(ctx, ln); close(done) }()
		return cancel, done
	}
	stop1, done1 := serve(e1)
	stop2, done2 := serve(e2)
	defer func() { stop2(); <-done2 }()
	time.Sleep(200 * time.Millisecond)
	t0 := time.Now()
	stop1()
	<-done1
	waitFor(t, "standby takes over", func() bool { return e2.srv.role() == RoleActive })
	if d := time.Since(t0); d > ttl {
		t.Fatalf("takeover took %v (lease not released on shutdown)", d)
	}
	in, _ := st.ListInstances(context.Background())
	if len(in) != 1 || in[0].ID != e2.srv.lease.id {
		t.Fatalf("instances after shutdown = %+v", in)
	}
}

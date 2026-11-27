package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strand/internal/promote"
	"strings"
	"sync"
	"testing"

	"strand/internal/admin"
)

// Lag giả cho Top của admin socket (mặc định 0.25 như mockPipeline).
var (
	testLagMu sync.Mutex
	testLag   = 0.25
)

func setTestLag(v float64) { testLagMu.Lock(); testLag = v; testLagMu.Unlock() }

func (m *recPipeline) Top(ctx context.Context) (*admin.TopInfo, error) {
	testLagMu.Lock()
	defer testLagMu.Unlock()
	return &admin.TopInfo{PipelineID: "orders", State: "synced", LagSeconds: testLag, Throughput: 1234, Zone: "green"}, nil
}

// wizFake thay script strand giả bằng bản in nội dung file `out.txt` (nếu có), ghi calls.log như newFakeStrand.
func wizFake(t *testing.T, e *testEnv) {
	t.Helper()
	script := `#!/bin/sh
d=$(dirname "$0")
{ echo CALL; for a in "$@"; do echo "$a"; done; echo "ENV=$STRAND_TEST_VAR"; echo "SECRETENV=$STRAND_PROMOTE_TARGET_DSN|$STRAND_PROMOTE_FENCE_DSN"; } >> "$d/calls.log"
[ -f "$d/out.txt" ] && cat "$d/out.txt"
while [ -e "$d/hold" ]; do sleep 0.05; done
c=0
[ -f "$d/exitcode" ] && c=$(cat "$d/exitcode")
exit $c
`
	if err := os.WriteFile(e.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (e *testEnv) setOut(s string) {
	_ = os.WriteFile(filepath.Join(filepath.Dir(e.bin), "out.txt"), []byte(s), 0o600)
}

// lastJobDone chờ job hiện tại của wizard (theo state trong DB) kết thúc và trả về nó.
func (e *testEnv) lastJobDone(kind string) JobStatus {
	e.t.Helper()
	var j JobStatus
	waitFor(e.t, "job to finish", func() bool {
		st, err := e.srv.wizLoad(kind, "orders")
		if err != nil || st == nil || st.JobID == "" {
			return false
		}
		got, ok := e.srv.jobRunner().Get(st.JobID)
		if !ok || got.Running {
			return false
		}
		j = got
		return true
	})
	return j
}

func (e *testEnv) wizState(c *http.Client, kind string) map[string]any {
	e.t.Helper()
	resp, body := e.do(c, "GET", "/api/v1/pipelines/orders/"+kind, "", nil)
	if resp.StatusCode != 200 {
		e.t.Fatalf("GET %s: %d %s", kind, resp.StatusCode, body)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		e.t.Fatal(err)
	}
	return m
}

func stepIs(m map[string]any, want string) bool {
	st, _ := m["state"].(map[string]any)
	return st != nil && st["step"] == want
}

var allWizardActions = []string{ActRepairBegin, ActRepairStop, ActRepairRun, ActRepairStart, ActRepairAbort,
	ActPromotePlan, ActPromoteRun, ActPromoteFailover, ActPromoteAbort}

func TestWizardActionsRequireAuthAndCSRF(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.sampleNow()
	c, csrf := e.loginOK()
	for _, n := range allWizardActions {
		if _, ok := e.srv.Actions().Get(n); !ok {
			t.Fatalf("action %s not registered", n)
		}
		if resp, _ := e.do(e.client(), "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", nil); resp.StatusCode != 401 {
			t.Errorf("%s unauthenticated: %d", n, resp.StatusCode)
		}
		if resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", map[string]string{"Content-Type": "application/json"}); resp.StatusCode != 403 {
			t.Errorf("%s without CSRF: %d", n, resp.StatusCode)
		}
		if resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", map[string]string{csrfHeader: csrf + "x"}); resp.StatusCode != 403 {
			t.Errorf("%s wrong CSRF: %d", n, resp.StatusCode)
		}
	}
	for _, p := range []string{"/pipelines/orders/repair", "/pipelines/orders/promote", "/api/v1/pipelines/orders/repair", "/api/v1/pipelines/orders/promote"} {
		if resp, _ := e.do(e.client(), "GET", p, "", map[string]string{"Accept": "application/json"}); resp.StatusCode != 401 && resp.StatusCode != 302 && resp.StatusCode != 303 {
			t.Errorf("GET %s unauthenticated: %d", p, resp.StatusCode)
		}
	}
	if len(e.fakeCalls()) != 0 || len(e.mock.Calls()) != 0 {
		t.Errorf("something ran without auth: %v", e.fakeCalls())
	}
}

func TestWizardConfirmNameRequired(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	for _, n := range []string{ActRepairBegin, ActRepairRun, ActPromoteRun, ActPromoteFailover} {
		for _, body := range []map[string]string{nil, {"confirm_name": "wrong"}} {
			if code, _ := e.act(c, csrf, "orders", n, body); code != 400 {
				t.Errorf("%s with %v: %d, want 400", n, body, code)
			}
		}
	}
	// Failover: the first retype alone is not enough.
	for _, body := range []map[string]string{{"confirm_name": "orders"}, {"confirm_name": "orders", "confirm_name2": "oders"}} {
		if code, b := e.act(c, csrf, "orders", ActPromoteFailover, body); code != 400 || !strings.Contains(b["error"].(string), "second time") {
			t.Errorf("failover %v: %d %v", body, code, b)
		}
	}
	if len(e.fakeCalls()) != 0 {
		t.Errorf("a backend ran without confirmation: %v", e.fakeCalls())
	}
}

const (
	cfgOrders  = "orders.yaml"
	leakTarget = "mysql://strand_admin:LeakyTargetPw@10.0.0.21:3306/strand"
	leakFence  = "mysql://strand_admin:LeakyFencePw@10.0.0.11:3306/"
)

func TestRepairWizardFlowBackground(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, cfgOrders)

	// Nothing works before begin.
	for _, n := range []string{ActRepairStop, ActRepairRun, ActRepairStart, ActRepairAbort} {
		if code, _ := e.act(c, csrf, "orders", n, map[string]string{"confirm_name": "orders"}); code != 409 {
			t.Errorf("%s before begin: %d, want 409", n, code)
		}
	}
	if code, b := e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders", "lag_threshold": "1"}); code != 200 {
		t.Fatalf("begin: %d %v", code, b)
	}
	if code, _ := e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders"}); code != 409 {
		t.Errorf("second begin: %d, want 409", code)
	}
	// Step ordering: 3 and 4 refused before 2.
	for _, n := range []string{ActRepairRun, ActRepairStart} {
		body := map[string]string{"confirm_name": "orders"}
		if code, b := e.act(c, csrf, "orders", n, body); code != 409 {
			t.Errorf("%s before stop: %d %v", n, code, b)
		}
	}
	if len(e.fakeCalls()) != 0 {
		t.Fatalf("something ran too early: %v", e.fakeCalls())
	}

	// Step 2 is refused while the lag is above the threshold.
	setTestLag(30)
	defer setTestLag(0.25)
	if code, b := e.act(c, csrf, "orders", ActRepairStop, nil); code != 409 || !strings.Contains(b["error"].(string), "above the threshold") {
		t.Fatalf("stop with lag: %d %v", code, b)
	}
	if len(e.fakeCalls()) != 0 {
		t.Fatalf("stop ran with lag above threshold: %v", e.fakeCalls())
	}
	// The wizard page shows the sampler's lag (rendering never calls the admin socket): take a sample first.
	e.sampleNow()
	v := e.wizState(c, "repair")
	if v["lag_ok"] != false || v["lag_seconds"] != float64(30) {
		t.Errorf("view lag: %v %v", v["lag_ok"], v["lag_seconds"])
	}

	setTestLag(0.5)
	if code, b := e.act(c, csrf, "orders", ActRepairStop, nil); code != 200 {
		t.Fatalf("stop: %d %v", code, b)
	}
	if got := e.fakeCalls(); len(got) != 1 || !reflect.DeepEqual(got[0].Args, []string{"stop", "-c", cfg}) || got[0].Env != "from-env-file" {
		t.Fatalf("stop argv: %v", got)
	}
	if !stepIs(e.wizState(c, "repair"), "verify") {
		t.Fatalf("state after stop: %v", e.wizState(c, "repair"))
	}
	// Stop twice, start before the repair: refused.
	if code, _ := e.act(c, csrf, "orders", ActRepairStop, nil); code != 409 {
		t.Errorf("second stop: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActRepairStart, nil); code != 409 {
		t.Errorf("start before repair: %d", code)
	}

	// Step 3.
	e.hold(true)
	code, b := e.act(c, csrf, "orders", ActRepairRun, map[string]string{"confirm_name": "orders"})
	if code != 200 || b["job_id"] == nil {
		t.Fatalf("repair-run: %d %v", code, b)
	}
	wantVerify := []string{"verify", "-c", cfg, "--mode", "full", "--repair", "fix"}
	waitFor(t, "verify call", func() bool { return len(e.fakeCalls()) == 2 })
	if got := e.fakeCalls()[1]; !reflect.DeepEqual(got.Args, wantVerify) || got.Env != "from-env-file" {
		t.Fatalf("verify argv: %v want %v", got, wantVerify)
	}
	// While it runs: no second repair, no start, other jobs refused (one job per pipeline), no abort.
	if code, _ := e.act(c, csrf, "orders", ActRepairRun, map[string]string{"confirm_name": "orders"}); code != 409 {
		t.Errorf("repair-run while running: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActRepairStart, nil); code != 409 {
		t.Errorf("start while repair runs: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActRepairAbort, nil); code != 409 {
		t.Errorf("abort while repair runs: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "delta"}); code != 409 {
		t.Errorf("verify while repair runs: %d", code)
	}
	e.hold(false)
	j := e.lastJobDone("repair")
	if j.Kind != "repair" || j.ExitCode == nil || *j.ExitCode != 0 {
		t.Fatalf("job: %+v", j)
	}

	// Step 4.
	if code, b := e.act(c, csrf, "orders", ActRepairStart, nil); code != 200 {
		t.Fatalf("start: %d %v", code, b)
	}
	calls := e.fakeCalls()
	if len(calls) != 3 || !reflect.DeepEqual(calls[2].Args, []string{"sync", "-c", cfg, "--background"}) {
		t.Fatalf("start argv: %v", calls)
	}
	if v := e.wizState(c, "repair"); !stepIs(v, "done") || v["active"] != false {
		t.Errorf("final state: %v", v)
	}
	// A new run is possible afterwards.
	if code, b := e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders", "table": "app.users"}); code != 200 {
		t.Errorf("begin again: %d %v", code, b)
	}

	// Audit: started + ok for every step, with the pipeline name.
	for _, n := range []string{ActRepairBegin, ActRepairStop, ActRepairRun, ActRepairStart} {
		if !auditHas(e.auditFor(n), "started") || !auditHas(e.auditFor(n), "ok") {
			t.Errorf("audit %s: %v", n, e.auditFor(n))
		}
	}
	if len(e.auditFor("job.end")) == 0 {
		t.Errorf("no job.end audit")
	}
}

func TestRepairWizardFailureAndRetry(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, cfgOrders)
	ok := map[string]string{"confirm_name": "orders"}
	e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders", "table": "app.users"})
	if code, b := e.act(c, csrf, "orders", ActRepairStop, nil); code != 200 {
		t.Fatalf("stop: %d %v", code, b)
	}
	// The CLI refuses (exit 9 here): the wizard stays at step 3, start is refused, the exit code is shown.
	e.setExit(9)
	e.setOut("pipeline is running; stop it first\n")
	if code, b := e.act(c, csrf, "orders", ActRepairRun, ok); code != 200 {
		t.Fatalf("run: %d %v", code, b)
	}
	e.lastJobDone("repair")
	v := e.wizState(c, "repair")
	st := v["state"].(map[string]any)
	if st["step"] != "verify" || st["last_exit"] != float64(9) {
		t.Fatalf("state: %v", st)
	}
	if !strings.Contains(v["output_tail"].(string), "stop it first") {
		t.Errorf("output tail: %v", v["output_tail"])
	}
	if code, _ := e.act(c, csrf, "orders", ActRepairStart, nil); code != 409 {
		t.Errorf("start after failed repair: %d", code)
	}
	waitFor(t, "calls", func() bool { return len(e.fakeCalls()) >= 2 })
	calls := e.fakeCalls()
	if got := calls[len(calls)-1].Args; !reflect.DeepEqual(got, []string{"verify", "-c", cfg, "--mode", "full", "--repair", "fix", "--table=app.users"}) {
		t.Errorf("argv with table: %v", got)
	}
	// Retry succeeds.
	e.setExit(0)
	e.act(c, csrf, "orders", ActRepairRun, ok)
	e.lastJobDone("repair")
	if !stepIs(e.wizState(c, "repair"), "start") {
		t.Fatalf("after retry: %v", e.wizState(c, "repair"))
	}
	// Abort leaves the pipeline as it is and closes the wizard.
	if code, b := e.act(c, csrf, "orders", ActRepairAbort, nil); code != 200 || !strings.Contains(b["message"].(string), "stays stopped") {
		t.Errorf("abort: %d %v", code, b)
	}
	if !stepIs(e.wizState(c, "repair"), "aborted") {
		t.Errorf("state after abort")
	}
}

func TestRepairWizardSystemdAndStoppedPipeline(t *testing.T) {
	e := opsEnv(t, true, nil)
	wizFake(t, e)
	e.cmd.setOut("systemctl ", "")
	e.sampleNow()
	c, csrf := e.loginOK()
	e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders"})
	if code, b := e.act(c, csrf, "orders", ActRepairStop, nil); code != 200 {
		t.Fatalf("stop: %d %v", code, b)
	}
	if got := e.cmd.callsWith("systemctl stop"); len(got) != 1 || got[0] != "systemctl stop strand@orders.service" {
		t.Errorf("systemctl stop: %v", got)
	}
	// systemctl failure surfaces the exit code and keeps the wizard at step 2.
	e.act(c, csrf, "orders", ActRepairAbort, nil)
	e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders"})
	e.cmd.setFail(map[string]int{"systemctl stop": 5})
	code, _ := e.act(c, csrf, "orders", ActRepairStop, nil)
	if code != 502 {
		t.Fatalf("failed stop: %d", code)
	}
	a := e.auditFor(ActRepairStop)
	if last := a[len(a)-1]; last["result"] != "error" || last["exit_code"] != float64(5) {
		t.Errorf("audit: %v", last)
	}
	if !stepIs(e.wizState(c, "repair"), "wait") {
		t.Errorf("state after failed stop: %v", e.wizState(c, "repair"))
	}
	e.cmd.setFail(nil)
	// Repair job then start via systemctl.
	e.act(c, csrf, "orders", ActRepairStop, nil)
	e.act(c, csrf, "orders", ActRepairRun, map[string]string{"confirm_name": "orders"})
	e.lastJobDone("repair")
	if code, _ := e.act(c, csrf, "orders", ActRepairStart, nil); code != 200 {
		t.Fatalf("start: %d", code)
	}
	if got := e.cmd.callsWith("systemctl start"); len(got) != 1 || got[0] != "systemctl start strand@orders.service" {
		t.Errorf("systemctl start: %v", got)
	}
}

func TestWizardStateSurvivesRestart(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	e.act(c, csrf, "orders", ActRepairBegin, map[string]string{"confirm_name": "orders", "lag_threshold": "2"})
	e.act(c, csrf, "orders", ActRepairStop, nil)
	e.act(c, csrf, "orders", ActRepairRun, map[string]string{"confirm_name": "orders"})
	e.lastJobDone("repair")

	// "Restart": a second server on the same state dir and the same config.
	e.srv.releaseLease()
	srv2, err := New(Options{Config: e.srv.cfg, Version: "test", Commander: e.cmd, Now: e.clock.Now, Store: e.srv.db.st})
	if err != nil {
		t.Fatal(err)
	}
	e2 := *e
	e2.srv = srv2
	ts2 := httpTestServer(srv2)
	defer ts2.Close()
	e2.ts = ts2
	c2, csrf2 := e2.loginOK()
	v := e2.wizState(c2, "repair")
	if !stepIs(v, "start") {
		t.Fatalf("resumed state: %v", v["state"])
	}
	if v["state"].(map[string]any)["opts"].(map[string]any)["lag_threshold"] != "2" {
		t.Errorf("opts not persisted: %v", v["state"])
	}
	if code, b := e2.act(c2, csrf2, "orders", ActRepairStart, nil); code != 200 {
		t.Fatalf("start after restart: %d %v", code, b)
	}
	if !stepIs(e2.wizState(c2, "repair"), "done") {
		t.Errorf("not done")
	}
	// The state lives in the metadata database, not in a state file.
	if _, err := os.Stat(filepath.Join(e.srv.cfg.StateDir, "wizards", "repair-orders.json")); err == nil {
		t.Error("wizard state written to a file")
	}
	if _, ok, err := e.srv.db.st.GetWizard(context.Background(), "repair", "orders"); !ok || err != nil {
		t.Errorf("wizard state not in the database: %v", err)
	}
}

func promoteBody(extra map[string]string) map[string]string {
	m := map[string]string{"target_dsn": leakTarget, "fence_dsn": leakFence}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestPromoteWizardNormalFlow(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, cfgOrders)
	// pipelineYAML targets postgres.
	base := []string{"promote", "-c", cfg, "--pipeline=orders", "--engine=pg"}

	// Ordering: promote and failover are refused before the dry-run succeeded; DSNs are validated.
	if code, _ := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(map[string]string{"confirm_name": "orders"})); code != 409 {
		t.Errorf("promote before plan: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActPromotePlan, map[string]string{"fence_dsn": leakFence}); code != 400 {
		t.Errorf("plan without target: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActPromotePlan, map[string]string{"target_dsn": leakTarget}); code != 400 {
		t.Errorf("plan without fence: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActPromotePlan, promoteBody(map[string]string{"catchup_timeout": "5m; rm -rf /"})); code != 400 {
		t.Errorf("bad catchup: %d", code)
	}
	if len(e.fakeCalls()) != 0 {
		t.Fatalf("ran with invalid input: %v", e.fakeCalls())
	}

	e.setOut(`{"time":"x","level":"INFO","msg":"resuming promote from interrupted step","pipeline":"orders","current_step":"apply_remaining","completed_steps":["fence_source","hold_readonly"]}` + "\nPlan promote (dry-run): OK\n")
	code, b := e.act(c, csrf, "orders", ActPromotePlan, promoteBody(map[string]string{"catchup_timeout": "10m", "app_role": "app_rw", "schemas": "public,sales"}))
	if code != 200 {
		t.Fatalf("plan: %d %v", code, b)
	}
	j := e.lastJobDone("promote")
	wantPlan := append(append([]string{}, base...), "--catchup-timeout=10m", "--app-role=app_rw", "--schemas=public,sales", "--dry-run")
	if got := e.fakeCalls()[0]; !reflect.DeepEqual(got.Args, wantPlan) || got.Env != "from-env-file" {
		t.Fatalf("plan argv:\n%v\nwant\n%v", got.Args, wantPlan)
	}
	// DSN đi qua môi trường của tiến trình con, không qua argv (ps đọc được argv).
	if got := e.fakeCalls()[0]; got.SecretEnv != leakTarget+"|"+leakFence {
		t.Fatalf("plan DSN env = %q", got.SecretEnv)
	}
	// The job record never holds the DSNs.
	if strings.Contains(strings.Join(j.Args, " "), "Leaky") || !strings.Contains(strings.Join(j.Args, " "), "--target=<env>") {
		t.Errorf("job args leak: %v", j.Args)
	}
	v := e.wizState(c, "promote")
	if !stepIs(v, "ready") || !strings.Contains(v["promote_state"].(string), "apply_remaining") {
		t.Fatalf("after plan: %v / %v", v["state"], v["promote_state"])
	}
	if v["state"].(map[string]any)["opts"].(map[string]any)["catchup_timeout"] != "10m" {
		t.Errorf("opts: %v", v["state"])
	}
	// Failover is not offered without exit 10.
	fo := promoteBody(map[string]string{"confirm_name": "orders", "confirm_name2": "orders"})
	if code, _ := e.act(c, csrf, "orders", ActPromoteFailover, fo); code != 409 {
		t.Errorf("failover without decision: %d", code)
	}

	// Promote (single retype).
	e.setOut("Promote pipeline orders thành công.\n")
	if code, b := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(map[string]string{"confirm_name": "orders", "fence_timeout": "30s", "objects_post": "true"})); code != 400 {
		t.Fatalf("objects_post on PG must be refused: %d %v", code, b)
	}
	if code, b := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(map[string]string{"confirm_name": "orders", "peak_rate": "500", "source_app_roles": "app_rw"})); code != 200 {
		t.Fatalf("promote: %d %v", code, b)
	}
	e.lastJobDone("promote")
	wantRun := append(append([]string{}, base...), "--peak-rate=500", "--source-app-roles=app_rw")
	waitFor(t, "calls", func() bool { return len(e.fakeCalls()) >= 2 })
	calls := e.fakeCalls()
	if got := calls[len(calls)-1].Args; !reflect.DeepEqual(got, wantRun) {
		t.Fatalf("promote argv:\n%v\nwant\n%v", got, wantRun)
	}
	if !stepIs(e.wizState(c, "promote"), "done") {
		t.Fatalf("final state: %v", e.wizState(c, "promote"))
	}
	// Audit: DSNs hidden, every step audited.
	for _, n := range []string{ActPromotePlan, ActPromoteRun} {
		if !auditHas(e.auditFor(n), "started") || !auditHas(e.auditFor(n), "ok") {
			t.Errorf("audit %s: %v", n, e.auditFor(n))
		}
	}
	assertNoSecret(t, e, "LeakyTargetPw", "LeakyFencePw")
}

func assertNoSecret(t *testing.T, e *testEnv, secrets ...string) {
	t.Helper()
	root := e.srv.cfg.StateDir
	_ = filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		b, _ := os.ReadFile(p)
		for _, s := range secrets {
			if strings.Contains(string(b), s) {
				t.Errorf("secret %q found in %s", s, p)
			}
		}
		return nil
	})
}

func TestPromoteWizardNeedsDecisionAndFailover(t *testing.T) {
	e := opsEnv(t, false, func(c *Config) {})
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, cfgOrders)
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(nil))
	e.lastJobDone("promote")

	// The CLI stops with exit 10.
	e.setExit(10)
	e.setOut("noise\n\n=== PROMOTE NEEDS OPERATOR DECISION ===\nTrạng thái pipeline: awaiting_operator\nDữ liệu sẽ mất nếu tiếp tục: applied uuid:1-99, last checkpoint 2026-11-15T10:00:00Z\n")
	ok := map[string]string{"confirm_name": "orders"}
	if code, b := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(ok)); code != 200 {
		t.Fatalf("promote: %d %v", code, b)
	}
	j := e.lastJobDone("promote")
	if j.ExitCode == nil || *j.ExitCode != 10 {
		t.Fatalf("job: %+v", j)
	}
	v := e.wizState(c, "promote")
	st := v["state"].(map[string]any)
	if st["step"] != "decision" || st["last_exit"] != float64(10) {
		t.Fatalf("state: %v", st)
	}
	dec, _ := v["decision"].(string)
	if !strings.HasPrefix(dec, "=== PROMOTE NEEDS OPERATOR DECISION ===") || !strings.Contains(dec, "Dữ liệu sẽ mất nếu tiếp tục: applied uuid:1-99") {
		t.Errorf("decision text: %q", dec)
	}
	can := v["can"].(map[string]any)
	if can[ActPromoteFailover] != true || can[ActPromoteRun] != true {
		t.Errorf("can: %v", can)
	}
	// The page shows the CLI message and the options.
	resp, page := e.do(c, "GET", "/pipelines/orders/promote", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(page, "PROMOTE NEEDS OPERATOR DECISION") || !strings.Contains(page, "Failover...") ||
		!strings.Contains(page, `data-confirm2=`) || strings.Contains(page, "Leaky") {
		t.Errorf("promote page: %d (missing decision/failover dialog, or leaks)", resp.StatusCode)
	}

	// Failover: one retype is not enough; both are.
	fo := promoteBody(ok)
	if code, _ := e.act(c, csrf, "orders", ActPromoteFailover, fo); code != 400 {
		t.Errorf("failover with one retype: %d", code)
	}
	fo["confirm_name2"] = "orders"
	delete(fo, "fence_dsn") // the dead source has no usable DSN
	fo["override_sequences"] = `{"public.orders_id_seq": 5000}`
	e.setExit(0)
	e.setOut("Promote pipeline orders thành công.\n")
	if code, b := e.act(c, csrf, "orders", ActPromoteFailover, fo); code != 200 {
		t.Fatalf("failover: %d %v", code, b)
	}
	e.lastJobDone("promote")
	waitFor(t, "calls", func() bool { return len(e.fakeCalls()) >= 3 })
	calls := e.fakeCalls()
	want := []string{"promote", "-c", cfg, "--pipeline=orders", "--engine=pg",
		`--override-sequences={"public.orders_id_seq":5000}`, "--source-fenced-externally"}
	if got := calls[len(calls)-1].Args; !reflect.DeepEqual(got, want) {
		t.Fatalf("failover argv:\n%v\nwant\n%v", got, want)
	}
	if got := calls[len(calls)-1].SecretEnv; got != leakTarget+"|" {
		t.Fatalf("failover DSN env = %q", got)
	}
	if !stepIs(e.wizState(c, "promote"), "done") {
		t.Errorf("final: %v", e.wizState(c, "promote"))
	}
	if a := e.auditFor(ActPromoteFailover); len(a) == 0 {
		t.Errorf("no failover audit")
	} else {
		for _, l := range a {
			if s := fmt.Sprint(l["args"]); strings.Contains(s, "Leaky") || !strings.Contains(s, "target_dsn=<hidden>") {
				t.Errorf("failover audit args: %v", s)
			}
		}
	}
	assertNoSecret(t, e, "LeakyTargetPw", "LeakyFencePw")
}

func TestPromoteWizardFailureRetryAndAbort(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(nil))
	e.lastJobDone("promote")
	e.setExit(3)
	e.setOut("fence source failed: boom\n")
	ok := map[string]string{"confirm_name": "orders"}
	e.act(c, csrf, "orders", ActPromoteRun, promoteBody(ok))
	e.lastJobDone("promote")
	v := e.wizState(c, "promote")
	st := v["state"].(map[string]any)
	if st["step"] != "failed" || st["last_exit"] != float64(3) || v["decision"] != nil {
		t.Fatalf("state: %v", st)
	}
	// Failover is only for exit 10.
	fo := promoteBody(ok)
	fo["confirm_name2"] = "orders"
	if code, _ := e.act(c, csrf, "orders", ActPromoteFailover, fo); code != 409 {
		t.Errorf("failover after exit 3: %d", code)
	}
	// Retry (CLI resumes) succeeds.
	e.setExit(0)
	e.act(c, csrf, "orders", ActPromoteRun, promoteBody(ok))
	e.lastJobDone("promote")
	if !stepIs(e.wizState(c, "promote"), "done") {
		t.Errorf("after retry: %v", e.wizState(c, "promote"))
	}
	// New wizard after done, then abort.
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(nil))
	e.lastJobDone("promote")
	if code, b := e.act(c, csrf, "orders", ActPromoteAbort, nil); code != 200 {
		t.Errorf("abort: %d %v", code, b)
	}
	if code, _ := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(ok)); code != 409 {
		t.Errorf("promote after abort: %d", code)
	}
}

func TestPromoteWizardMySQLTarget(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	// Same pipeline, but a MySQL target: --engine=mysql and --objects-post allowed, PG-only flags dropped.
	cp := filepath.Join(e.dir, cfgOrders)
	raw, _ := os.ReadFile(cp)
	e.writePipelineFile("orders", cp, strings.Replace(string(raw), "kind: postgres", "kind: mysql", 1), 0o640)
	e.sampleNow()
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, cfgOrders)
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(map[string]string{"app_role": "x", "schemas": "y", "objects_post": "true"}))
	e.lastJobDone("promote")
	want := []string{"promote", "-c", cfg, "--pipeline=orders", "--engine=mysql", "--objects-post", "--dry-run"}
	if got := e.fakeCalls()[0].Args; !reflect.DeepEqual(got, want) {
		t.Errorf("argv %v want %v", got, want)
	}
}

func httpTestServer(s *Server) *httptest.Server { return httptest.NewServer(s.Handler()) }

func auditHas(lines []map[string]any, result string) bool {
	for _, l := range lines {
		if l["result"] == result && l["pipeline"] == "orders" {
			return true
		}
	}
	return false
}

func TestWizardPagesRenderInEveryStep(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.ordersMySQLTarget()
	e.sampleNow()
	c, csrf := e.loginOK()
	get := func(path string) string {
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s: %d %.300s", path, resp.StatusCode, body)
		}
		return body
	}
	ok := map[string]string{"confirm_name": "orders"}
	if p := get("/pipelines/orders/repair"); !strings.Contains(p, "Start the repair wizard") || !strings.Contains(p, `data-retype="orders"`) {
		t.Errorf("repair page, no wizard")
	}
	e.act(c, csrf, "orders", ActRepairBegin, ok)
	if p := get("/pipelines/orders/repair"); !strings.Contains(p, "Steps 1 and 2") || !strings.Contains(p, "Stop the pipeline") {
		t.Errorf("repair page, wait step")
	}
	e.act(c, csrf, "orders", ActRepairStop, nil)
	if p := get("/pipelines/orders/repair"); !strings.Contains(p, "Run repair...") || !strings.Contains(p, "--repair fix") {
		t.Errorf("repair page, verify step")
	}
	e.act(c, csrf, "orders", ActRepairRun, ok)
	e.lastJobDone("repair")
	if p := get("/pipelines/orders/repair"); !strings.Contains(p, "Step 4") {
		t.Errorf("repair page, start step")
	}
	if p := get("/pipelines/orders/promote"); !strings.Contains(p, "Check state (dry-run)") || !strings.Contains(p, "Failover...") {
		t.Errorf("promote page, no wizard")
	}
	if p := get("/pipelines/orders"); !strings.Contains(p, `href="/pipelines/orders/repair"`) || !strings.Contains(p, `href="/pipelines/orders/promote"`) {
		t.Errorf("detail tabs lack Repair/Promote links")
	}
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(nil))
	e.lastJobDone("promote")
	if p := get("/pipelines/orders/promote"); !strings.Contains(p, "promote-plan") || strings.Contains(p, "Leaky") {
		t.Errorf("promote page, ready step")
	}
}

// Exit 10 vì source mất sau fence: trang hiện lý do, lời khuyên đúng trường hợp và bảng sequence gợi ý,
// nút điền sẵn override_sequences mang JSON gợi ý (người vận hành vẫn bấm Promote/Failover).
func TestPromoteWizardStructuredDecision(t *testing.T) {
	e := opsEnv(t, false, func(c *Config) {})
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	e.act(c, csrf, "orders", ActPromotePlan, promoteBody(nil))
	e.lastJobDone("promote")

	d := promote.Decision{
		Reason: promote.DecisionSourceLostAfterFence, Fenced: true, CurrentStep: promote.StepApplyRemaining,
		CompletedSteps:     []promote.PromoteStep{promote.StepFenceSource, promote.StepHoldReadOnly},
		FailoverSummary:    "target applied up to uuid:1-42",
		SuggestedSequences: map[string]int64{"public.z_seq": 900, "public.a_seq": 12345},
		Message:            "the source became unreachable after it was fenced",
	}
	e.setExit(10)
	e.setOut("=== PROMOTE NEEDS OPERATOR DECISION ===\nTrạng thái pipeline: awaiting_operator\n" + d.Line() + "\nError [promote_needs_decision]: x\n")
	if code, b := e.act(c, csrf, "orders", ActPromoteRun, promoteBody(map[string]string{"confirm_name": "orders"})); code != 200 {
		t.Fatalf("promote: %d %v", code, b)
	}
	e.lastJobDone("promote")
	v := e.wizState(c, "promote")
	dd, _ := v["decision_detail"].(map[string]any)
	if dd == nil || dd["reason"] != promote.DecisionSourceLostAfterFence || dd["fenced"] != true ||
		dd["override_json"] != `{"public.a_seq":12345,"public.z_seq":900}` || dd["failover_offered"] != true {
		t.Fatalf("decision_detail = %v", dd)
	}
	seqs, _ := dd["suggested_sequences"].([]any)
	if len(seqs) != 2 || seqs[0].(map[string]any)["name"] != "public.a_seq" {
		t.Fatalf("sequences = %v", seqs)
	}
	resp, page := e.do(c, "GET", "/pipelines/orders/promote", "", nil)
	for _, want := range []string{
		"The source went away after it was fenced", "fenced (read-only)", "target applied up to uuid:1-42",
		"Wait for the source to come back", `data-fill-name="override_sequences"`,
		`data-fill-value="{&#34;public.a_seq&#34;:12345,&#34;public.z_seq&#34;:900}"`, ">12345<", "CLI report",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("promote page (%d) misses %q", resp.StatusCode, want)
		}
	}

	// Dòng JSON hỏng: trang quay về hiện nguyên văn như trước, không có decision_detail.
	e.setOut("=== PROMOTE NEEDS OPERATOR DECISION ===\n" + promote.DecisionLinePrefix + "{broken\n")
	e.act(c, csrf, "orders", ActPromoteRun, promoteBody(map[string]string{"confirm_name": "orders"}))
	e.lastJobDone("promote")
	if v := e.wizState(c, "promote"); v["decision_detail"] != nil || !strings.Contains(fmt.Sprint(v["decision"]), "PROMOTE NEEDS OPERATOR DECISION") {
		t.Errorf("fallback: detail=%v decision=%v", v["decision_detail"], v["decision"])
	}
}

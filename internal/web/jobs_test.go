package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strand/internal/web/store"
	"strings"
	"syscall"
	"testing"
	"time"

	"strand/internal/backup"
)

func TestBackupProcessJobLinks(t *testing.T) {
	e := newCPEnv(t)
	e.st.SeedBackupJob(backup.Job{ID: "bjob-shop", Name: "Shop backups", EverySeconds: 3600})
	runner, err := NewFileJobRunner(t.TempDir(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	const runID, restoreID = "20261115T100000Z-deadbeef", "20261115T100001Z-deadbeef"
	runner.jobs[runID] = &JobStatus{ID: runID, Pipeline: backupJobRunPipeline("bjob-shop"), Kind: "backup-run",
		Args: []string{"backup", "schedule", "--job-id", "bjob-shop", "--once"}, Running: true}
	runner.jobs[restoreID] = &JobStatus{ID: restoreID, Pipeline: "backup-restore-r-1", Kind: "backup-restore",
		Args: []string{"backup", "restore", "--id", "bkp-shop"}}
	e.srv.SetJobRunner(runner)
	a := e.admin()
	const runs = "/backups/jobs/bjob-shop?tab=runs"
	for path, want := range map[string]string{"/jobs": runs, "/jobs/" + runID: runs, "/jobs/" + restoreID: "/backups"} {
		body := a.must("GET", path, nil, 200)
		if !strings.Contains(body, `href="`+want+`"`) || strings.Contains(body, `href="/pipelines/run-bjob-shop`) ||
			strings.Contains(body, `href="/pipelines/backup-restore-r-1`) || strings.Contains(body, `data-action="job-cancel"`) {
			t.Errorf("%s did not show a valid backup link and actions", path)
		}
	}
	a.must("GET", runs, nil, 200)
}

func (e *testEnv) job(c interface{}, id string) JobStatus {
	j, ok := e.srv.jobs.Get(id)
	if !ok {
		e.t.Fatalf("job %s unknown", id)
	}
	return j
}

func TestVerifyJobsArgvAndEnv(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, "orders.yaml")
	for _, tc := range []struct {
		body map[string]string
		want []string
	}{
		{map[string]string{"mode": "sample", "table": "app.users", "rate": "0.05"}, []string{"verify", "-c", cfg, "--mode", "sample", "--table=app.users", "--rate=0.05"}},
		{map[string]string{"mode": "full"}, []string{"verify", "-c", cfg, "--mode", "full"}},
		{map[string]string{"mode": "full", "table": "app.orders", "rate": "50", "resume": "true"}, []string{"verify", "-c", cfg, "--mode", "full", "--table=app.orders", "--rate=50", "--resume"}},
		{map[string]string{"mode": "delta"}, []string{"verify", "-c", cfg, "--mode", "delta", "--socket=" + e.sock}},
	} {
		code, body := e.act(c, csrf, "orders", ActVerify, tc.body)
		if code != 200 || body["job_id"] == nil {
			t.Fatalf("verify %v: %d %v", tc.body, code, body)
		}
		id := body["job_id"].(string)
		waitFor(t, "job end", func() bool { return !e.job(c, id).Running })
		calls := e.fakeCalls()
		last := calls[len(calls)-1]
		if !reflect.DeepEqual(last.Args, tc.want) {
			t.Errorf("argv %v, want %v", last.Args, tc.want)
		}
		if last.Env != "from-env-file" {
			t.Errorf("env_file not passed to the child: %q", last.Env)
		}
		j := e.job(c, id)
		if j.ExitCode == nil || *j.ExitCode != 0 || j.Pipeline != "orders" || j.Kind != "verify" {
			t.Errorf("job status %+v", j)
		}
	}
}

func TestVerifyJobStreamingHistoryAndAudit(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	e.hold(true)
	e.setExit(5) // verify mismatch
	code, body := e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "full"})
	if code != 200 {
		t.Fatalf("start: %d %v", code, body)
	}
	id := body["job_id"].(string)

	// Live output while the process is still running: the first lines show up, with the DSN password masked.
	var off int64
	var got strings.Builder
	waitFor(t, "first output", func() bool {
		resp, b := e.do(c, "GET", fmt.Sprintf("/api/v1/jobs/%s/output?offset=%d", id, off), "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("output: %d", resp.StatusCode)
		}
		var o JobOutput
		if err := json.Unmarshal([]byte(b), &o); err != nil {
			t.Fatal(err)
		}
		if !o.Running {
			t.Fatalf("job should still be running")
		}
		got.WriteString(o.Data)
		off = o.Offset
		return strings.Contains(got.String(), "line one of verify")
	})
	if strings.Contains(got.String(), "LeakyPw") || !strings.Contains(got.String(), "mysql://u:********@h/db") {
		t.Errorf("output not masked: %q", got.String())
	}
	if strings.Contains(got.String(), "line two") {
		t.Errorf("line two printed before release")
	}
	// A second job for the same pipeline is refused while one runs.
	for _, body := range []map[string]string{{"mode": "full"}, {"mode": "sample", "table": "app.users"}} {
		code, resp := e.act(c, csrf, "orders", ActVerify, body)
		if code != 409 || !strings.Contains(resp["error"].(string), id) {
			t.Errorf("second job: %d %v, want 409 naming %s", code, resp, id)
		}
	}
	if n := len(e.srv.jobs.List("orders")); n != 1 {
		t.Errorf("%d jobs listed, want 1", n)
	}
	// Release: the rest streams, the exit code is captured.
	e.hold(false)
	waitFor(t, "job end", func() bool { return !e.job(c, id).Running })
	var o JobOutput
	_, b := e.do(c, "GET", fmt.Sprintf("/api/v1/jobs/%s/output?offset=%d", id, off), "", nil)
	_ = json.Unmarshal([]byte(b), &o)
	got.WriteString(o.Data)
	if o.Running || o.ExitCode == nil || *o.ExitCode != 5 || !strings.Contains(got.String(), "line two") {
		t.Errorf("final output: %+v / %q", o, got.String())
	}
	j := e.job(c, id)
	if j.ExitCode == nil || *j.ExitCode != 5 || j.Ended == nil {
		t.Errorf("job %+v", j)
	}
	// History persisted in the metadata database: job row + output.
	jobsDir := filepath.Join(e.srv.cfg.StateDir, "jobs")
	if b, err := e.srv.db.st.ReadJobOutput(context.Background(), id, 0, 1<<20); err != nil || !strings.Contains(string(b), "line two") {
		t.Errorf("job output not in the database: %q %v", b, err)
	}
	// A fresh runner on the same database (web restart) sees the history.
	r2, err := newJobRunner(jobsDir, e.bin, nil, e.srv.db.st, false)
	if err != nil {
		t.Fatal(err)
	}
	if l := r2.List("orders"); len(l) != 1 || l[0].ID != id || *l[0].ExitCode != 5 {
		t.Errorf("history after reload: %+v", l)
	}
	// Audit: started + ok (action) and a job.end line with the exit code.
	end := e.auditFor("job.end")
	if len(end) != 1 || end[0]["result"] != "error" || end[0]["exit_code"] != float64(5) || end[0]["pipeline"] != "orders" {
		t.Errorf("job.end audit: %v", end)
	}
	// After the job ended a new one is accepted.
	e.setExit(0)
	code2, body2 := e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "full"})
	if code2 != 200 {
		t.Fatalf("job after completion: %d", code2)
	}
	// Chờ job thứ hai kết thúc: runner ghi <id>.json khi job xong; nếu test trả về trước đó thì lần ghi
	// rơi vào lúc t.TempDir đang xóa thư mục ("directory not empty", chập chờn ~6%).
	id2 := body2["job_id"].(string)
	defer waitFor(t, "second job end", func() bool { return !e.job(c, id2).Running })
	// Listing endpoints and pages.
	resp, b := e.do(c, "GET", "/api/v1/pipelines/orders/jobs", "", nil)
	var lst struct{ Jobs []JobStatus }
	if resp.StatusCode != 200 || json.Unmarshal([]byte(b), &lst) != nil || len(lst.Jobs) != 2 {
		t.Errorf("pipeline jobs: %d %s", resp.StatusCode, b)
	}
	if resp, b := e.do(c, "GET", "/api/v1/jobs?pipeline=orders", "", nil); resp.StatusCode != 200 || !strings.Contains(b, id) {
		t.Errorf("jobs: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/jobs/"+id, "", nil); resp.StatusCode != 200 {
		t.Errorf("job: %d", resp.StatusCode)
	}
	for _, bad := range []string{"/api/v1/jobs/..%2f..%2fetc", "/api/v1/jobs/nope", "/api/v1/jobs/20261115T100000Z-deadbeef/output"} {
		if resp, _ := e.do(c, "GET", bad, "", nil); resp.StatusCode != 404 {
			t.Errorf("GET %s: %d, want 404", bad, resp.StatusCode)
		}
	}
	resp, b = e.do(c, "GET", "/jobs/"+id, "", nil)
	if resp.StatusCode != 200 || !strings.Contains(b, "line two") || !strings.Contains(b, "exit 5") || strings.Contains(b, "LeakyPw") {
		t.Errorf("job page: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/jobs/20261115T100000Z-deadbeef", "", nil); resp.StatusCode != 404 {
		t.Errorf("unknown job page: %d", resp.StatusCode)
	}
	// Unauthenticated access is refused.
	for _, p := range []string{"/api/v1/jobs", "/api/v1/jobs/" + id, "/api/v1/jobs/" + id + "/output", "/api/v1/pipelines/orders/jobs"} {
		if resp, _ := e.do(e.client(), "GET", p, "", nil); resp.StatusCode != 401 {
			t.Errorf("GET %s without session: %d", p, resp.StatusCode)
		}
	}
}

func TestJobCancelAction(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	e.hold(true)
	_, body := e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "full"})
	id := body["job_id"].(string)
	waitFor(t, "output", func() bool { o, _ := e.srv.jobs.Output(id, 0); return strings.Contains(o.Data, "line one") })
	// A job id of another pipeline is not cancellable through this pipeline.
	if code, _ := e.act(c, csrf, "orders", ActJobCancel, map[string]string{"job_id": "20261115T100000Z-deadbeef"}); code != 404 {
		t.Errorf("cancel unknown: %d", code)
	}
	if code, b := e.act(c, csrf, "orders", ActJobCancel, map[string]string{"job_id": id}); code != 200 {
		t.Fatalf("cancel: %d %v", code, b)
	}
	waitFor(t, "cancelled", func() bool { return !e.job(c, id).Running })
	j := e.job(c, id)
	if !j.Cancelled {
		t.Errorf("job not flagged cancelled: %+v", j)
	}
	if code, _ := e.act(c, csrf, "orders", ActJobCancel, map[string]string{"job_id": id}); code != 409 {
		t.Errorf("cancel finished job: %d, want 409", code)
	}
	if end := e.auditFor("job.end"); len(end) != 1 || end[0]["detail"].(map[string]any)["cancelled"] != true {
		t.Errorf("job.end audit: %v", end)
	}
}

// --- FileJobRunner ---

func newRunner(t *testing.T) (*FileJobRunner, string, string) {
	t.Helper()
	dir := t.TempDir()
	bin := newFakeStrand(t, dir)
	r, err := NewFileJobRunner(filepath.Join(dir, "jobs"), bin, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r, bin, dir
}

func TestRunnerOutputOnlyCompleteLinesWhileRunning(t *testing.T) {
	r, _, dir := newRunner(t)
	_ = os.WriteFile(filepath.Join(dir, "hold"), nil, 0o600)
	id, err := r.Start(nil, JobSpec{Pipeline: "p", Kind: "verify", Args: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "output", func() bool { o, _ := r.Output(id, 0); return o.Data != "" })
	o, _ := r.Output(id, 0)
	if !strings.HasSuffix(o.Data, "\n") || !o.Running {
		t.Errorf("chunk %+v must end at a line boundary while running", o)
	}
	// Offsets chain: asking from the returned offset yields nothing new until more is written.
	o2, _ := r.Output(id, o.Offset)
	if o2.Data != "" || o2.Offset != o.Offset {
		t.Errorf("second read %+v", o2)
	}
	// Append a partial line directly to the log: it is withheld until completed or the job ends.
	f, _ := os.OpenFile(filepath.Join(dir, "jobs", id+".log"), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString("partial")
	f.Close()
	if o3, _ := r.Output(id, o.Offset); o3.Data != "" {
		t.Errorf("partial line leaked: %q", o3.Data)
	}
	_ = os.Remove(filepath.Join(dir, "hold"))
	waitFor(t, "end", func() bool { j, _ := r.Get(id); return !j.Running })
	o4, _ := r.Output(id, o.Offset)
	if !strings.Contains(o4.Data, "partial") || !strings.Contains(o4.Data, "line two") || o4.Running {
		t.Errorf("final chunk %+v", o4)
	}
	// Offset beyond the end is clamped, not an error.
	if o5, ok := r.Output(id, 1<<40); !ok || o5.Data != "" {
		t.Errorf("clamp: %+v %v", o5, ok)
	}
	if _, ok := r.Output("20261115T100000Z-deadbeef", 0); ok {
		t.Errorf("unknown job reported as existing")
	}
}

func TestRunnerOnePerPipelineAndExitCode(t *testing.T) {
	r, _, dir := newRunner(t)
	_ = os.WriteFile(filepath.Join(dir, "hold"), nil, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "exitcode"), []byte("7"), 0o600)
	ended := make(chan JobStatus, 4)
	r.OnEnd = func(j JobStatus) { ended <- j }
	id1, err := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"y"}}); err == nil {
		t.Fatal("second job for the same pipeline must be refused")
	} else if re, ok := err.(*ErrJobRunning); !ok || re.ID != id1 {
		t.Fatalf("error %v", err)
	}
	// Another pipeline is independent.
	id2, err := r.Start(nil, JobSpec{Pipeline: "b", Kind: "verify", Args: []string{"z"}})
	if err != nil {
		t.Fatalf("other pipeline refused: %v", err)
	}
	_ = os.Remove(filepath.Join(dir, "hold"))
	for i := 0; i < 2; i++ {
		select {
		case j := <-ended:
			if j.ExitCode == nil || *j.ExitCode != 7 || j.Running || j.Ended == nil {
				t.Errorf("OnEnd %+v", j)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("OnEnd not called")
		}
	}
	_ = id2
	// Started again right after the end.
	if _, err := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"x"}}); err != nil {
		t.Fatalf("after end: %v", err)
	}
	// Chờ job cuối xong (runner ghi <id>.json khi kết thúc) để không ghi vào lúc t.TempDir đang xóa.
	select {
	case <-ended:
	case <-time.After(8 * time.Second):
		t.Fatal("last job did not end")
	}
}

func TestRunnerStartFailureLeavesNoJob(t *testing.T) {
	dir := t.TempDir()
	r, err := NewFileJobRunner(filepath.Join(dir, "jobs"), filepath.Join(dir, "missing-binary"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify"}); err == nil {
		t.Fatal("expected start error")
	}
	if l := r.List(""); len(l) != 0 {
		t.Errorf("failed start left jobs: %+v", l)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "jobs"))
	if len(ents) != 0 {
		t.Errorf("failed start left files: %v", ents)
	}
}

func TestRunnerReapsChildNoZombie(t *testing.T) {
	r, _, _ := newRunner(t)
	id, _ := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"x"}})
	j, _ := r.Get(id)
	pid := j.PID
	waitFor(t, "end", func() bool { j, _ := r.Get(id); return !j.Running })
	// cmd.Wait reaped it: /proc/<pid> is gone (a zombie would still be listed).
	waitFor(t, "reaped", func() bool { _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); return err != nil })
}

// Web restart while a job runs: the child keeps running (own process group, output goes to a file), the new
// runner adopts it, keeps the one-per-pipeline rule, and closes the job when the process ends. The exit code of
// an adopted job cannot be observed (it is no longer our child), which is recorded in the job.
func TestRunnerAdoptsJobAcrossWebRestart(t *testing.T) {
	r1, bin, dir := newRunner(t)
	_ = os.WriteFile(filepath.Join(dir, "hold"), nil, 0o600)
	id, err := r1.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "output", func() bool { o, _ := r1.Output(id, 0); return o.Data != "" })
	// "Web server dies": a second runner over the same directory plays the restarted server.
	r2, err := NewFileJobRunner(filepath.Join(dir, "jobs"), bin, nil)
	if err != nil {
		t.Fatal(err)
	}
	j, ok := r2.Get(id)
	if !ok || !j.Running {
		t.Fatalf("restarted runner lost the running job: %+v %v", j, ok)
	}
	if _, err := r2.Start(nil, JobSpec{Pipeline: "a", Kind: "verify"}); err == nil {
		t.Errorf("adopted job must still block a second job for the pipeline")
	}
	if o, _ := r2.Output(id, 0); !strings.Contains(o.Data, "line one") {
		t.Errorf("output of adopted job: %q", o.Data)
	}
	ended := make(chan JobStatus, 1)
	r2.mu.Lock()
	r2.OnEnd = func(j JobStatus) { ended <- j }
	r2.mu.Unlock()
	_ = os.Remove(filepath.Join(dir, "hold"))
	select {
	case j := <-ended:
		if j.Running || j.ExitCode != nil || j.Ended == nil || !strings.Contains(j.Error, "restarted") {
			t.Errorf("adopted job closed as %+v", j)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("adopted job never closed")
	}
	// r1 also reaps its own child (it is still the parent in this test), without panicking or double-closing.
	waitFor(t, "r1 end", func() bool { j, _ := r1.Get(id); return !j.Running })
}

func TestRunnerMarksVanishedJobInterrupted(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	_ = os.MkdirAll(jobs, 0o750)
	id := "20261115T100000Z-abcdef01"
	js := JobStatus{ID: id, Pipeline: "a", Kind: "verify", Started: time.Now().Add(-time.Minute), Running: true, PID: 999999}
	b, _ := json.Marshal(js)
	_ = os.WriteFile(filepath.Join(jobs, id+".json"), b, 0o600)
	r, err := NewFileJobRunner(jobs, filepath.Join(dir, "strand"), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := r.Get(id)
	if got.Running || !got.Interrupted || got.Ended == nil || got.ExitCode != nil {
		t.Errorf("vanished job: %+v", got)
	}
	// The pipeline is free again, and the correction was persisted.
	var disk JobStatus
	raw, _ := os.ReadFile(filepath.Join(jobs, id+".json"))
	if json.Unmarshal(raw, &disk) != nil || disk.Running || !disk.Interrupted {
		t.Errorf("not persisted: %s", raw)
	}
}

func TestRunnerPrunesOldHistory(t *testing.T) {
	r, _, _ := newRunner(t)
	r.MaxHistory = 3
	var ids []string
	for i := 0; i < 6; i++ {
		id, err := r.Start(nil, JobSpec{Pipeline: "a", Kind: "verify", Args: []string{"x"}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		waitFor(t, "end", func() bool { j, _ := r.Get(id); return !j.Running })
		time.Sleep(1100 * time.Millisecond / 4) // distinct ids are guaranteed by the random suffix; spread Started a little
	}
	if n := len(r.List("a")); n > 4 {
		t.Errorf("history not pruned: %d jobs", n)
	}
	if _, ok := r.Get(ids[0]); ok {
		t.Errorf("oldest job kept")
	}
}

func TestJobOutputMasksCredentialsAcrossPageBoundaries(t *testing.T) {
	const secret = "credential-canary"
	for _, dbOnly := range []bool{false, true} {
		t.Run(fmt.Sprint("db=", dbOnly), func(t *testing.T) {
			r, _, _ := newRunner(t)
			const id = "20261123T000000Z-deadbeef"
			r.jobs[id] = &JobStatus{ID: id}
			prefix := strings.Repeat("x", maxOutputChunk-len(" mysql://user:")+3)
			raw := []byte(prefix + " mysql://user:" + secret + "@host/db\npassword='line one\n" + secret + "'\n")
			if dbOnly {
				r.st = store.NewMem()
				r.persistLocked(r.jobs[id])
				if err := r.st.AppendJobOutput(t.Context(), id, 0, raw); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(r.logPath(id), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			for _, offset := range []int64{0, int64(maxOutputChunk), int64(bytes.Index(raw, []byte(secret))) + 3,
				int64(bytes.LastIndex(raw, []byte(secret)))} {
				out, _ := r.Output(id, offset)
				if strings.Contains(out.Data, "canary") || strings.Contains(out.Data, "credential") || out.Offset <= offset {
					t.Fatalf("offset %d returned a secret fragment or failed to advance", offset)
				}
			}
		})
	}
}

func TestJobSignalRejectsUnrelatedLaunchWithMatchingBinary(t *testing.T) {
	r, bin, dir := newRunner(t)
	hold := filepath.Join(dir, "hold")
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(hold)
	id, err := r.Start(t.Context(), JobSpec{Pipeline: "p", Kind: "verify"})
	if err != nil {
		t.Fatal(err)
	}
	job, _ := r.Get(id)
	if err := signalJobProcess(job.PID, "20261123T000000Z-deadbeef", bin, r.logPath(id), syscall.SIGTERM); !errors.Is(err, errJobNotRunning) {
		t.Fatalf("unrelated process signal = %v", err)
	}
	if !jobProcessMatches(job.PID, id, bin, r.logPath(id)) {
		t.Fatal("job process disappeared after signaling a different launch ID")
	}
	if err := r.Cancel(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "job exit", func() bool { job, _ := r.Get(id); return !job.Running })
}

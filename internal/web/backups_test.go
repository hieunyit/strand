package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func seedBackups(t *testing.T, st store.Store, n int) {
	t.Helper()
	cat, _ := backup.NewCatalog(st)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bkp-%03d", i)
		_, _, err := cat.Create(context.Background(), backup.Record{ID: id, Source: "mysql-prod", Engine: "mysql", Tool: "mysqldump",
			Bucket: "central-backups", ObjectKey: "p/" + id + "/artifact", ManifestURI: "s3://central-backups/p/" + id + "/manifest.json",
			Position: backup.Position{GTIDSet: "u:1-5"}, Status: backup.StatusQueued, CreatedAt: base.Add(time.Duration(i) * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBackupsPageAndAPI(t *testing.T) {
	st := store.NewMem()
	seedBackups(t, st, backupsPageSize+5)
	e := dbEnv(t, st, nil, nil)

	// Unauthenticated access is refused for both the page and the API.
	anon := e.client()
	if resp, _ := e.do(anon, "GET", "/api/v1/backups", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous API = %d", resp.StatusCode)
	}
	if resp, _ := e.do(anon, "GET", "/backups", "", nil); resp.StatusCode == http.StatusOK {
		t.Fatal("anonymous page must not render")
	}

	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/backups", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, "bkp-204") || strings.Contains(body, "bkp-004") {
		t.Fatalf("page 1: %d", resp.StatusCode)
	}
	if strings.Contains(body, " style=") || strings.Contains(body, "<style") || strings.Contains(body, "<script>") || strings.Contains(body, "onclick=") {
		t.Fatal("backups page uses inline style/script, which the CSP blocks")
	}
	if !strings.Contains(body, `aria-current="page"`) || !strings.Contains(body, "<caption") {
		t.Fatal("backups page lacks nav state / table caption")
	}
	i := strings.Index(body, `/backups?cursor=`)
	if i < 0 {
		t.Fatal("full first page must link to older backups")
	}
	link := body[i : i+strings.Index(body[i:], `"`)]
	resp, body = e.do(c, "GET", link, "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, "bkp-000") || strings.Contains(body, "bkp-204") || strings.Contains(body, "?cursor=") {
		t.Fatalf("page 2: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/backups?cursor=zzzz", "", nil); resp.StatusCode != 400 {
		t.Fatalf("bad cursor page = %d", resp.StatusCode)
	}

	// API pagination walks every record once.
	seen := map[string]bool{}
	path := "/api/v1/backups?limit=100"
	for pages := 0; path != "" && pages < 10; pages++ {
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("api %s: %d %s", path, resp.StatusCode, body)
		}
		var out struct {
			Backups []backup.Record `json:"backups"`
			Next    string          `json:"next_cursor"`
		}
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		for _, r := range out.Backups {
			if seen[r.ID] {
				t.Fatalf("duplicate %s across pages", r.ID)
			}
			seen[r.ID] = true
		}
		path = ""
		if out.Next != "" {
			path = "/api/v1/backups?limit=100&cursor=" + out.Next
		}
	}
	if len(seen) != backupsPageSize+5 {
		t.Fatalf("API pages returned %d records, want %d", len(seen), backupsPageSize+5)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/backups?status=bogus", "", nil); resp.StatusCode != 400 {
		t.Fatalf("bad status filter = %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/backups/bkp-001/status", "", nil); resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/backups/missing", "", nil); resp.StatusCode != 404 {
		t.Fatalf("missing = %d", resp.StatusCode)
	}
	// Backups are read-only over the web: no mutating route exists.
	if resp, _ := e.do(c, "DELETE", "/api/v1/backups/bkp-001", "", nil); resp.StatusCode < 400 {
		t.Fatalf("DELETE accepted: %d", resp.StatusCode)
	}
}

func TestBackupsPageRendersManagedJob(t *testing.T) {
	pages, err := parsePages()
	if err != nil {
		t.Fatal(err)
	}
	next := time.Now().UTC().Add(time.Hour)
	data := pageData{
		Title: "Backups",
		Body: backupsPageBody{Jobs: []backupJobView{{
			Job: backup.Job{ID: "job-1", Name: "nightly", Command: "/usr/local/bin/backup",
				EverySeconds: 3600, Enabled: true, NextRunAt: &next},
			Process: &JobStatus{ID: "process-1", Running: false, ExitCode: intPtr(0)},
		}}},
	}
	var rendered bytes.Buffer
	if err := pages["backups"].ExecuteTemplate(&rendered, "base", data); err != nil {
		t.Fatalf("render backup page: %v", err)
	}
	if !bytes.Contains(rendered.Bytes(), []byte("nightly")) || !bytes.Contains(rendered.Bytes(), []byte("next")) {
		t.Fatalf("backup job state not present in rendered page: %s", rendered.String())
	}
}

func intPtr(v int) *int { return &v }

type backupStopTestRunner struct {
	mu   sync.Mutex
	jobs map[string]JobStatus
}

func (r *backupStopTestRunner) Start(context.Context, JobSpec) (string, error) {
	return "", errors.New("unused")
}
func (r *backupStopTestRunner) Get(id string) (JobStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	return j, ok
}
func (r *backupStopTestRunner) List(pipeline string) []JobStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []JobStatus
	for _, j := range r.jobs {
		if j.Pipeline == pipeline {
			out = append(out, j)
		}
	}
	return out
}
func (r *backupStopTestRunner) Cancel(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok || !j.Running {
		return errors.New("not running")
	}
	j.Running = false
	r.jobs[id] = j
	return nil
}
func (*backupStopTestRunner) Output(string, int64) (JobOutput, bool) { return JobOutput{}, false }

func TestStopBackupJobStopsSchedulerAndRunNow(t *testing.T) {
	runner := &backupStopTestRunner{jobs: map[string]JobStatus{
		"schedule": {ID: "schedule", Pipeline: backupJobPipeline("bjob-1"), Kind: "backup-schedule", Running: true},
		"manual":   {ID: "manual", Pipeline: backupJobRunPipeline("bjob-1"), Kind: "backup-run", Running: true},
	}}
	s := &Server{jobs: runner}
	if err := s.stopBackupJob("bjob-1"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"schedule", "manual"} {
		job, ok := runner.Get(id)
		if !ok || job.Running {
			t.Fatalf("process %q was not stopped: %+v", id, job)
		}
	}
}

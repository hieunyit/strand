package web

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
)

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

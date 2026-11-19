package web

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/web/store"
)

func init() { shipEvery = 20 * time.Millisecond }

// DB mode: job output lands in job_output, the spool file is removed when the job ends, the output API
// reads it from the database (masked), and it survives a web restart and a lost state volume.
func TestJobOutputStoredInDatabase(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, body := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("verify: %d %v", code, body)
	}
	var id string
	for _, j := range e.srv.jobs.List("orders") {
		id = j.ID
	}
	spool := filepath.Join(state(e), "jobs", id+".log")
	waitFor(t, "job done and spool shipped", func() bool {
		j, _ := e.srv.jobs.Get(id)
		return !j.Running && !fileExists(spool)
	})
	b, _ := st.ReadJobOutput(context.Background(), id, 0, 1<<20)
	if !strings.Contains(string(b), "line one of verify") || !strings.Contains(string(b), "line two") {
		t.Fatalf("output not in the database: %q", b)
	}
	e2 := e.restart()
	c2, _ := e2.loginOK()
	resp, body := e2.do(c2, "GET", "/api/v1/jobs/"+id+"/output?offset=0", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, "line two") || strings.Contains(body, "LeakyPw") {
		t.Fatalf("output API after restart: %d %s", resp.StatusCode, body)
	}
	// Offset past the end: clamped, empty.
	if o, _ := e2.srv.jobs.Output(id, 1<<30); o.Data != "" || o.Offset != int64(len(b)) {
		t.Fatalf("offset past end: %+v", o)
	}
}

// Output above MaxJobOutputDB keeps the head plus a truncation note.
func TestJobOutputTruncatedInDatabase(t *testing.T) {
	st := store.NewMem()
	dir := t.TempDir()
	r, err := NewJobRunner(dir, "/bin/sh", nil, st)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.Start(context.Background(), JobSpec{Pipeline: "p", Kind: "big",
		Args: []string{"-c", `head -c 11000000 /dev/zero | tr '\0' 'x'; echo; echo tail-marker`}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "big job shipped", func() bool {
		j, _ := r.Get(id)
		return !j.Running && !fileExists(filepath.Join(dir, id+".log"))
	})
	n, _ := st.JobOutputSize(context.Background(), id)
	if n < MaxJobOutputDB || n > MaxJobOutputDB+300 {
		t.Fatalf("stored %d bytes, want the first %d plus a note", n, MaxJobOutputDB)
	}
	tail, _ := st.ReadJobOutput(context.Background(), id, MaxJobOutputDB, 1000)
	if !strings.Contains(string(tail), "output truncated") || strings.Contains(string(tail), "tail-marker") {
		t.Fatalf("tail = %q", tail)
	}
}

// File mode is unchanged: output stays in <id>.log and nothing else is needed.
func TestJobOutputFileModeKeepsFile(t *testing.T) {
	dir := t.TempDir()
	r, err := NewJobRunner(dir, "/bin/sh", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.Start(context.Background(), JobSpec{Pipeline: "p", Kind: "k", Args: []string{"-c", "echo hi"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "job done", func() bool { j, _ := r.Get(id); return !j.Running })
	time.Sleep(5 * shipEvery)
	if b, err := os.ReadFile(filepath.Join(dir, id+".log")); err != nil || string(b) != "hi\n" {
		t.Fatalf("file mode output: %q %v", b, err)
	}
}

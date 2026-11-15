package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/admin"
)

// newFakeStrand ghi một script sh đóng vai binary `strand`: ghi mỗi lần gọi vào calls.log
// (CALL, mỗi đối số một dòng, ENV), in vài dòng output, giữ lại chừng nào file `hold` còn tồn tại, thoát với mã
// trong file `exitcode` (mặc định 0).
func newFakeStrand(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "fakestrand")
	script := `#!/bin/sh
d=$(dirname "$0")
{ echo CALL; for a in "$@"; do echo "$a"; done; echo "ENV=$STRAND_TEST_VAR"; } >> "$d/calls.log"
echo "line one of $1"
echo "dsn mysql://u:LeakyPw@h/db"
while [ -e "$d/hold" ]; do sleep 0.05; done
echo "line two"
c=0
[ -f "$d/exitcode" ] && c=$(cat "$d/exitcode")
exit $c
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

// fakeCalls đọc calls.log: mỗi phần tử là argv (không gồm tên binary) + giá trị ENV của lần gọi.
type fakeCall struct {
	Args []string
	Env  string
	// SecretEnv: "<STRAND_PROMOTE_TARGET_DSN>|<STRAND_PROMOTE_FENCE_DSN>" (chỉ script của wizard ghi).
	SecretEnv string
}

func (e *testEnv) fakeCalls() []fakeCall {
	e.t.Helper()
	b, err := os.ReadFile(filepath.Join(filepath.Dir(e.bin), "calls.log"))
	if err != nil {
		return nil
	}
	var out []fakeCall
	for _, blk := range strings.Split(string(b), "CALL\n")[1:] {
		lines := strings.Split(strings.TrimRight(blk, "\n"), "\n")
		c := fakeCall{}
		for _, l := range lines {
			if strings.HasPrefix(l, "ENV=") {
				c.Env = strings.TrimPrefix(l, "ENV=")
			} else if strings.HasPrefix(l, "SECRETENV=") {
				c.SecretEnv = strings.TrimPrefix(l, "SECRETENV=")
			} else {
				c.Args = append(c.Args, l)
			}
		}
		out = append(out, c)
	}
	return out
}

func (e *testEnv) hold(on bool) {
	p := filepath.Join(filepath.Dir(e.bin), "hold")
	if on {
		_ = os.WriteFile(p, nil, 0o600)
	} else {
		_ = os.Remove(p)
	}
}

func (e *testEnv) setExit(code int) {
	_ = os.WriteFile(filepath.Join(filepath.Dir(e.bin), "exitcode"), []byte(fmt.Sprint(code)), 0o600)
}

// recPipeline: admin socket giả ghi lại mọi lệnh ghi nhận được.
type recPipeline struct {
	*mockPipeline
	mu    sync.Mutex
	calls []string
	fail  string // nếu khác rỗng: các lệnh ghi trả lỗi này
}

func (m *recPipeline) rec(s string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, s)
	if m.fail != "" {
		return fmt.Errorf("%s", m.fail)
	}
	return nil
}
func (m *recPipeline) Calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}
func (m *recPipeline) Pause(ctx context.Context) (*admin.PauseResult, error) {
	if err := m.rec("pause"); err != nil {
		return nil, err
	}
	return &admin.PauseResult{State: "paused", Paused: true}, nil
}
func (m *recPipeline) PauseSnapshot(ctx context.Context) (*admin.PauseResult, error) {
	if err := m.rec("pause_snapshot"); err != nil {
		return nil, err
	}
	return &admin.PauseResult{State: "bootstrapping"}, nil
}
func (m *recPipeline) Resume(ctx context.Context, so bool) (*admin.ResumeResult, error) {
	if err := m.rec(fmt.Sprintf("resume snapshot_only=%v", so)); err != nil {
		return nil, err
	}
	return &admin.ResumeResult{State: "synced", SnapshotOnly: so, Resumed: true}, nil
}
func (m *recPipeline) ThrottleSet(ctx context.Context, r admin.Request) (*admin.ThrottleResult, error) {
	if err := m.rec(fmt.Sprintf("throttle src=%d tgt=%d soft=%v", r.SourceIOPS, r.TargetIOPS, r.Soft)); err != nil {
		return nil, err
	}
	return &admin.ThrottleResult{SourceIOPSSoft: r.SourceIOPS, SourceIOPSHard: 9000}, nil
}
func (m *recPipeline) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	if err := m.rec("rewind " + to); err != nil {
		return nil, err
	}
	return &admin.CheckpointResult{PipelineID: "orders", Epoch: 5, Applied: to, RewoundTo: to}, nil
}
func (m *recPipeline) SnapshotAdd(ctx context.Context, table string) error {
	return m.rec("snapshot_add " + table)
}
func (m *recPipeline) SourceTables(ctx context.Context) ([]admin.SourceTable, error) {
	return []admin.SourceTable{{Table: "app.users", InPipeline: true}, {Table: "app.extra"}, {Table: "app.more"}}, nil
}

// act gửi POST action với session + CSRF.
func (e *testEnv) act(c *http.Client, csrf, pipeline, action string, body map[string]string) (int, map[string]any) {
	e.t.Helper()
	b, _ := json.Marshal(body)
	resp, out := e.do(c, "POST", "/api/v1/pipelines/"+pipeline+"/actions/"+action, string(b),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	var m map[string]any
	_ = json.Unmarshal([]byte(out), &m)
	return resp.StatusCode, m
}

func (e *testEnv) loginOK() (*http.Client, string) {
	e.t.Helper()
	c := e.client()
	resp, csrf := e.login(c, testPassword)
	if resp.StatusCode != 200 || csrf == "" {
		e.t.Fatalf("login failed: %d", resp.StatusCode)
	}
	return c, csrf
}

// auditFor trả các dòng audit của action (theo thứ tự).
func (e *testEnv) auditFor(action string) []map[string]any {
	var out []map[string]any
	for _, l := range e.auditLines() {
		if l["action"] == action {
			out = append(out, l)
		}
	}
	return out
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// opsEnv: pipeline "orders" với env_file; systemd=true thì gắn unit.
func opsEnv(t *testing.T, systemd bool, mutate func(c *Config)) *testEnv {
	t.Helper()
	return newEnv(t, func(e *testEnv, c *Config) {
		envf := filepath.Join(e.dir, "orders.env")
		if err := os.WriteFile(envf, []byte("STRAND_TEST_VAR=from-env-file\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		c.Pipelines[0].EnvFile = envf
		if systemd {
			c.Pipelines[0].SystemdUnit = "strand@orders.service"
		}
		if mutate != nil {
			mutate(c)
		}
	})
}

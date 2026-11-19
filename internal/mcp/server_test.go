package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeService struct {
	block chan struct{}
}

func (f *fakeService) ListBackups(ctx context.Context, filters map[string]any) ([]Record, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s, _ := filters["status"].(string); s == "bogus" {
		return nil, &InvalidArgumentError{Msg: "unsupported status"}
	}
	return []Record{{"id": "b1", "status": "complete"}}, nil
}
func (f *fakeService) GetBackup(_ context.Context, id string) (Record, error) {
	if id == "b1" {
		return Record{"id": "b1"}, nil
	}
	if id == "boom" {
		return nil, errors.New("pq: password authentication failed for user secretuser")
	}
	return nil, ErrNotFound
}
func (f *fakeService) BackupStatus(ctx context.Context, id string) (Record, error) {
	return f.GetBackup(ctx, id)
}

type recAuditor struct {
	mu    sync.Mutex
	calls []string
	fail  bool
}

func (a *recAuditor) Audit(_ context.Context, tool string, _ map[string]any, result string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return errors.New("db down")
	}
	a.calls = append(a.calls, tool+":"+result)
	return nil
}

type client struct {
	t   *testing.T
	in  *io.PipeWriter
	out *bufio.Scanner
}

func start(t *testing.T, svc BackupService, opts ...Option) *client {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = Serve(ctx, inR, outW, svc, opts...); outW.Close(); close(done) }()
	t.Cleanup(func() { inW.Close(); cancel(); <-done })
	sc := bufio.NewScanner(outR)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &client{t: t, in: inW, out: sc}
}

func (c *client) send(msg string) {
	c.t.Helper()
	if _, err := io.WriteString(c.in, msg+"\n"); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) recv() map[string]any {
	c.t.Helper()
	ch := make(chan bool, 1)
	go func() { ch <- c.out.Scan() }()
	select {
	case ok := <-ch:
		if !ok {
			c.t.Fatal("server closed output")
		}
	case <-time.After(5 * time.Second):
		c.t.Fatal("no response")
	}
	var m map[string]any
	if err := json.Unmarshal(c.out.Bytes(), &m); err != nil {
		c.t.Fatalf("bad JSON %q", c.out.Bytes())
	}
	return m
}

func (c *client) init() {
	c.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`)
	r := c.recv()
	res := r["result"].(map[string]any)
	if res["protocolVersion"] != "2025-06-18" {
		c.t.Fatalf("initialize = %v", r)
	}
	c.send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

func toolText(t *testing.T, r map[string]any) (string, bool) {
	t.Helper()
	res, ok := r["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result: %v", r)
	}
	content := res["content"].([]any)[0].(map[string]any)
	return content["text"].(string), res["isError"] == true
}

func TestProtocolInitializeListCall(t *testing.T) {
	a := &recAuditor{}
	c := start(t, &fakeService{}, WithAuditor(a))
	// Calls before initialization are refused.
	c.send(`{"jsonrpc":"2.0","id":0,"method":"tools/list"}`)
	if r := c.recv(); r["error"] == nil {
		t.Fatalf("tools/list before initialize = %v", r)
	}
	c.init()
	c.send(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	r := c.recv()
	tools := r["result"].(map[string]any)["tools"].([]any)
	var names []string
	for _, tl := range tools {
		names = append(names, tl.(map[string]any)["name"].(string))
	}
	if strings.Join(names, ",") != "list_backups,get_backup,backup_status" {
		t.Fatalf("tools = %v (the allowlist must stay read-only)", names)
	}
	c.send(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_backups","arguments":{"limit":5}}}`)
	if txt, isErr := toolText(t, c.recv()); isErr || !strings.Contains(txt, `"b1"`) {
		t.Fatalf("list_backups = %s", txt)
	}
	c.send(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_backup","arguments":{"backup_id":"nope"}}}`)
	if txt, isErr := toolText(t, c.recv()); !isErr || txt != "backup not found" {
		t.Fatalf("missing backup = %q", txt)
	}
	c.send(`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_backups","arguments":{"status":"bogus"}}}`)
	if txt, isErr := toolText(t, c.recv()); !isErr || txt != "unsupported status" {
		t.Fatalf("invalid filter = %q", txt)
	}
	c.send(`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"get_backup","arguments":{"backup_id":"boom"}}}`)
	if txt, isErr := toolText(t, c.recv()); !isErr || strings.Contains(txt, "secretuser") {
		t.Fatalf("internal error leaked: %q", txt)
	}
	c.send(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"restore_backup","arguments":{"backup_id":"b1"}}}`)
	if _, isErr := toolText(t, c.recv()); !isErr {
		t.Fatal("tools outside the allowlist must be refused")
	}
	c.send(`{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"list_backups","arguments":{"bucket":"x"}}}`)
	if _, isErr := toolText(t, c.recv()); !isErr {
		t.Fatal("unknown arguments must be refused")
	}
	a.mu.Lock()
	got := strings.Join(a.calls, ",")
	a.mu.Unlock()
	want := "list_backups:ok,get_backup:denied,list_backups:denied,get_backup:denied,restore_backup:denied,list_backups:denied"
	if got != want {
		t.Fatalf("audit = %s\nwant    %s", got, want)
	}
}

func TestAuditFailureWithholdsResult(t *testing.T) {
	c := start(t, &fakeService{}, WithAuditor(&recAuditor{fail: true}))
	c.init()
	c.send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_backup","arguments":{"backup_id":"b1"}}}`)
	txt, isErr := toolText(t, c.recv())
	if !isErr || strings.Contains(txt, "b1") {
		t.Fatalf("result returned without an audit record: %q", txt)
	}
}

func TestConcurrentCallsBoundedAndCancellable(t *testing.T) {
	svc := &fakeService{block: make(chan struct{})}
	c := start(t, svc)
	c.init()
	for i := 0; i < MaxConcurrentCalls; i++ {
		c.send(`{"jsonrpc":"2.0","id":"c` + string(rune('a'+i)) + `","method":"tools/call","params":{"name":"list_backups","arguments":{}}}`)
	}
	c.send(`{"jsonrpc":"2.0","id":"over","method":"tools/call","params":{"name":"list_backups","arguments":{}}}`)
	r := c.recv()
	if r["id"] != "over" || r["error"] == nil {
		t.Fatalf("request beyond the concurrency bound = %v", r)
	}
	c.send(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"ca"}}`)
	r = c.recv()
	if r["id"] != "ca" || r["error"] == nil {
		t.Fatalf("cancelled request = %v", r)
	}
	close(svc.block)
	for i := 1; i < MaxConcurrentCalls; i++ {
		if r := c.recv(); r["result"] == nil {
			t.Fatalf("blocked call result = %v", r)
		}
	}
}

func TestMalformedInput(t *testing.T) {
	c := start(t, &fakeService{})
	c.send(`{not json`)
	if r := c.recv(); r["error"].(map[string]any)["code"].(float64) != -32700 {
		t.Fatalf("parse error = %v", r)
	}
	c.send(`{"jsonrpc":"1.0","id":9,"method":"ping"}`)
	if r := c.recv(); r["error"].(map[string]any)["code"].(float64) != -32600 {
		t.Fatalf("invalid request = %v", r)
	}
}

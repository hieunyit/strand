package web

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CLI features exposed on the web as jobs: plan, schema diff / approve (dry run and real, which needs the
// pipeline name retyped), schema init preview, database objects. Each runs `strand <args>` with the config
// materialised from the database.
func TestCLIJobsOnTheWeb(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	cfg := e.srv.reg.Get("orders").ConfigPath
	run := func(action string, args map[string]string) (int, map[string]any) {
		t.Helper()
		code, body := e.act(c, csrf, "orders", action, args)
		if code == 200 {
			id, _ := body["job_id"].(string)
			waitFor(t, action+" job", func() bool { j, ok := e.srv.jobs.Get(id); return ok && !j.Running })
		}
		return code, body
	}
	for _, tc := range []struct {
		action string
		args   map[string]string
		want   string
	}{
		{ActPlan, nil, "plan -c " + cfg},
		{ActSchemaDiff, map[string]string{"all": "true"}, "schema diff -c " + cfg + " --all"},
		{ActSchemaApproveDry, map[string]string{"id": "42"}, "schema approve 42 -c " + cfg + " --dry-run"},
		{ActSchemaApprove, map[string]string{"id": "42", "confirm_name": "orders"}, "schema approve 42 -c " + cfg},
		{ActSchemaInitDry, nil, "schema init -c " + cfg + " --dry-run"},
	} {
		if code, body := run(tc.action, tc.args); code != 200 {
			t.Fatalf("%s: %d %v", tc.action, code, body)
		}
		calls := lastCallArgs(t, e)
		if got := strings.Join(calls, " "); got != tc.want {
			t.Errorf("%s ran %q, want %q", tc.action, got, tc.want)
		}
	}
	// Approve without retyping the name, or with a bad id: refused, nothing runs.
	if code, _ := e.act(c, csrf, "orders", ActSchemaApprove, map[string]string{"id": "42"}); code != 400 {
		t.Errorf("approve without confirm_name: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", ActSchemaApproveDry, map[string]string{"id": "1;rm"}); code != 400 {
		t.Errorf("bad id: %d", code)
	}
	// Objects: the test pipeline is mysql -> postgres: list allowed, apply refused (MySQL target only).
	if code, body := run(ActObjectsList, nil); code != 200 {
		t.Errorf("objects-list: %d %v", code, body)
	}
	if code, _ := e.act(c, csrf, "orders", ActObjectsApply, map[string]string{"phase": "pre", "confirm_name": "orders"}); code != 400 {
		t.Errorf("objects-apply on a postgres target: %d", code)
	}
	// The Jobs tab offers the buttons.
	_, page := e.do(c, "GET", "/pipelines/orders/jobs", "", nil)
	for _, want := range []string{`data-do="schema-diff"`, `data-do="schema-approve"`, `data-do="plan"`, `data-do="objects-list"`} {
		if !strings.Contains(page, want) {
			t.Errorf("jobs tab lacks %s", want)
		}
	}
}

// lastCallArgs: argv của lần gọi strand giả gần nhất (calls.log của newFakeStrand: CALL rồi mỗi đối số một dòng).
func lastCallArgs(t *testing.T, e *testEnv) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(filepath.Dir(e.bin), "calls.log"))
	if err != nil {
		t.Fatal(err)
	}
	blocks := strings.Split(string(b), "CALL\n")
	last := blocks[len(blocks)-1]
	var out []string
	for _, l := range strings.Split(strings.TrimRight(last, "\n"), "\n") {
		if strings.Contains(l, "=") && (strings.HasPrefix(l, "ENV=") || strings.HasPrefix(l, "SECRETENV=")) {
			continue
		}
		out = append(out, l)
	}
	return out
}

// Download gives the stored (database) config with secrets masked; MCP page shows connection help and tools.
func TestConfigDownloadAndMCPPage(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/pipelines/orders/config.yaml", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Disposition"), `orders.yaml`) ||
		!strings.Contains(body, "name: orders") || strings.Contains(body, "SuperSecretSrc") || !strings.Contains(body, "********") {
		t.Fatalf("download: %d %q\n%s", resp.StatusCode, resp.Header.Get("Content-Disposition"), body)
	}
	if a := e.auditFor("config.download"); len(a) != 1 {
		t.Errorf("config.download audit: %v", a)
	}
	_, page := e.do(c, "GET", "/pipelines/orders/config", "", nil)
	if !strings.Contains(page, `id="cfg-upload"`) || !strings.Contains(page, "/pipelines/orders/config.yaml") {
		t.Error("config tab lacks download/upload controls")
	}
	resp, page = e.do(c, "GET", "/mcp", "", nil)
	for _, want := range []string{"strand mcp", "list_backups", "get_backup", "backup_status", "mcpServers", "claude mcp add"} {
		if resp.StatusCode != 200 || !strings.Contains(page, want) {
			t.Errorf("MCP page (%d) lacks %q", resp.StatusCode, want)
		}
	}
	resp, body = e.do(c, "GET", "/api/v1/mcp", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"tools"`) {
		t.Fatalf("api mcp: %d %s", resp.StatusCode, body)
	}
}

package web

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Output thật của CLI (lấy từ e2e hot-row / full verify) → bảng cần kiểm lại và lý do.
func TestParseVerifyOutcome(t *testing.T) {
	five, zero := 5, 0
	done := func(args ...string) JobStatus { return JobStatus{Kind: "verify", ExitCode: &five, Args: args} }
	cases := []struct {
		name       string
		job        JobStatus
		out        string
		status     string
		tables     []verifySkipTable
		noMismatch bool
	}{
		{"delta hot row", done("verify", "-c", "x", "--mode", "delta"),
			"Error [verify_incomplete]: changed-key verification; tables skipped by key verification: app.accounts (rows continued changing during verification), app.b (primary key order differs between source and target): [verify_incomplete] delta verify incomplete: 2 tables cannot be verified by key: app.accounts, app.b; 2 changed keys in 2 tables verified, 0 mismatches; run `strand verify --mode sample` or `--mode full` on those tables\n",
			"incomplete", []verifySkipTable{
				{"app.accounts", "rows continued changing during verification"},
				{"app.b", "primary key order differs between source and target"},
			}, true},
		{"full hot keys and keyless", done("verify", "--mode=full"),
			"verify app.t (full, key id): 4 chunks, 0 mismatched chunks, 100 rows compared, 0 mismatches\n" +
				"Error [verify_incomplete]: app.t has 3 unverifiable hot keys; keyless table app.log inconclusive: changing source (5 attempts)\n",
			"incomplete", []verifySkipTable{
				{"app.t", "3 keys kept changing while they were compared"},
				{"app.log", "no key; the source kept changing over 5 count+checksum attempts"},
			}, true},
		{"incomplete with tracking limit", done("verify", "--mode", "delta"),
			"Error [verify_incomplete]: delta verify incomplete: 1 tables have incomplete changed-key tracking (limit, encoding failure or implicit cascade): app.big; 3 changed keys in 1 tables verified, 2 mismatches\n",
			"incomplete", []verifySkipTable{{"app.big", "changed-key tracking is incomplete (limit, encoding failure or implicit cascade)"}}, false},
		{"mismatch", done("verify", "--mode", "full"),
			"Error [verify_mismatch]: 2 mismatched rows in app.orders (pk 1,2)\n", "mismatch", nil, false},
	}
	for _, tc := range cases {
		v := parseVerifyOutcome(tc.job, tc.out)
		if v == nil || v.Status != tc.status || !reflect.DeepEqual(v.Tables, tc.tables) || v.NoMismatch != tc.noMismatch {
			b, _ := json.Marshal(v)
			t.Errorf("%s: got %s", tc.name, b)
		}
	}
	// Không phải verify mã 5: không có thẻ kết quả.
	for _, j := range []JobStatus{
		{Kind: "verify", ExitCode: &zero}, {Kind: "repair", ExitCode: &five}, {Kind: "verify", ExitCode: &five, Running: true},
	} {
		if v := parseVerifyOutcome(j, "Error [verify_incomplete]: x\n"); v != nil {
			t.Errorf("%+v: unexpected outcome %+v", j, v)
		}
	}
	if v := parseVerifyOutcome(done(), "Error [log_gap]: x\n"); v != nil {
		t.Errorf("log_gap parsed as verify outcome: %+v", v)
	}
}

func TestVerifyIncompleteJobPage(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	c, csrf := e.loginOK()
	e.setExit(5)
	e.setOut("Error [verify_incomplete]: changed-key verification; tables skipped by key verification: app.accounts (rows continued changing during verification): [verify_incomplete] delta verify incomplete: 1 tables cannot be verified by key: app.accounts; 4 changed keys in 2 tables verified, 0 mismatches\n")
	code, body := e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "delta"})
	if code != 200 {
		t.Fatalf("start: %d %v", code, body)
	}
	id := body["job_id"].(string)
	waitFor(t, "verify job", func() bool { j, ok := e.srv.jobRunner().Get(id); return ok && !j.Running })

	resp, page := e.do(c, "GET", "/jobs/"+id, "", nil)
	for _, want := range []string{
		`data-verify-status="incomplete"`, "Verify could not finish: no difference found", "incomplete · 0 mismatches",
		"app.accounts", "rows continued changing during verification",
		`name="mode" value="sample"><input type="hidden" name="table" value="app.accounts">`,
		`name="mode" value="full"><input type="hidden" name="table" value="app.accounts">`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("job page (%d) misses %q", resp.StatusCode, want)
		}
	}
	_, api := e.do(c, "GET", "/api/v1/jobs/"+id, "", nil)
	var got struct {
		V *verifyOutcome `json:"verify_outcome"`
	}
	if err := json.Unmarshal([]byte(api), &got); err != nil || got.V == nil || got.V.Status != "incomplete" || !got.V.NoMismatch ||
		len(got.V.Tables) != 1 || got.V.Tables[0].Name != "app.accounts" || got.V.Mode != "delta" {
		t.Fatalf("api verify_outcome = %s (%v)", api, err)
	}

	// Mismatch thật: thẻ đỏ, không có nút verify lại từng bảng.
	e.setOut("Error [verify_mismatch]: 1 mismatched rows in app.orders (pk 7)\n")
	_, body = e.act(c, csrf, "orders", ActVerify, map[string]string{"mode": "full"})
	id2 := body["job_id"].(string)
	waitFor(t, "verify job 2", func() bool { j, ok := e.srv.jobRunner().Get(id2); return ok && !j.Running })
	_, page = e.do(c, "GET", "/jobs/"+id2, "", nil)
	if !strings.Contains(page, `data-verify-status="mismatch"`) || !strings.Contains(page, "Repair wizard") || strings.Contains(page, `name="mode" value="sample"`) {
		t.Errorf("mismatch job page wrong")
	}
}

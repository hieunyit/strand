package web

import (
	"net/http"
	"strings"
	"testing"
)

// TestStandardPageSkeletonAndNoInlineStyles asserts that every registered page adheres
// to the shared skeleton (breadcrumb, pagehead, no inline styles) and tabs markup where relevant.
func TestStandardPageSkeletonAndNoInlineStyles(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c, _ := e.loginOK()

	allPages := []struct {
		path    string
		hasTabs bool
	}{
		{"/", false},
		{"/pipelines", false},
		{"/pipelines/new", false},
		{"/jobs", false},
		{"/backups", true},
		{"/mcp", false},
		{"/settings", true},
		{"/settings/db", true},
		{"/help/schema", false},
		{"/tools/spool-inspect", false},
		{"/archive", false},
		{"/tunnels", false},
		{"/pipelines/orders", true},
		{"/pipelines/orders/tables", true},
		{"/pipelines/orders/checkpoint", true},
		{"/pipelines/orders/errors", true},
		{"/pipelines/orders/config", true},
		{"/pipelines/orders/logs", true},
		{"/pipelines/orders/jobs", true},
		{"/pipelines/orders/schema", true},
		{"/pipelines/orders/objects", true},
		{"/pipelines/orders/repair", true},
		{"/pipelines/orders/promote", true},
		{"/pipelines/orders/seed", true},
	}

	for _, tc := range allPages {
		t.Run(tc.path, func(t *testing.T) {
			resp, body := e.do(c, "GET", tc.path, "", nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s returned HTTP %d: %s", tc.path, resp.StatusCode, body)
			}

			// 1. Shared Skeleton: Breadcrumbs
			if !strings.Contains(body, `class="breadcrumb"`) {
				t.Errorf("%s missing <nav class=\"breadcrumb\">", tc.path)
			}

			// 2. Shared Skeleton: Pagehead with h1
			if !strings.Contains(body, `class="pagehead"`) {
				t.Errorf("%s missing <header class=\"pagehead\">", tc.path)
			}
			if !strings.Contains(body, `<h1>`) || !strings.Contains(body, `</h1>`) {
				t.Errorf("%s missing <h1> inside pagehead", tc.path)
			}

			// 3. Tabs component where relevant
			if tc.hasTabs {
				if !strings.Contains(body, `class="tabs"`) {
					t.Errorf("%s expected to have tabs component", tc.path)
				}
				if !strings.Contains(body, `role="tablist"`) {
					t.Errorf("%s tabs missing role=\"tablist\"", tc.path)
				}
			}

			// 4. Zero inline styles (CSP compliance)
			if strings.Contains(body, ` style="`) || strings.Contains(body, ` style='`) {
				t.Errorf("%s contains inline style attribute (CSP violation)", tc.path)
			}
			if strings.Contains(body, `<style`) {
				t.Errorf("%s contains <style> block (CSP violation)", tc.path)
			}
			if strings.Contains(body, `onclick=`) || strings.Contains(body, `onload=`) {
				t.Errorf("%s contains inline event handlers (CSP violation)", tc.path)
			}
		})
	}
}

func TestSeedWizardLifecycle(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()

	// 1. Begin seed wizard
	code, res := e.act(c, csrf, "orders", "seed-begin", map[string]string{
		"backup_dir":   "/tmp/test-seed",
		"seed_type":    "xtrabackup",
		"confirm_name": "orders",
	})
	if code != 200 {
		t.Fatalf("seed-begin failed: %d %v", code, res)
	}

	// 2. Verify state at /pipelines/orders/seed
	resp, body := e.do(c, "GET", "/pipelines/orders/seed", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET seed wizard returned HTTP %d", resp.StatusCode)
	}
	if !strings.Contains(body, "Step 1: Check backup and dry-run plan") {
		t.Fatalf("expected Step 1 in seed wizard body: %s", body)
	}

	// 3. Run plan
	code, res = e.act(c, csrf, "orders", "seed-plan", nil)
	if code != 200 {
		t.Fatalf("seed-plan failed: %d %v", code, res)
	}

	// 4. Run apply
	code, res = e.act(c, csrf, "orders", "seed-apply", map[string]string{"confirm_name": "orders"})
	if code != 200 {
		t.Fatalf("seed-apply failed: %d %v", code, res)
	}

	// 5. Run start
	code, res = e.act(c, csrf, "orders", "seed-start", nil)
	if code != 200 {
		t.Fatalf("seed-start failed: %d %v", code, res)
	}

	// Verify done
	resp, body = e.do(c, "GET", "/pipelines/orders/seed", "", nil)
	if !strings.Contains(body, "completed successfully") && !strings.Contains(body, "done") {
		t.Fatalf("expected seed wizard completed, got: %s", body)
	}
}

func TestJobDownloadEndpoint(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()

	// Start a verify job to generate output
	code, res := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "sample", "table": "public.orders"})
	if code != 200 {
		t.Fatalf("start verify failed: %d %v", code, res)
	}
	jobID, _ := res["job_id"].(string)
	if jobID == "" {
		t.Fatalf("expected job_id in response: %v", res)
	}

	// Download job output
	resp, _ := e.do(c, "GET", "/api/v1/jobs/"+jobID+"/download", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET job download returned HTTP %d", resp.StatusCode)
	}
}

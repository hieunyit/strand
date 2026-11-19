package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

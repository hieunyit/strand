package web

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// A pipeline with 2,000 tables: the Tables tab (re-fetched every 5 s by the page refresh) must render
// every row in well under a second of server time.
func TestTablesTabTwoThousandTables(t *testing.T) {
	e := newEnv(t, nil)
	var ts []admin.TableInfo
	for i := 0; i < 2000; i++ {
		st := []string{"done", "running", "pending"}[i%3]
		ts = append(ts, admin.TableInfo{Table: fmt.Sprintf("schema_%02d.a_rather_long_table_name_number_%05d", i%40, i), State: st,
			RowsEstimate: int64(i) * 1_000_003, RowsCopied: int64(i) * 500_000, ProgressPct: 50, PK: []string{"id"}})
	}
	e.mock.mockPipeline.tables = ts
	e.srv.samp.SampleOnce(context.Background()) // trang đọc từ bộ nhớ đệm của sampler
	c, _ := e.loginOK()
	var worst time.Duration
	var body string
	for i := 0; i < 3; i++ {
		t0 := time.Now()
		resp, b := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
		if d := time.Since(t0); d > worst {
			worst = d
		}
		if resp.StatusCode != 200 {
			t.Fatalf("tables tab: %d", resp.StatusCode)
		}
		body = b
	}
	if !strings.Contains(body, "a_rather_long_table_name_number_01999") {
		t.Fatal("last table missing from the page")
	}
	t.Logf("tables tab with 2000 tables: worst %v, %d KiB", worst, len(body)/1024)
	if worst > 3*time.Second {
		t.Fatalf("tables tab took %v", worst)
	}
}

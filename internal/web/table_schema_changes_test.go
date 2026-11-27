package web

import (
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// Trang một bảng hiện số thay đổi schema (DDL) Strand đã áp lên bảng ở target.
func TestTableDetailShowsSchemaChanges(t *testing.T) {
	e := newEnv(t, nil)
	at := time.Date(2026, 11, 27, 9, 30, 0, 0, time.UTC)
	e.mock.mockPipeline.tables = []admin.TableInfo{
		{Table: "app.users", State: "done", SchemaChanges: 2, LastSchemaChangeAt: &at},
		{Table: "app.orders", State: "done"},
	}
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	_, body := e.do(c, "GET", "/pipelines/orders/tables/app.users", "", nil)
	if !strings.Contains(body, "data-schema-changes") || !strings.Contains(body, `<span class="c-num">2</span> applied to the target`) {
		t.Fatalf("table detail lacks the schema change count")
	}
	_, body = e.do(c, "GET", "/pipelines/orders/tables/app.orders", "", nil)
	if !strings.Contains(body, "None applied since the pipeline started") {
		t.Fatalf("table without schema changes must say so")
	}
}

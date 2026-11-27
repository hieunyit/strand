package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"strand/internal/admin"
	"strand/internal/metrics"
	"strand/internal/model"
)

// The router counts the rows of a Txn per SOURCE table and operation (the routed rows carry the target
// name), and a committed batch turns them into the per-table activity of the Tables view.
func TestRouteCountsChangesPerSourceTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sim := newCSim(t, 7)
	sim.build(ctx, cancel)
	p := sim.p
	f, err := newTableFilter([]string{"app.items", "app.accounts"}, nil, map[string]string{"app.items": "app.items_v2"})
	if err != nil {
		t.Fatal(err)
	}
	p.filter = f
	ready := p.sched.Ready()
	tx := &model.Txn{SourcePos: model.Pos{LSN: sim.lsn + 0x100}, SourceTS: 1_700_000_000, Rows: []model.RowChange{
		sim.image(cItems, model.OpInsert, 1, 0, false),
		sim.image(cItems, model.OpUpdate, 2, 0, false),
		sim.image(cItems, model.OpUpdate, 3, 0, false),
		sim.image(cItems, model.OpDelete, 4, 0, false),
	}}
	if err := p.route(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var got *model.Txn
	select {
	case got = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("routed Txn not ready")
	}
	if len(got.Rows) != 4 || got.Rows[0].Table.Table != "items_v2" {
		t.Fatalf("routed rows: %+v", got.Rows)
	}
	want := []model.TableOpCount{{Table: cItems, Inserts: 1, Updates: 2, Deletes: 1}}
	if len(got.TableOps.Counts) != 1 || got.TableOps.Counts[0] != want[0] {
		t.Fatalf("TableOps = %+v, want %+v", got.TableOps.Counts, want)
	}

	at := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	p.activity.record([]*model.Txn{got, got}, at, nil, "")
	out := []admin.TableInfo{{Table: cItems.String()}, {Table: cAccounts.String()}}
	p.activity.fill(out, []model.TableID{cItems, cAccounts})
	it := out[0]
	if it.Inserts != 2 || it.Updates != 4 || it.Deletes != 2 || it.Changes() != 8 {
		t.Fatalf("items activity: %+v", it)
	}
	if it.LastChangeAt == nil || !it.LastChangeAt.Equal(at) || it.LastSourceAt == nil || it.LastSourceAt.Unix() != 1_700_000_000 {
		t.Fatalf("items times: last=%v source=%v", it.LastChangeAt, it.LastSourceAt)
	}
	if a := out[1]; a.LastChangeAt != nil || a.Changes() != 0 {
		t.Fatalf("accounts had no change: %+v", a)
	}
}

func TestTableOpsManyTables(t *testing.T) {
	var o model.TableOps
	for round := 0; round < 3; round++ {
		for i := 0; i < 20; i++ {
			o.Add(model.TableID{Schema: "s", Table: string(rune('a' + i))}, model.OpUpdate)
		}
	}
	if len(o.Counts) != 20 {
		t.Fatalf("tables: %d", len(o.Counts))
	}
	for _, c := range o.Counts {
		if c.Updates != 3 || c.Inserts != 0 || c.Deletes != 0 {
			t.Fatalf("%+v", c)
		}
	}
}

// A finished table reports the source statistics as its size (cached, refreshed in the background), so
// the Tables view can filter and sort every table by rows, not only the ones being copied.
func TestAdminTablesDoneTableRowsFromCache(t *testing.T) {
	p := observeTestPipeline(t)
	a := model.TableID{Schema: "app", Table: "a"}
	p.rowCounts.m = map[model.TableID]int64{a: 12345}
	p.rowCounts.at = time.Now()
	tabs, err := (&adminHandler{p: p}).Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, ti := range tabs {
		if ti.Table == "app.a" {
			if ti.State != "done" || ti.RowsEstimate != 12345 {
				t.Fatalf("done table: %+v", ti)
			}
			return
		}
	}
	t.Fatal("app.a missing")
}

// queryRowCounts reads every table of the listed schemas in one query (per engine's statistics).
func TestQueryRowCountsDB(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	for _, engine := range []string{"mysql", "pg"} {
		t.Run(engine, func(t *testing.T) {
			db, _ := startTestDB(t, engine)
			ctx := context.Background()
			stmts := []string{`CREATE SCHEMA s1`, `CREATE TABLE s1.big (id int PRIMARY KEY)`, `CREATE TABLE s1.empty (id int PRIMARY KEY)`,
				`INSERT INTO s1.big SELECT g FROM generate_series(1, 5000) g`, `ANALYZE s1.big`, `ANALYZE s1.empty`}
			eng := model.EnginePG
			if engine == "mysql" {
				eng = model.EngineMySQL
				stmts = []string{`CREATE DATABASE s1`, `CREATE TABLE s1.big (id int PRIMARY KEY)`, `CREATE TABLE s1.empty (id int PRIMARY KEY)`,
					`SET SESSION cte_max_recursion_depth = 10000; INSERT INTO s1.big WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 5000) SELECT n FROM g`,
					`ANALYZE TABLE s1.big, s1.empty`}
			}
			for _, q := range stmts {
				if _, err := db.ExecContext(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			p := &Pipeline{src: db, srcEngine: eng}
			big, empty := model.TableID{Schema: "s1", Table: "big"}, model.TableID{Schema: "s1", Table: "empty"}
			missing := model.TableID{Schema: "s2", Table: "x"}
			m, err := p.queryRowCounts(ctx, []model.TableID{big, empty, missing})
			if err != nil {
				t.Fatal(err)
			}
			// Statistics are estimates: InnoDB samples pages, PostgreSQL's reltuples is exact after ANALYZE here.
			if n := m[big]; n < 2500 || n > 10000 {
				t.Fatalf("big: %d (%v)", n, m)
			}
			if n, ok := m[empty]; !ok || n != 0 {
				t.Fatalf("empty: %d %v (%v)", n, ok, m)
			}
			if _, ok := m[missing]; ok {
				t.Fatalf("missing table reported: %v", m)
			}
		})
	}
}

// Per-table Prometheus series (như table statistics của AWS DMS) và số thay đổi schema theo bảng.
func TestTableActivityMetricsAndSchemaChanges(t *testing.T) {
	m := metrics.NewMetrics(prometheus.NewRegistry())
	var a tableActivity
	items, acc := model.TableID{Schema: "app", Table: "items"}, model.TableID{Schema: "app", Table: "accounts"}
	tx := &model.Txn{SourceTS: 1_700_000_000}
	tx.TableOps.Add(items, model.OpInsert)
	tx.TableOps.Add(items, model.OpUpdate)
	tx.TableOps.Add(items, model.OpUpdate)
	tx.TableOps.Add(acc, model.OpDelete)
	at := time.Unix(1_700_000_003, 0)
	a.record([]*model.Txn{tx, tx}, at, m, "p")

	for _, c := range []struct {
		table, op string
		want      float64
	}{{"app.items", "insert", 2}, {"app.items", "update", 4}, {"app.accounts", "delete", 2}} {
		if got := metricValue(t, m.TableRowsAppliedTotal.WithLabelValues("p", c.table, c.op)); got != c.want {
			t.Errorf("%s %s = %v, want %v", c.table, c.op, got, c.want)
		}
	}
	if got := metricValue(t, m.TableApplyDelay.WithLabelValues("p", "app.items")); got != 3 {
		t.Errorf("apply delay = %v, want 3", got)
	}
	if got := metricValue(t, m.TableLastChange.WithLabelValues("p", "app.accounts")); got != float64(at.Unix()) {
		t.Errorf("last change = %v", got)
	}

	ddlAt := at.Add(time.Minute)
	a.recordDDL([]model.TableID{items}, ddlAt)
	a.recordDDL([]model.TableID{items, {Schema: "app", Table: "quiet"}}, ddlAt)
	out := []admin.TableInfo{{Table: "app.items"}, {Table: "app.quiet"}}
	a.fill(out, []model.TableID{items, {Schema: "app", Table: "quiet"}})
	if out[0].SchemaChanges != 2 || out[0].LastSchemaChangeAt == nil || !out[0].LastSchemaChangeAt.Equal(ddlAt) || out[0].Changes() != 6 {
		t.Fatalf("items: %+v", out[0])
	}
	// Bảng chỉ có DDL, chưa có thay đổi row: không báo thời điểm thay đổi row.
	if out[1].SchemaChanges != 1 || out[1].LastChangeAt != nil || out[1].Changes() != 0 {
		t.Fatalf("quiet: %+v", out[1])
	}
}

// metricValue reads a counter or gauge value.
func metricValue(t *testing.T, c prometheus.Metric) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return m.Gauge.GetValue()
}

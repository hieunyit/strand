package pipeline

import (
	"context"
	"sync"
	"testing"

	"strand/internal/model"
)

// The admin socket answers Tables while auto-seed copies (before ready()),
// concurrently with the copy updating its progress and replacing p.tables.
func TestAdminTablesDuringAutoSeed(t *testing.T) {
	state, err := openLocalState(t.TempDir(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetDone("app.logical_done", true); err != nil {
		t.Fatal(err)
	}
	meta := func(table string) *tableMeta {
		return &tableMeta{ID: model.TableID{Schema: "app", Table: table}, PK: []string{"id"}, PKNumeric: true, KeyName: "PRIMARY"}
	}
	tables := func() map[model.TableID]*tableMeta {
		out := map[model.TableID]*tableMeta{}
		for _, name := range []string{"a_first", "b_second", "c_third", "logical_done", "logical_pending"} {
			m := meta(name)
			out[m.ID] = m
		}
		return out
	}
	p := &Pipeline{state: state, tables: tables()}
	p.autoSeeding.Store(true)
	p.autoSeedGroup = []string{"app.a_first", "app.b_second", "app.c_third"}
	p.autoSeedCopied = map[string]int64{"app.a_first": 7, "app.b_second": 3}
	p.autoSeedDone.Store(1)
	h := &adminHandler{p: p}

	got, err := h.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"app.a_first": "done", "app.b_second": "running", "app.c_third": "pending",
		"app.logical_done": "done", "app.logical_pending": "pending"}
	if len(got) != len(want) {
		t.Fatalf("tables: %+v", got)
	}
	for _, ti := range got {
		if ti.State != want[ti.Table] {
			t.Errorf("%s: state %s, want %s", ti.Table, ti.State, want[ti.Table])
		}
	}
	if got[0].RowsCopied != 7 || got[1].RowsCopied != 3 {
		t.Errorf("rows copied: %+v", got[:2])
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := int64(0); i < 200; i++ {
			p.autoSeedMu.Lock()
			p.autoSeedCopied["app.b_second"] = i
			p.autoSeedMu.Unlock()
			p.tablesMu.Lock()
			p.tables = tables()
			p.tablesMu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := h.Tables(context.Background()); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()

	p.autoSeeding.Store(false)
	if _, err := h.Tables(context.Background()); err != errNotReady {
		t.Fatalf("outside auto-seed before ready: %v, want %v", err, errNotReady)
	}
}

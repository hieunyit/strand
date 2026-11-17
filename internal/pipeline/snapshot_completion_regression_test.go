package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/sched"
	"strand/internal/snapshot"
)

type completionDB struct {
	applied    string
	failUpdate bool
	writes     int
}
type completionConnector struct{ db *completionDB }

func (c completionConnector) Connect(context.Context) (driver.Conn, error) {
	return completionConn{c.db}, nil
}
func (c completionConnector) Driver() driver.Driver { return completionDriver{} }

type completionDriver struct{}

func (completionDriver) Open(string) (driver.Conn, error) { return nil, errors.New("connector only") }

type completionConn struct{ db *completionDB }

func (completionConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (completionConn) Close() error                        { return nil }
func (completionConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

type completionRows struct {
	cols []string
	data [][]driver.Value
}

func (r *completionRows) Columns() []string { return r.cols }
func (r *completionRows) Close() error      { return nil }
func (r *completionRows) Next(v []driver.Value) error {
	if len(r.data) == 0 {
		return io.EOF
	}
	copy(v, r.data[0])
	r.data = r.data[1:]
	return nil
}
func (c completionConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.HasPrefix(q, "SELECT worker_id, epoch, batch_no, applied"):
		return &completionRows{cols: []string{"worker_id", "epoch", "batch_no", "applied"}}, nil
	case strings.HasPrefix(q, "SELECT applied FROM _strand_checkpoint"):
		return &completionRows{cols: []string{"applied"}, data: [][]driver.Value{{c.db.applied}}}, nil
	default:
		return nil, fmt.Errorf("unexpected query %s", q)
	}
}
func (c completionConn) ExecContext(_ context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(q, "UPDATE _strand_checkpoint SET applied") {
		return nil, fmt.Errorf("unexpected exec %s", q)
	}
	if c.db.failUpdate {
		return nil, errors.New("checkpoint unavailable")
	}
	c.db.applied = a[0].Value.(string)
	c.db.writes++
	return driver.RowsAffected(1), nil
}
func TestPGSnapshotCompletionDurableBeforeDone(t *testing.T) {
	for _, tc := range []struct {
		name               string
		seen               uint64
		fail               bool
		reread             bool
		wantRetry, wantErr bool
	}{
		{"durable", 0x200, false, false, false, false},
		{"checkpoint update fails", 0x200, true, false, false, true},
		{"checkpoint below watermark", 0x100, false, false, false, true},
		{"new reread", 0x200, false, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := openLocalState(t.TempDir(), "p")
			if err != nil {
				t.Fatal(err)
			}
			table := model.TableID{Schema: "app", Table: "t"}
			plan := snapshot.SnapshotPlan{Empty: true, BoundSnapshot: &snapshot.PGSnapshot{Xmin: 100, Xmax: 100, BoundsLSN: 0x100}}
			if err := st.SetPlan(table.String(), plan); err != nil {
				t.Fatal(err)
			}
			raw := &completionDB{applied: checkpoint.NewPGAppliedSet(0x10).String(), failUpdate: tc.fail}
			db := sql.OpenDB(completionConnector{raw})
			defer db.Close()
			p := &Pipeline{id: "p", epoch: 1, dst: db, dstEngine: model.EnginePG, srcEngine: model.EnginePG, state: st, sched: sched.NewScheduler(sched.Config{}), lsns: &lsnTracker{}}
			p.snap = snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
			p.snap.Manager.RegisterTable(table, "id", nil, 0)
			if tc.reread {
				p.snap.Manager.AddReread(table, []int64{2}, time.Now())
			}
			p.lsns.routed(tc.seen, 0)
			p.comp = checkpoint.NewCompactor(db, nil, "p", model.EnginePG, model.EnginePG, checkpoint.DefaultCompactorConfig("p", 1))
			p.comp.SetVouch(p.vouchGlobal)
			retry, err := p.finishPGSnapshot(ctx, table, 0x200)
			if retry != tc.wantRetry || (err != nil) != tc.wantErr {
				t.Fatalf("retry=%v err=%v", retry, err)
			}
			done := st.IsDone(table.String())
			_, hasPlan := st.Plan(table.String())
			registered := p.snap.Manager.IsRegistered(table)
			if tc.wantErr || tc.wantRetry {
				if done || !hasPlan || !registered {
					t.Fatalf("failure lost guard: done=%v plan=%v registered=%v", done, hasPlan, registered)
				}
				return
			}
			set, err := checkpoint.ParseAppliedSet(model.EnginePG, raw.applied)
			if err != nil {
				t.Fatal(err)
			}
			if !done || hasPlan || registered || set.Lwd().LSN < 0x200 || raw.writes != 1 {
				t.Fatalf("done=%v plan=%v registered=%v durable=%s writes=%d", done, hasPlan, registered, raw.applied, raw.writes)
			}
		})
	}
}
func TestPGPreRegistrationRetainsBoundsGuard(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "t"}
	pos := model.Pos{Engine: model.EnginePG, LSN: 0x80}
	for _, tc := range []struct {
		name string
		plan snapshot.SnapshotPlan
		key  int64
		old  []model.Value
		xid  uint64
		lsn  uint64
		want int
	}{
		{"empty historical", snapshot.SnapshotPlan{Empty: true}, 1, nil, 50, 0x80, 0},
		{"empty later xid", snapshot.SnapshotPlan{Empty: true}, 1, nil, 150, 0x80, 1},
		{"empty later lsn", snapshot.SnapshotPlan{Empty: true}, 1, nil, 50, 0x200, 1},
		{"finished outside", snapshot.SnapshotPlan{MinPK: 10, MaxPK: 20, Finished: true}, 1, nil, 50, 0x80, 0},
		{"finished inside", snapshot.SnapshotPlan{MinPK: 10, MaxPK: 20, Finished: true}, 15, nil, 50, 0x80, 1},
		{"finished old and new outside", snapshot.SnapshotPlan{MinPK: 10, MaxPK: 20, Finished: true}, 1, []model.Value{int64(2)}, 50, 0x80, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := openLocalState(t.TempDir(), "p")
			if err != nil {
				t.Fatal(err)
			}
			tc.plan.BoundSnapshot = &snapshot.PGSnapshot{Xmin: 100, Xmax: 100, BoundsLSN: 0x100}
			if err = st.SetPlan(table.String(), tc.plan); err != nil {
				t.Fatal(err)
			}
			p := &Pipeline{state: st, snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
			p.snap.LoadPlan = func(t model.TableID) (snapshot.SnapshotPlan, bool) { return st.Plan(t.String()) }
			p.snap.SavePlan = func(t model.TableID, pl snapshot.SnapshotPlan) error { return st.SetPlan(t.String(), pl) }
			row := &model.RowChange{Table: table, Op: model.OpUpdate, Key: []model.Value{tc.key}, OldKey: tc.old, Cols: []model.ColValue{{Name: "doc", Unchanged: true}}}
			pos.LSN = tc.lsn
			parts, err := p.keepBeforeRegisterXID(row, pos, tc.xid)
			if err != nil || len(parts) != tc.want {
				t.Fatalf("parts=%v err=%v", parts, err)
			}
			if len(tc.old) > 0 && len(parts) > 0 && parts[0].Op != model.OpDelete {
				t.Fatalf("historical move should only delete old key: %v", parts)
			}
		})
	}
}

func TestPGPreRegistrationCompositeBounds(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "composite"}
	lo, hi := "b", "m"
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	plan := snapshot.SnapshotPlan{GenericKey: true, Finished: true, MinKey: []snapshot.KeyVal{{S: &lo}}, MaxKey: []snapshot.KeyVal{{S: &hi}}, BoundSnapshot: &snapshot.PGSnapshot{Xmin: 100, Xmax: 100, BoundsLSN: 0x100}}
	if err = st.SetPlan(table.String(), plan); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{state: st, snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions()), tables: map[model.TableID]*tableMeta{table: {PKCodec: &snapshot.PKCodec{Cols: []snapshot.PKColumn{{Name: "id", Kind: snapshot.PKBytes}}}}}}
	for _, tc := range []struct {
		key  string
		want int
	}{{"a", 0}, {"z", 0}, {"f", 1}} {
		rc := &model.RowChange{Table: table, Op: model.OpUpdate, Key: []model.Value{tc.key}, Cols: []model.ColValue{{Name: "doc", Unchanged: true}}}
		got, err := p.keepBeforeRegisterXID(rc, model.Pos{Engine: model.EnginePG, LSN: 0x80}, 50)
		if err != nil || len(got) != tc.want {
			t.Fatalf("key %q: parts=%v err=%v", tc.key, got, err)
		}
	}
}

func TestPGSnapshotCompletionWaitsForAdmittedWork(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "t"}
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPlan(table.String(), snapshot.SnapshotPlan{Empty: true}); err != nil {
		t.Fatal(err)
	}
	raw := &completionDB{applied: checkpoint.NewPGAppliedSet(0x10).String()}
	db := sql.OpenDB(completionConnector{raw})
	defer db.Close()
	p := &Pipeline{id: "p", epoch: 1, dst: db, dstEngine: model.EnginePG, srcEngine: model.EnginePG, state: st, sched: sched.NewScheduler(sched.Config{}), lsns: &lsnTracker{}}
	p.snap = snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
	p.snap.Manager.RegisterTable(table, "id", nil, 0)
	txn := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 0x200}}
	if err = p.sched.Admit(context.Background(), txn); err != nil {
		t.Fatal(err)
	}
	p.admitted.Store(txn.Seq())
	p.lsns.routed(0x200, txn.Seq())
	p.comp = checkpoint.NewCompactor(db, nil, "p", model.EnginePG, model.EnginePG, checkpoint.DefaultCompactorConfig("p", 1))
	p.comp.SetVouch(p.vouchGlobal)
	done := make(chan error, 1)
	go func() { _, err := p.finishPGSnapshot(context.Background(), table, 0x200); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("completed before admitted work: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if st.IsDone(table.String()) || !p.snap.Manager.IsRegistered(table) {
		t.Fatal("guard removed before drain")
	}
	p.sched.Done(txn.Seq(), nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("completion did not finish after drain")
	}
	if !st.IsDone(table.String()) {
		t.Fatal("snapshot not marked done")
	}
}

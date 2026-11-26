package snapshot

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// tableBoundsDriver answers the bounds read with a snapshot that depends on the table, so a table that
// takes another table's snapshot is visible: app.a sees xid < 100, app.b sees xid < 300.
type tableBoundsDriver struct{}
type tableBoundsConn struct{}

func (tableBoundsDriver) Open(string) (driver.Conn, error) { return tableBoundsConn{}, nil }
func (tableBoundsConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepare")
}
func (tableBoundsConn) Close() error              { return nil }
func (tableBoundsConn) Begin() (driver.Tx, error) { return boundsTx{}, nil }
func (tableBoundsConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(q, "MIN(") {
		return boundsConn{}.QueryContext(context.Background(), q, nil) // the other queries of a registration
	}
	snap := "100:100:"
	if strings.Contains(q, `"b"`) {
		snap = "300:300:"
	}
	return &boundsRows{[]string{"min", "max", "snapshot", "lsn"}, [][]driver.Value{{int64(2), int64(100), snap, "0/100"}}}, nil
}

func init() { sql.Register("snapshot-table-bounds-test", tableBoundsDriver{}) }

// The bounds snapshot is kept per table. Registrations of several tables can run concurrently in one
// barrier group (a `snapshot add` during the bootstrap); with one shared "latest" snapshot a table could
// register with the snapshot of another table's bounds read. Too old: an UPDATE with unchanged TOAST of a Txn the table's own bounds
// already contained is applied to a row ClearTarget removed (row missing, exit 5). Too new: such an UPDATE
// of a row inserted after the table's bounds read is discarded (silent divergence).
func TestPGBoundSnapshotPerTable(t *testing.T) {
	db, err := sql.Open("snapshot-table-bounds-test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	a, b := model.TableID{Schema: "app", Table: "a"}, model.TableID{Schema: "app", Table: "b"}

	// Bounds of a, then of b (another table's registration), then a reads its snapshot.
	source := &PGChunkSource{DB: db}
	if _, err := source.Bounds(ctx, a, "id"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Bounds(ctx, b, "id"); err != nil {
		t.Fatal(err)
	}
	if s := source.BoundSnapshotOf(a); s == nil || !s.Visible(50) || s.Visible(150) {
		t.Fatalf("app.a took another table's bounds snapshot: %+v", s)
	}
	if s := source.BoundSnapshotOf(b); s == nil || !s.Visible(250) {
		t.Fatalf("app.b snapshot: %+v", s)
	}

	// Through SnapshotAdd: both registrations inside ONE barrier, concurrently, many times.
	for i := 0; i < 50; i++ {
		source := &PGChunkSource{DB: db}
		s := NewMySQLSnapshotter(DefaultChunkOptions())
		s.Chunks = source
		s.ClearTarget = func(context.Context, model.TableID) error { return nil }
		s.ClearUnread = func(context.Context, model.TableID, string, int64, int64, bool, []int64) error { return nil }
		stop := fmt.Errorf("stop after registration")
		var started sync.WaitGroup
		started.Add(2)
		s.Barrier = func(_ context.Context, register func() error) error {
			started.Done()
			started.Wait() // both tables register at the same time
			if err := register(); err != nil {
				return err
			}
			return stop
		}
		var wg sync.WaitGroup
		for _, tb := range []model.TableID{a, b} {
			wg.Add(1)
			go func(tb model.TableID) {
				defer wg.Done()
				if err := s.SnapshotAdd(ctx, nil, nil, tb, "id", []string{"id"}, func(context.Context, checkpoint.AppliedSet) error { return nil }); !errors.Is(err, stop) {
					t.Errorf("%s: %v", tb, err)
				}
			}(tb)
		}
		wg.Wait()
		ta, tbr := s.Manager.tables[a.String()], s.Manager.tables[b.String()]
		if ta == nil || tbr == nil || ta.BoundSnap == nil || tbr.BoundSnap == nil {
			t.Fatalf("run %d: tables not registered", i)
		}
		if ta.BoundSnap.Visible(150) || !tbr.BoundSnap.Visible(250) {
			t.Fatalf("run %d: registered with another table's bounds snapshot: a=%+v b=%+v", i, ta.BoundSnap, tbr.BoundSnap)
		}
	}
}

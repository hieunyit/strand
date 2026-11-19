package snapshot

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strand/internal/checkpoint"
	"strand/internal/model"
	"strings"
	"testing"
	"time"
)

type boundsDriver struct{}
type boundsConn struct{}
type boundsTx struct{}
type boundsRows struct {
	cols   []string
	values [][]driver.Value
}

func (boundsDriver) Open(string) (driver.Conn, error)  { return boundsConn{}, nil }
func (boundsConn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("unexpected prepare") }
func (boundsConn) Close() error                        { return nil }
func (boundsConn) Begin() (driver.Tx, error)           { return boundsTx{}, nil }
func (boundsConn) BeginTx(_ context.Context, o driver.TxOptions) (driver.Tx, error) {
	if o.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) || !o.ReadOnly {
		return nil, fmt.Errorf("unsafe bounds transaction: %+v", o)
	}
	return boundsTx{}, nil
}
func (boundsTx) Commit() error   { return nil }
func (boundsTx) Rollback() error { return nil }
func (boundsConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "pg_attribute"):
		return &boundsRows{[]string{"attname", "oid"}, [][]driver.Value{{"id", int64(20)}}}, nil
	case strings.Contains(q, "MIN("):
		if !strings.Contains(q, "pg_current_snapshot()") {
			return nil, fmt.Errorf("bounds lack statement snapshot")
		}
		return &boundsRows{[]string{"min", "max", "snapshot", "lsn"}, [][]driver.Value{{int64(2), int64(100), "100:100:", "0/100"}}}, nil
	case q == "SELECT pg_current_snapshot()::text, pg_current_wal_insert_lsn()::text":
		return &boundsRows{[]string{"snapshot", "lsn"}, [][]driver.Value{{"100:100:", "0/100"}}}, nil
	case q == "SELECT pg_current_snapshot()::text":
		return &boundsRows{[]string{"snapshot"}, [][]driver.Value{{"100:100:"}}}, nil
	case strings.Contains(q, "ORDER BY"):
		v := "2"
		if strings.Contains(q, "DESC") {
			v = "100"
		}
		return &boundsRows{[]string{"id"}, [][]driver.Value{{v}}}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", q)
}
func (r *boundsRows) Columns() []string { return r.cols }
func (r *boundsRows) Close() error      { return nil }
func (r *boundsRows) Next(v []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(v, r.values[0])
	r.values = r.values[1:]
	return nil
}
func init() { sql.Register("snapshot-bounds-test", boundsDriver{}) }
func TestPGBoundsSingleConnection(t *testing.T) {
	db, err := sql.Open("snapshot-bounds-test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	source := &PGChunkSource{DB: db}
	table := model.TableID{Schema: "app", Table: "t"}
	lo, hi, ok, err := source.KeyBounds(ctx, table, &PKCodec{Cols: []PKColumn{{Name: "id", Kind: PKInt}}})
	if err != nil || !ok || lo.V[0] != int64(2) || hi.V[0] != int64(100) {
		t.Fatalf("key bounds: %v %v %v %v", lo, hi, ok, err)
	}
	if s := source.BoundSnapshot(); s == nil || !s.Visible(50) || s.Visible(150) || s.BoundsLSN != 0x100 {
		t.Fatalf("snapshot: %+v", s)
	}
	b, err := source.Bounds(ctx, table, "id")
	if err != nil || b.MinPK != 2 || b.MaxPK != 100 {
		t.Fatalf("integer bounds: %+v %v", b, err)
	}
}

func TestPGBoundSnapshotPlanRegistrationResume(t *testing.T) {
	db, err := sql.Open("snapshot-bounds-test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	source := &PGChunkSource{DB: db}
	table := model.TableID{Schema: "app", Table: "t"}
	stopped := fmt.Errorf("stop after registration")
	var saved SnapshotPlan
	for _, resume := range []bool{false, true} {
		s := NewMySQLSnapshotter(DefaultChunkOptions())
		s.Chunks = source
		s.ClearTarget = func(context.Context, model.TableID) error { return nil }
		s.ClearUnread = func(context.Context, model.TableID, string, int64, int64, bool, []int64) error { return nil }
		s.LoadPlan = func(model.TableID) (SnapshotPlan, bool) { return saved, resume }
		s.SavePlan = func(_ model.TableID, p SnapshotPlan) error { saved = p; return nil }
		s.Barrier = func(_ context.Context, register func() error) error {
			if err := register(); err != nil {
				return err
			}
			tr := s.Manager.tables[table.String()]
			if tr.BoundSnap == nil || !tr.BoundSnap.Visible(50) || tr.BoundSnap.Visible(150) {
				t.Fatalf("resume=%v lost original visibility: %+v", resume, tr.BoundSnap)
			}
			return stopped
		}
		if resume {
			// A newer source marker must not replace the persisted original marker.
			source.boundSnap = &PGSnapshot{Xmin: 200, Xmax: 200}
		}
		err := s.SnapshotAdd(context.Background(), nil, nil, table, "id", []string{"id"}, func(context.Context, checkpoint.AppliedSet) error { return nil })
		if !errors.Is(err, stopped) {
			t.Fatalf("resume=%v: %v", resume, err)
		}
		if saved.BoundSnapshot == nil || saved.BoundSnapshot.Visible(150) {
			t.Fatalf("original bounds snapshot missing/replaced: %+v", saved.BoundSnapshot)
		}
	}
}

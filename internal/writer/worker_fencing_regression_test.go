package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

type fencingState struct {
	epoch, workerEpoch           int64
	pipeline, worker, checkpoint bool
	inserts, commits, rollbacks  int
	shared                       bool
}
type fencingConnector struct{ s *fencingState }

func (c fencingConnector) Connect(context.Context) (driver.Conn, error) {
	return &fencingConn{s: c.s}, nil
}
func (c fencingConnector) Driver() driver.Driver { return fencingDriver{} }

type fencingDriver struct{}

func (fencingDriver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("use connector") }

type fencingConn struct {
	s    *fencingState
	inTx bool
}

func (c *fencingConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepare")
}
func (c *fencingConn) Close() error              { return nil }
func (c *fencingConn) Begin() (driver.Tx, error) { c.inTx = true; return c, nil }
func (c *fencingConn) Commit() error             { c.s.commits++; c.inTx = false; c.s.shared = false; return nil }
func (c *fencingConn) Rollback() error {
	c.s.rollbacks++
	c.inTx = false
	c.s.shared = false
	return nil
}
func (c *fencingConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(strings.TrimSpace(q), "INSERT INTO _strand_worker_delta") || !c.inTx || !c.s.shared {
		return nil, fmt.Errorf("worker insert without transaction and pipeline lock: %s", q)
	}
	c.s.inserts++
	epoch := args[2].Value.(int64)
	if !c.s.worker || c.s.workerEpoch < epoch {
		c.s.workerEpoch = epoch
	}
	c.s.worker = true
	return driver.RowsAffected(1), nil
}

type fencingRows struct {
	cols   []string
	values [][]driver.Value
}

func (r *fencingRows) Columns() []string { return r.cols }
func (r *fencingRows) Close() error      { return nil }
func (r *fencingRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}
func (c *fencingConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	one := func(col string, v driver.Value, exists bool) driver.Rows {
		r := &fencingRows{cols: []string{col}}
		if exists {
			r.values = [][]driver.Value{{v}}
		}
		return r
	}
	switch {
	case strings.HasPrefix(q, "SELECT epoch FROM _strand_pipeline"):
		if !c.inTx || !strings.Contains(q, "FOR SHARE") {
			return nil, fmt.Errorf("pipeline read lacks shared transaction lock")
		}
		c.s.shared = true
		return one("epoch", c.s.epoch, c.s.pipeline), nil
	case strings.HasPrefix(q, "SELECT epoch FROM _strand_worker_delta"):
		if !c.inTx || !c.s.shared {
			return nil, fmt.Errorf("worker read outside lock")
		}
		return one("epoch", c.s.workerEpoch, c.s.worker), nil
	case strings.HasPrefix(q, "SELECT worker_id, epoch, batch_no, applied"):
		return &fencingRows{cols: []string{"worker_id", "epoch", "batch_no", "applied"}}, nil
	case strings.HasPrefix(q, "SELECT applied FROM _strand_checkpoint"):
		return one("applied", "", c.s.checkpoint && args[1].Value.(int64) == c.s.epoch), nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", q)
	}
}
func TestWorkerCreationFencedByPipelineEpoch(t *testing.T) {
	for _, pg := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			state      fencingState
			wantFenced bool
			wantInsert int
		}{
			{"stale missing worker", fencingState{pipeline: true, epoch: 2}, true, 0},
			{"missing pipeline", fencingState{}, true, 0},
			{"current missing worker", fencingState{pipeline: true, epoch: 1}, false, 1},
			{"current existing worker", fencingState{pipeline: true, epoch: 1, worker: true, workerEpoch: 1}, false, 1},
			{"newer worker", fencingState{pipeline: true, epoch: 1, worker: true, workerEpoch: 2}, true, 1},
		} {
			t.Run(fmt.Sprintf("pg=%t/%s", pg, tc.name), func(t *testing.T) {
				db := sql.OpenDB(fencingConnector{&tc.state})
				defer db.Close()
				var err error
				if pg {
					err = NewPGWriter(db, "p", 7, 1).EnsureWorkerRow(context.Background())
				} else {
					err = NewMySQLWriter(db, "p", 7, 1).EnsureWorkerRow(context.Background())
				}
				if (exitcode.GetExitCode(err) == exitcode.Fenced) != tc.wantFenced || (!tc.wantFenced && err != nil) {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.state.inserts != tc.wantInsert {
					t.Fatalf("inserts = %d", tc.state.inserts)
				}
				if !tc.wantFenced && tc.state.commits != 1 {
					t.Fatal("valid worker creation did not commit")
				}
				if tc.wantFenced && tc.state.rollbacks != 1 {
					t.Fatal("fenced transaction did not roll back")
				}
			})
		}
	}
}
func TestCompactorNoWorkChecksEpoch(t *testing.T) {
	for _, engine := range []model.Engine{model.EngineMySQL, model.EnginePG} {
		for _, tc := range []struct {
			name   string
			state  fencingState
			fenced bool
			vouch  bool
		}{
			{"stale", fencingState{epoch: 2, checkpoint: true}, true, false}, {"missing checkpoint", fencingState{epoch: 1}, true, false}, {"current", fencingState{epoch: 1, checkpoint: true}, false, false}, {"stale with vouch", fencingState{epoch: 2, checkpoint: true}, true, true}, {"current with vouch", fencingState{epoch: 1, checkpoint: true}, false, true},
		} {
			t.Run(fmt.Sprintf("%s/%s", engine, tc.name), func(t *testing.T) {
				db := sql.OpenDB(fencingConnector{&tc.state})
				defer db.Close()
				c := checkpoint.NewCompactor(db, nil, "p", engine, engine, checkpoint.DefaultCompactorConfig("p", 1))
				if tc.vouch {
					c.SetVouch(func(checkpoint.AppliedSet) {})
				}
				err := c.Compact(context.Background())
				if (exitcode.GetExitCode(err) == exitcode.Fenced) != tc.fenced || (!tc.fenced && err != nil) {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		}
	}
}

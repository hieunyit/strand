package verify

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"strand/internal/model"
)

type resumeConnector struct{ cleared *bool }

func (c resumeConnector) Connect(context.Context) (driver.Conn, error) { return resumeConn{c}, nil }
func (c resumeConnector) Driver() driver.Driver                        { return resumeDriver{} }

type resumeDriver struct{}

func (resumeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type resumeConn struct{ resumeConnector }

func (resumeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (resumeConn) Close() error                        { return nil }
func (resumeConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected transaction") }
func (c resumeConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "DELETE FROM _strand_verify") {
		*c.cleared = true
	}
	return driver.RowsAffected(1), nil
}
func (resumeConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "information_schema.COLUMNS"):
		return &keylessHashRows{cols: []string{"column", "type"}, data: [][]driver.Value{{"id", "bigint"}}}, nil
	case strings.HasPrefix(q, "SELECT MIN("):
		// Empty source: one chunk spans the entire key space.
		return &keylessHashRows{cols: []string{"min", "max"}, data: [][]driver.Value{{nil, nil}}}, nil
	case strings.Contains(q, "COALESCE(SUM("):
		return &keylessHashRows{cols: []string{"count", "checksum"}, data: [][]driver.Value{{int64(0), "0"}}}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", q)
	}
}

type resumeWaiter struct {
	markErr, waitErr error
	marks, waits     int
}

func (w *resumeWaiter) Mark(context.Context) (ApplyMark, error) {
	w.marks++
	return ApplyMark{}, w.markErr
}
func (w *resumeWaiter) Wait(context.Context, ApplyMark) error {
	w.waits++
	return w.waitErr
}

func TestMerkleResumeRequiresSuccessfulCatchUp(t *testing.T) {
	catchUpErr := errors.New("target has not applied source mark")
	for _, tc := range []struct {
		name     string
		markErr  error
		waitErr  error
		wantWait int
	}{
		{"mark failure", catchUpErr, nil, 0},
		{"wait failure", nil, catchUpErr, 1},
		{"cancellation", nil, context.Canceled, 1},
		{"caught up", nil, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cleared := false
			db := sql.OpenDB(resumeConnector{&cleared})
			defer db.Close()
			w := &resumeWaiter{markErr: tc.markErr, waitErr: tc.waitErr}
			mv := NewMerkleVerifier(FullVerifyConfig{
				Source: db, Target: db, Resume: true, ApplyWaiter: w,
			})
			rep, err := mv.VerifyTableFull(context.Background(), model.TableID{Schema: "app", Table: "items"}, "id")
			wantErr := tc.markErr
			if wantErr == nil {
				wantErr = tc.waitErr
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("got report=%+v error=%v, want error %v", rep, err, wantErr)
			}
			if w.marks != 1 || w.waits != tc.wantWait {
				t.Fatalf("waiter calls: marks=%d waits=%d", w.marks, w.waits)
			}
			if wantErr != nil {
				if rep != nil || cleared {
					t.Fatalf("failed catch-up must preserve progress and return no clean report: report=%+v cleared=%v", rep, cleared)
				}
			} else if !cleared || rep.ResumedChunks != 0 || rep.HashedChunks != 1 {
				t.Fatalf("successful resume: report=%+v cleared=%v", rep, cleared)
			}
		})
	}
}

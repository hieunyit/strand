package mysql

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"strand/internal/exitcode"
	"strand/internal/reader"
	"testing"
)

func TestPrefetchRefusesOmittedRowImages(t *testing.T) {
	for _, kind := range []replication.EventType{replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2} {
		r := n03Reader(t)
		r.cfg.RequireFullRowImage = true
		ev := n03Ev(kind, 1, 1, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), nil}}, SkippedColumns: [][]int{{1}}})
		got, ok := n03Collect(r, ev)
		if ok {
			t.Fatal("prefetch accepted omitted columns as SQL NULL")
		}
		if len(got) != 1 || got[0].Type != reader.EventError || exitcode.GetExitCode(got[0].Err) != exitcode.Rejected {
			t.Fatalf("missing explicit FULL-image rejection: %+v", got)
		}
	}
}

func TestPrefetchScopeRetainsTransactionBoundaries(t *testing.T) {
	r := n03Reader(t)
	r.cfg.RequireFullRowImage = true
	r.cfg.TableScope = func(schema, table string) bool { return schema == "app" && table == "included" }
	excluded := n03TableMap()
	evs := []*replication.BinlogEvent{
		n03GTID(),
		n03Ev(replication.TABLE_MAP_EVENT, 1, 1, excluded),
		n03Ev(replication.WRITE_ROWS_EVENTv2, 1, 1, &replication.RowsEvent{TableID: excluded.TableID, Rows: [][]any{{nil}}, SkippedColumns: [][]int{{0}}}),
		n03Ev(replication.XID_EVENT, 1, 1, &replication.XIDEvent{XID: 7}),
	}
	got, ok := n03Collect(r, evs...)
	if !ok {
		t.Fatalf("excluded metadata/rows blocked selected seed scope: %+v", got)
	}
	if len(got) != 2 || got[0].Type != reader.EventGTID || got[1].Type != reader.EventXID {
		t.Fatalf("transaction boundaries lost across excluded rows: %+v", got)
	}
}

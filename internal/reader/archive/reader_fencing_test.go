package archive

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

func archiveEvent(kind replication.EventType, body replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: kind}, Event: body}
}
func fullTableMap() *replication.TableMapEvent {
	return &replication.TableMapEvent{TableID: 7, Schema: []byte("app"), Table: []byte("t"), ColumnCount: 2, ColumnName: [][]byte{[]byte("payload"), []byte("id")}, ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, ColumnMeta: []uint16{0, 0}, PrimaryKey: []uint64{1}}
}
func TestArchiveRejectsUnsafeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*replication.TableMapEvent)
	}{
		{"no names", func(m *replication.TableMapEvent) { m.ColumnName = nil }},
		{"short names", func(m *replication.TableMapEvent) { m.ColumnName = m.ColumnName[:1] }},
		{"no primary key", func(m *replication.TableMapEvent) { m.PrimaryKey = nil }},
		{"invalid primary key", func(m *replication.TableMapEvent) { m.PrimaryKey = []uint64{2} }},
		{"duplicate primary key", func(m *replication.TableMapEvent) { m.PrimaryKey = []uint64{1, 1} }},
		{"duplicate names", func(m *replication.TableMapEvent) { m.ColumnName[0] = m.ColumnName[1] }},
		{"empty name", func(m *replication.TableMapEvent) { m.ColumnName[0] = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := fullTableMap()
			tc.mutate(m)
			r, _ := New(Config{})
			out := make(chan reader.Event, 1)
			maps := map[uint64]*replication.TableMapEvent{}
			err := r.handleEvent(context.Background(), maps, archiveEvent(replication.TABLE_MAP_EVENT, m), out)
			if exitcode.GetExitCode(err) != exitcode.Rejected || len(out) != 0 || len(maps) != 0 {
				t.Fatalf("err=%v events=%d maps=%d", err, len(out), len(maps))
			}
		})
	}
}
func TestArchiveRejectsPartialImagesBeforeCommit(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind replication.EventType
		rows *replication.RowsEvent
	}{
		{"json diff", replication.PARTIAL_UPDATE_ROWS_EVENT, &replication.RowsEvent{TableID: 7}},
		{"minimal before", replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 7, Rows: [][]any{{nil, int32(1)}, {int32(5), int32(1)}}, SkippedColumns: [][]int{{0}, nil}}},
		{"minimal after", replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 7, Rows: [][]any{{int32(2), int32(1)}, {nil, int32(1)}}, SkippedColumns: [][]int{nil, {0}}}},
		{"missing table map", replication.WRITE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 8, Rows: [][]any{{int32(2), int32(1)}}}},
		{"unpaired update", replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 7, Rows: [][]any{{int32(2), int32(1)}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := New(Config{})
			out := make(chan reader.Event, 10)
			maps := map[uint64]*replication.TableMapEvent{7: fullTableMap()}
			gtid := archiveEvent(replication.GTID_EVENT, &replication.GTIDEvent{SID: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, GNO: 1})
			if err := r.handleEvent(context.Background(), maps, gtid, out); err != nil {
				t.Fatal(err)
			}
			err := r.handleEvent(context.Background(), maps, archiveEvent(tc.kind, tc.rows), out)
			if err == nil {
				t.Fatal("unsafe row event accepted")
			}
			asm := assemble.New()
			for len(out) > 0 {
				txn, err := asm.Push(<-out)
				if err != nil || txn != nil {
					t.Fatalf("unexpected covered transaction=%v err=%v", txn, err)
				}
			}
		})
	}
}
func TestArchiveCompressedPayloadChecksEveryEvent(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "partial json"}[partial], func(t *testing.T) {
			r, _ := New(Config{})
			out := make(chan reader.Event, 10)
			maps := map[uint64]*replication.TableMapEvent{}
			rows := archiveEvent(replication.WRITE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 7, Rows: [][]any{{int32(42), int32(9)}}})
			if partial {
				rows = archiveEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, &replication.RowsEvent{TableID: 7})
			}
			payload := archiveEvent(replication.TRANSACTION_PAYLOAD_EVENT, &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{archiveEvent(replication.TABLE_MAP_EVENT, fullTableMap()), rows, archiveEvent(replication.XID_EVENT, &replication.XIDEvent{XID: 1})}})
			if err := r.handleEvent(context.Background(), maps, archiveEvent(replication.GTID_EVENT, &replication.GTIDEvent{SID: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, GNO: 1}), out); err != nil {
				t.Fatal(err)
			}
			err := r.handleEvent(context.Background(), maps, payload, out)
			if partial {
				if exitcode.GetExitCode(err) != exitcode.Rejected {
					t.Fatalf("partial error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			asm := assemble.New()
			var got *model.Txn
			for len(out) > 0 {
				txn, err := asm.Push(<-out)
				if err != nil {
					t.Fatal(err)
				}
				if txn != nil {
					got = txn
				}
			}
			if partial {
				if got != nil {
					t.Fatal("partial transaction was marked covered")
				}
				return
			}
			if got == nil || len(got.Rows) != 1 || len(got.Rows[0].Key) != 1 || got.Rows[0].Key[0] != int32(9) {
				t.Fatalf("wrong decoded transaction: %+v", got)
			}
		})
	}
}
func TestArchiveRejectsUndecodedCriticalEvents(t *testing.T) {
	for _, typ := range []replication.EventType{replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, replication.XID_EVENT, replication.TRANSACTION_PAYLOAD_EVENT, replication.ANONYMOUS_GTID_EVENT, replication.INCIDENT_EVENT, replication.XA_PREPARE_LOG_EVENT} {
		r, _ := New(Config{})
		err := r.handleEvent(context.Background(), map[uint64]*replication.TableMapEvent{}, archiveEvent(typ, &replication.GenericEvent{}), make(chan reader.Event, 1))
		if err == nil {
			t.Errorf("%v was ignored", typ)
		}
	}
}

// Synthetic binlog files exercise the real parser and Start error propagation.
func TestArchiveFileRejectsUnsafeTransaction(t *testing.T) {
	for _, kind := range []string{"missing metadata", "minimal row image", "partial json"} {
		t.Run(kind, func(t *testing.T) {
			fde := make([]byte, 57+40)
			binary.LittleEndian.PutUint16(fde, 4)
			copy(fde[2:52], "5.5.0")
			fde[56] = replication.EventHeaderSize
			for i := 57; i < len(fde); i++ {
				fde[i] = 8
			}
			fde[57+int(replication.WRITE_ROWS_EVENTv2)-1] = 10
			fde[57+int(replication.UPDATE_ROWS_EVENTv2)-1] = 10
			fde[57+int(replication.PARTIAL_UPDATE_ROWS_EVENT)-1] = 10
			gtid := make([]byte, 25)
			copy(gtid[1:17], []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			binary.LittleEndian.PutUint64(gtid[17:], 1)
			tm := []byte{7, 0, 0, 0, 0, 0, 0, 0, 3, 'a', 'p', 'p', 0, 1, 't', 0, 2, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, 0, 0}
			if kind != "missing metadata" {
				tm = append(tm, byte(replication.TABLE_MAP_OPT_META_COLUMN_NAME), 11, 7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', 2, 'i', 'd', byte(replication.TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY), 1, 1)
			}
			data := append([]byte{}, replication.BinLogFileHeader...)
			data = append(data, rawEvent(replication.FORMAT_DESCRIPTION_EVENT, 1, 0, fde)...)
			data = append(data, rawEvent(replication.GTID_EVENT, 1, 0, gtid)...)
			data = append(data, rawEvent(replication.TABLE_MAP_EVENT, 1, 0, tm)...)
			if kind == "minimal row image" {
				// two columns declared, but only column 1 is present (nil in decoded column 0).
				body := []byte{7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 2, 0, 9, 0, 0, 0}
				data = append(data, rawEvent(replication.WRITE_ROWS_EVENTv2, 1, 0, body)...)
			} else if kind == "partial json" {
				// A valid partial-update event with no JSON columns still must fail closed.
				body := []byte{7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 3, 3, 0, 42, 0, 0, 0, 9, 0, 0, 0, 0, 0, 43, 0, 0, 0, 9, 0, 0, 0}
				data = append(data, rawEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, 1, 0, body)...)
			}
			data = append(data, rawEvent(replication.XID_EVENT, 1, 0, make([]byte, 8))...)
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "binlog.000001"), data, 0600); err != nil {
				t.Fatal(err)
			}
			r, _ := New(Config{Dir: dir})
			stream, err := r.Start(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			asm := assemble.New()
			var gotErr error
			var committed bool
			for ev := range stream {
				if ev.Type == reader.EventError {
					gotErr = ev.Err
					continue
				}
				txn, err := asm.Push(ev)
				if err != nil {
					t.Fatal(err)
				}
				committed = committed || txn != nil
			}
			if exitcode.GetExitCode(gotErr) != exitcode.Rejected || committed {
				t.Fatalf("error=%v committed=%v", gotErr, committed)
			}
		})
	}
}

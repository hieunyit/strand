package mysql

import (
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strand/internal/reader"
)

func TestPrefetchTableMapUniqueIdentityFromCatalog(t *testing.T) {
	for _, full := range []bool{false, true} {
		p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("a", "int"), col("b", "int")}, PK: []string{"b"}, KeyMode: "unique"}}}
		r, err := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: p, RequireFullRowImage: true})
		if err != nil {
			t.Fatal(err)
		}
		tm := tm57([]byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_LONG}, []uint16{0, 0})
		if full {
			tm.ColumnName = [][]byte{[]byte("a"), []byte("b")}
		}
		st := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
		var events []reader.Event
		if !r.handleEvent(st, tmEv(tm), func(e reader.Event) bool { events = append(events, e); return true }) {
			t.Fatalf("full=%v: %v", full, events)
		}
		if len(events) != 1 || events[0].KeyMode != "unique" || len(events[0].PKColumns) != 1 || events[0].PKColumns[0] != "b" {
			t.Fatalf("full=%v: %+v", full, events)
		}
	}
}

func TestPrefetchTableMapRejectsCatalogNameMismatch(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("renamed", "int")}, PK: []string{"renamed"}, KeyMode: "unique"}}}
	r, err := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: p, RequireFullRowImage: true})
	if err != nil {
		t.Fatal(err)
	}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_LONG}, []uint16{0})
	tm.ColumnName = [][]byte{[]byte("old")}
	st := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	var events []reader.Event
	if r.handleEvent(st, tmEv(tm), func(e reader.Event) bool { events = append(events, e); return true }) {
		t.Fatalf("accepted mismatched metadata: %+v", events)
	}
}

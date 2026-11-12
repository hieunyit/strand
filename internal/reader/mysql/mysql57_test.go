package mysql

// Unit test thuần cho nguồn MySQL 5.7: TABLE_MAP không có metadata tùy chọn (tên cột, PK, signedness,
// charset, nhãn ENUM/SET), mọi thứ lấy từ TableInfoProvider (information_schema).

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/exitcode"
	"strand/internal/reader"
)

type fakeInfo struct {
	tables map[string]*TableInfo
	loads  int
}

func (f *fakeInfo) LoadTable(_ context.Context, schema, table string) (*TableInfo, bool, error) {
	f.loads++
	t, ok := f.tables[schema+"."+table]
	return t, ok, nil
}

func col(name, dt string) ColumnInfo { return ColumnInfo{Name: name, DataType: dt} }

// tm57 dựng TABLE_MAP như MySQL 5.7: chỉ kiểu + meta, không có optional metadata.
func tm57(types []byte, meta []uint16) *replication.TableMapEvent {
	return &replication.TableMapEvent{TableID: 42, Schema: []byte("app"), Table: []byte("t"),
		ColumnCount: uint64(len(types)), ColumnType: types, ColumnMeta: meta}
}

func run57(t *testing.T, p TableInfoProvider, charsets map[uint64]string, evs ...*replication.BinlogEvent) ([]reader.Event, bool) {
	t.Helper()
	r, err := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: p, CollationCharsets: charsets})
	if err != nil {
		t.Fatal(err)
	}
	st := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	for _, ev := range evs {
		if !r.handleEvent(st, ev, emit) {
			return got, false
		}
	}
	return got, true
}

func tmEv(tm *replication.TableMapEvent) *replication.BinlogEvent {
	return n03Ev(replication.TABLE_MAP_EVENT, 600, 50, tm)
}
func wrEv(rows ...[]any) *replication.BinlogEvent {
	return n03Ev(replication.WRITE_ROWS_EVENTv2, 700, 60, &replication.RowsEvent{TableID: 42, Rows: rows})
}

func TestM57TableMapNamesAndPKFromInformationSchema(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {
		Columns: []ColumnInfo{col("id", "int"), col("name", "varchar")}, PK: []string{"id"}}}}
	got, ok := run57(t, p, nil, tmEv(tm57([]byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR}, []uint16{0, 100})))
	if !ok || len(got) != 1 {
		t.Fatalf("ok=%v got=%+v", ok, got)
	}
	if !reflect.DeepEqual(got[0].Columns, []string{"id", "name"}) || !reflect.DeepEqual(got[0].PKColumns, []string{"id"}) {
		t.Fatalf("columns=%v pk=%v", got[0].Columns, got[0].PKColumns)
	}
}

func TestM57UnsignedFromInformationSchema(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{
		{Name: "a", DataType: "tinyint", Unsigned: true}, {Name: "b", DataType: "smallint", Unsigned: true},
		{Name: "c", DataType: "mediumint", Unsigned: true}, {Name: "d", DataType: "int", Unsigned: true},
		{Name: "e", DataType: "bigint", Unsigned: true}, {Name: "f", DataType: "bigint"}}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_SHORT, gomysql.MYSQL_TYPE_INT24,
		gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_LONGLONG, gomysql.MYSQL_TYPE_LONGLONG}, make([]uint16, 6))
	got, ok := run57(t, p, nil, tmEv(tm), wrEv(
		[]any{int8(-1), int16(-1), int32(-1), int32(-1), int64(-1), int64(-1)},
		[]any{int8(5), int16(5), int32(5), int32(5), int64(5), nil}))
	if !ok || len(got) != 2 {
		t.Fatalf("ok=%v got=%+v", ok, got)
	}
	want := [][]any{
		{uint8(255), uint16(65535), uint32(0xFFFFFF), uint32(math.MaxUint32), uint64(math.MaxUint64), int64(-1)},
		{uint8(5), uint16(5), uint32(5), uint32(5), uint64(5), nil}}
	if !reflect.DeepEqual(got[1].Rows, want) {
		t.Fatalf("rows=%#v want %#v", got[1].Rows, want)
	}
}

// Có SignednessBitmap trong TABLE_MAP (8.0): go-mysql đã giải mã đúng, không chuyển lần nữa.
func TestM57UnsignedKeepsTableMapMetadata(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{{Name: "a", DataType: "bigint", Unsigned: true}}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_LONGLONG}, []uint16{0})
	tm.SignednessBitmap = []byte{0x80}
	got, _ := run57(t, p, nil, tmEv(tm), wrEv([]any{uint64(math.MaxUint64)}))
	if got[1].Rows[0][0] != uint64(math.MaxUint64) {
		t.Fatalf("rows=%#v", got[1].Rows)
	}
}

func TestM57EnumSetLabelsFromInformationSchema(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{
		{Name: "e", DataType: "enum", Values: []string{"small", "medium", "large"}},
		{Name: "s", DataType: "set", Values: []string{"a", "b", "c", "d"}}}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_STRING, gomysql.MYSQL_TYPE_STRING},
		[]uint16{uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, uint16(gomysql.MYSQL_TYPE_SET)<<8 | 1})
	got, ok := run57(t, p, nil, tmEv(tm), wrEv(
		[]any{int64(3), int64(0b1011)}, []any{int64(0), int64(0)}, []any{nil, nil}))
	if !ok || len(got) != 2 {
		t.Fatalf("ok=%v got=%+v", ok, got)
	}
	want := [][]any{{"large", "a,b,d"}, {"", ""}, {nil, nil}}
	if !reflect.DeepEqual(got[1].Rows, want) {
		t.Fatalf("rows=%#v want %#v", got[1].Rows, want)
	}
	// Chỉ số ngoài danh sách nhãn: lỗi thay vì ghi sai.
	_, ok = run57(t, p, nil, tmEv(tm), wrEv([]any{int64(9), int64(1)}))
	if ok {
		t.Fatal("ENUM index beyond labels must fail")
	}
}

func TestM57Latin1AndBinaryFromInformationSchema(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{
		{Name: "v", DataType: "varchar", Charset: "latin1"}, {Name: "u", DataType: "varchar", Charset: "utf8mb4"},
		{Name: "b", DataType: "binary"}}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_STRING},
		[]uint16{100, 100, 4})
	got, ok := run57(t, p, map[uint64]string{}, tmEv(tm), wrEv([]any{"caf\xe9 \x80", "hé", "ab"}))
	if !ok {
		t.Fatalf("got=%+v", got)
	}
	r := got[1].Rows[0]
	if r[0] != "café €" || r[1] != "hé" || r[2] != "ab\x00\x00" {
		t.Fatalf("row=%#v", r)
	}
}

func queryEv(q string) *replication.BinlogEvent {
	return n03Ev(replication.QUERY_EVENT, 650, 40, &replication.QueryEvent{Schema: []byte("app"), Query: []byte(q)})
}

// ALTER TABLE ADD COLUMN khi đang stream: số cột binlog đổi, tên cột không có trong event; reader phải đọc lại
// information_schema và phát tên cột mới.
func TestM57AlterTableAddColumnReloads(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("id", "int"), col("a", "varchar")}, PK: []string{"id"}}}}
	two := tm57([]byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR}, []uint16{0, 100})
	three := tm57([]byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_LONG}, []uint16{0, 100, 0})
	r, _ := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: p})
	st := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	r.handleEvent(st, tmEv(two), emit)
	// Source đã ALTER xong khi reader tới DDL.
	p.tables["app.t"] = &TableInfo{Columns: []ColumnInfo{col("id", "int"), col("a", "varchar"), col("b", "int")}, PK: []string{"id"}}
	r.handleEvent(st, queryEv("ALTER TABLE t ADD COLUMN b INT"), emit)
	if !r.handleEvent(st, tmEv(three), emit) {
		t.Fatalf("stream stopped: %+v", got)
	}
	last := got[len(got)-1]
	if !reflect.DeepEqual(last.Columns, []string{"id", "a", "b"}) {
		t.Fatalf("columns=%v", last.Columns)
	}
}

// Số cột lệch dù không thấy DDL (cache cũ): đọc lại một lần; không đối chiếu được thì dừng với lỗi rõ ràng.
func TestM57ColumnCountMismatchFailsClosed(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("id", "int"), col("a", "varchar")}, PK: []string{"id"}}}}
	three := tm57([]byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_LONG}, []uint16{0, 100, 0})
	got, ok := run57(t, p, nil, tmEv(three))
	if ok || len(got) != 1 || got[0].Type != reader.EventError {
		t.Fatalf("ok=%v got=%+v", ok, got)
	}
	if !strings.Contains(got[0].Err.Error(), exitcode.ErrSchemaChangePending) || !strings.Contains(got[0].Err.Error(), "binlog has 3 columns, information_schema has 2") {
		t.Fatalf("err=%v", got[0].Err)
	}
}

// Số cột không đổi nhưng kiểu cột đổi (MODIFY): phát hiện qua kiểu binlog.
func TestM57ColumnTypeMismatchFailsClosed(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("id", "int"), col("a", "varchar")}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_LONGLONG, gomysql.MYSQL_TYPE_VARCHAR}, []uint16{0, 100})
	if _, ok := run57(t, p, nil, tmEv(tm)); ok {
		t.Fatal("type mismatch must fail closed")
	}
}

// DDL làm cache vô hiệu: MODIFY INT -> INT UNSIGNED (cùng số cột, cùng kiểu binlog) phải được nhận ra.
func TestM57DDLInvalidatesInfoCache(t *testing.T) {
	p := &fakeInfo{tables: map[string]*TableInfo{"app.t": {Columns: []ColumnInfo{col("id", "int")}}}}
	tm := tm57([]byte{gomysql.MYSQL_TYPE_LONG}, []uint16{0})
	r, _ := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: p})
	st := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	r.handleEvent(st, tmEv(tm), emit)
	r.handleEvent(st, tmEv(tm), emit)
	if p.loads != 1 {
		t.Fatalf("cache not used: loads=%d", p.loads)
	}
	p.tables["app.t"] = &TableInfo{Columns: []ColumnInfo{{Name: "id", DataType: "int", Unsigned: true}}}
	r.handleEvent(st, queryEv("ALTER TABLE t MODIFY id INT UNSIGNED"), emit)
	r.handleEvent(st, tmEv(tm), emit)
	r.handleEvent(st, wrEv([]any{int32(-1)}), emit)
	if rows := got[len(got)-1].Rows; rows[0][0] != uint32(math.MaxUint32) {
		t.Fatalf("rows=%#v", rows)
	}
}

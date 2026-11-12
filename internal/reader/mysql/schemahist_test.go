package mysql

import (
	"reflect"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/checkpoint"
	"strand/internal/reader"
)

const hU = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func gs(t *testing.T, s string) gomysql.GTIDSet {
	t.Helper()
	g, err := gomysql.ParseMysqlGTIDSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

type memStore struct {
	m     map[string][]SchemaVersion
	saves int
}

func (s *memStore) LoadSchemaHistory() map[string][]SchemaVersion { return s.m }
func (s *memStore) SaveSchemaHistory(m map[string][]SchemaVersion) error {
	s.saves++
	s.m = map[string][]SchemaVersion{}
	for k, vs := range m {
		s.m[k] = append([]SchemaVersion(nil), vs...)
	}
	return nil
}

func ti(names ...string) *TableInfo {
	t := &TableInfo{}
	for _, n := range names {
		t.Columns = append(t.Columns, col(n, "int"))
	}
	return t
}

func TestSeedSchemaVersions(t *testing.T) {
	old, nw := ti("a", "b", "c"), ti("a", "b")
	vs := []SchemaVersion{
		{Info: old, From: hU + ":1-5", Until: hU + ":10"},
		{Info: nw, From: hU + ":10"},
	}
	cases := []struct {
		name, from string
		want       *TableInfo
		kept       int
	}{
		{"DDL chưa áp: dùng version cũ", hU + ":1-9", old, 2},
		{"DDL đã áp: bỏ version cũ, dùng mới", hU + ":1-10", nw, 1},
		{"checkpoint trước From của mọi version cũ: không dùng gì", hU + ":1-3", nil, 2},
		{"tập rỗng", "", nil, 2},
	}
	for _, c := range cases {
		got, kept := seedSchemaVersions(vs, gs(t, c.from))
		if got != c.want || len(kept) != c.kept {
			t.Errorf("%s: got=%v kept=%d, want %v kept=%d", c.name, got, len(kept), c.want, c.kept)
		}
	}
}

func TestSchemaHistoryRecordAndSupersede(t *testing.T) {
	st := &memStore{}
	h, cache := openSchemaHistory(st, gs(t, hU+":1-5"))
	if len(cache) != 0 {
		t.Fatal("cache phải rỗng")
	}
	if err := h.record("app.t", ti("a", "b", "c")); err != nil {
		t.Fatal(err)
	}
	if v := st.m["app.t"][0]; v.From != hU+":1-5" || v.Until != "" {
		t.Fatalf("version đầu: %+v", v)
	}
	if err := h.supersede(hU + ":8"); err != nil {
		t.Fatal(err)
	}
	if v := st.m["app.t"][0]; v.Until != hU+":8" {
		t.Fatalf("supersede: %+v", v)
	}
	if err := h.record("app.t", ti("a", "b")); err != nil {
		t.Fatal(err)
	}
	vs := st.m["app.t"]
	if len(vs) != 2 || vs[1].From != hU+":8" || vs[1].Until != "" {
		t.Fatalf("version sau DDL: %+v", vs)
	}
	// Restart khi DDL (gno 8) chưa áp: dùng version cũ 3 cột. Đã áp: dùng version 2 cột.
	if _, c := openSchemaHistory(st, gs(t, hU+":1-7")); len(c["app.t"].Columns) != 3 {
		t.Fatalf("from=1-7: %+v", c["app.t"])
	}
	if _, c := openSchemaHistory(st, gs(t, hU+":1-8")); len(c["app.t"].Columns) != 2 {
		t.Fatalf("from=1-8: %+v", c["app.t"])
	}
}

// Version còn mở bị binlog bác bỏ (không có DDL) thì bị thay, không tích lũy.
func TestSchemaHistoryReplacesOpenVersion(t *testing.T) {
	st := &memStore{}
	h, _ := openSchemaHistory(st, gs(t, hU+":1-5"))
	_ = h.record("app.t", ti("a"))
	_ = h.record("app.t", ti("a", "b"))
	if len(st.m["app.t"]) != 1 || len(st.m["app.t"][0].Info.Columns) != 2 {
		t.Fatalf("%+v", st.m["app.t"])
	}
}

func TestSchemaHistoryCapsVersions(t *testing.T) {
	st := &memStore{}
	h, _ := openSchemaHistory(st, gs(t, hU+":1-5"))
	for i := 0; i < 10; i++ {
		_ = h.record("app.t", ti("a"))
		_ = h.supersede(hU + ":" + string(rune('6'+i)))
	}
	if n := len(st.m["app.t"]); n != maxSchemaVersions {
		t.Fatalf("giữ %d version, want %d", n, maxSchemaVersions)
	}
}

// Tái hiện lỗi: Strand dừng, DDL DROP COLUMN chạy ở nguồn, information_schema đã là cấu trúc mới.
// Row ghi trước DDL (4 cột) phải giải mã bằng info đã lưu; sau DDL thì đọc lại information_schema.
func TestSchemaHistorySeedDecodesPreDDLRows(t *testing.T) {
	old := &TableInfo{Columns: []ColumnInfo{col("id", "int"), col("a", "int"), col("b", "int"), col("w", "int")}, PK: []string{"id"}}
	nw := &TableInfo{Columns: []ColumnInfo{col("id", "int"), col("a", "int"), col("b", "int")}, PK: []string{"id"}}
	st := &memStore{m: map[string][]SchemaVersion{"app.t": {{Info: old, From: hU + ":1-5"}}}}
	live := &fakeInfo{tables: map[string]*TableInfo{"app.t": nw}}
	r, err := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: live, SchemaHistory: st})
	if err != nil {
		t.Fatal(err)
	}
	s := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	s.hist, s.infoCache = openSchemaHistory(st, gs(t, hU+":1-5"))
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	L := byte(gomysql.MYSQL_TYPE_LONG)
	// TABLE_MAP 4 cột (trước DDL): dùng info đã lưu, không chạm information_schema.
	if !r.handleEvent(s, tmEv(tm57([]byte{L, L, L, L}, make([]uint16, 4))), emit) {
		t.Fatalf("pre-DDL: %+v", got)
	}
	if live.loads != 0 || !reflect.DeepEqual(got[0].Columns, []string{"id", "a", "b", "w"}) {
		t.Fatalf("loads=%d cols=%v", live.loads, got[0].Columns)
	}
	// DDL: version cũ kết thúc, ghi bền; sau đó TABLE_MAP 3 cột đọc lại information_schema.
	r.setPos(hU + ":6")
	ddl := n03Ev(replication.QUERY_EVENT, 800, 40, &replication.QueryEvent{Query: []byte("ALTER TABLE t DROP COLUMN w")})
	if !r.handleEvent(s, ddl, emit) {
		t.Fatal("ddl")
	}
	if st.m["app.t"][0].Until != hU+":6" {
		t.Fatalf("Until chưa ghi: %+v", st.m["app.t"])
	}
	if !r.handleEvent(s, tmEv(tm57([]byte{L, L, L}, make([]uint16, 3))), emit) {
		t.Fatal("post-DDL")
	}
	if live.loads != 1 || len(st.m["app.t"]) != 2 || st.m["app.t"][1].From != hU+":6" {
		t.Fatalf("loads=%d hist=%+v", live.loads, st.m["app.t"])
	}
}

// Nhiều DDL khi dừng: info đã lưu không khớp binlog thì vẫn fail closed (thông báo cũ).
func TestSchemaHistorySeedMismatchStillFailsClosed(t *testing.T) {
	old := ti("id", "a", "b", "w")
	st := &memStore{m: map[string][]SchemaVersion{"app.t": {{Info: old, From: hU + ":1-5"}}}}
	live := &fakeInfo{tables: map[string]*TableInfo{"app.t": ti("id")}} // 2 DDL: live 1 cột, binlog 3 cột
	r, _ := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: live, SchemaHistory: st})
	s := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	s.hist, s.infoCache = openSchemaHistory(st, gs(t, hU+":1-5"))
	L := byte(gomysql.MYSQL_TYPE_LONG)
	var got []reader.Event
	ok := r.handleEvent(s, tmEv(tm57([]byte{L, L, L}, make([]uint16, 3))), func(e reader.Event) bool { got = append(got, e); return true })
	if ok || len(got) != 1 || got[0].Err == nil {
		t.Fatalf("ok=%v got=%+v", ok, got)
	}
}

// --- Chế độ vị trí binlog (SH-1/SH-2) ---

func pk(t *testing.T, seq uint32, off uint64) uint64 {
	t.Helper()
	k, err := checkpoint.BinlogKey(seq, off)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestSeedSchemaVersionsPos(t *testing.T) {
	old, nw := ti("a", "b", "c"), ti("a", "b")
	f := checkpoint.FormatBinlogKey
	vs := []SchemaVersion{
		{Info: old, From: f(pk(t, 1, 100)), Until: f(pk(t, 1, 500))},
		{Info: nw, From: f(pk(t, 1, 500))},
	}
	cases := []struct {
		name  string
		start uint64
		want  *TableInfo
		kept  int
	}{
		{"DDL chưa qua: dùng version cũ", pk(t, 1, 400), old, 2},
		{"bắt đầu đúng đầu DDL (Txn của DDL chưa apply): version cũ", pk(t, 1, 480), old, 2},
		{"bắt đầu đúng cuối DDL: DDL đã qua, dùng mới", pk(t, 1, 500), nw, 1},
		{"sang file sau", pk(t, 2, 4), nw, 1},
		{"trước From của mọi version: không dùng gì", pk(t, 1, 50), nil, 2},
	}
	for _, c := range cases {
		got, kept := seedSchemaVersionsPos(vs, c.start)
		if got != c.want || len(kept) != c.kept {
			t.Errorf("%s: got=%v kept=%d want %v kept=%d", c.name, got, len(kept), c.want, c.kept)
		}
	}
	// State kiểu GTID (hoặc hỏng) không đoán: bỏ version.
	bad := []SchemaVersion{{Info: old, From: hU + ":1-5"}}
	if got, kept := seedSchemaVersionsPos(bad, pk(t, 1, 400)); got != nil || len(kept) != 0 {
		t.Fatalf("GTID-marked version must be dropped in pos mode: %v %v", got, kept)
	}
}

// Tái hiện lỗi (d): strand dừng, DDL DROP COLUMN chạy ở nguồn không GTID; row trước DDL giải mã bằng info đã lưu.
func TestSchemaHistoryPosDecodesPreDDLRows(t *testing.T) {
	old := &TableInfo{Columns: []ColumnInfo{col("id", "int"), col("a", "int"), col("b", "int"), col("w", "int")}, PK: []string{"id"}}
	nw := &TableInfo{Columns: []ColumnInfo{col("id", "int"), col("a", "int"), col("b", "int")}, PK: []string{"id"}}
	start := pk(t, 3, 200)
	st := &memStore{m: map[string][]SchemaVersion{"app.t": {{Info: old, From: checkpoint.FormatBinlogKey(pk(t, 3, 100))}}}}
	live := &fakeInfo{tables: map[string]*TableInfo{"app.t": nw}}
	r, err := New(Config{Host: "127.0.0.1", User: "x", ServerID: 9, TableInfo: live, SchemaHistory: st})
	if err != nil {
		t.Fatal(err)
	}
	s := &streamState{tableMap: map[uint64]*replication.TableMapEvent{}}
	if err := s.initPosMode("mysql-bin", "mysql-bin.000003"); err != nil {
		t.Fatal(err)
	}
	s.hist, s.infoCache = openSchemaHistoryPos(st, start)
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	L := byte(gomysql.MYSQL_TYPE_LONG)
	if !r.handleEvent(s, tmEv(tm57([]byte{L, L, L, L}, make([]uint16, 4))), emit) {
		t.Fatalf("pre-DDL: %+v", got)
	}
	if live.loads != 0 || !reflect.DeepEqual(got[0].Columns, []string{"id", "a", "b", "w"}) {
		t.Fatalf("loads=%d cols=%v", live.loads, got[0].Columns)
	}
	ddl := n03Ev(replication.QUERY_EVENT, 800, 40, &replication.QueryEvent{Query: []byte("ALTER TABLE t DROP COLUMN w")})
	if !r.handleEvent(s, ddl, emit) {
		t.Fatal("ddl")
	}
	if want := checkpoint.FormatBinlogKey(pk(t, 3, 800)); st.m["app.t"][0].Until != want {
		t.Fatalf("Until=%q want %q", st.m["app.t"][0].Until, want)
	}
	if !r.handleEvent(s, tmEv(tm57([]byte{L, L, L}, make([]uint16, 3))), emit) {
		t.Fatal("post-DDL")
	}
	if live.loads != 1 || len(st.m["app.t"]) != 2 || st.m["app.t"][1].From != checkpoint.FormatBinlogKey(pk(t, 3, 800)) {
		t.Fatalf("loads=%d hist=%+v", live.loads, st.m["app.t"])
	}
	// Restart sau DDL (start ≥ Until): dùng version mới, không dùng version 4 cột.
	if _, c := openSchemaHistoryPos(st, pk(t, 3, 800)); len(c["app.t"].Columns) != 3 {
		t.Fatalf("restart after DDL: %+v", c["app.t"])
	}
}

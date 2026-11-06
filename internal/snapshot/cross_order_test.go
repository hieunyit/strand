package snapshot

// Gate 2 "_ci → C" (cross_order.go): unit test thuần — điều kiện nhận cặp PK khác thứ tự, phép thuộc phần
// chưa đọc, phần DELETE của flush theo tập khóa, flush dừng khi khóa row snapshot không mã hóa được, và
// weigher theo lô (driver giả, không cần DB).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"strand/internal/model"
)

func TestG2_CrossEngineKeySetAcceptsOnlyFinerTargetEquality(t *testing.T) {
	ci := func(pad bool, char bool) PKColumn {
		return PKColumn{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci", PadSpace: pad, FixedChar: char}
	}
	text := PKColumn{Name: "k", Kind: PKBytes, SQLType: "text", Collation: "C"}
	varchar := PKColumn{Name: "k", Kind: PKBytes, SQLType: "varchar", Collation: "C"}
	bpchar := PKColumn{Name: "k", Kind: PKTrimBytes, SQLType: "bpchar", Collation: "C"}
	bytea := PKColumn{Name: "k", Kind: PKBytes, SQLType: "bytea"}
	one := func(c PKColumn) *PKCodec { return &PKCodec{Cols: []PKColumn{c}} }
	cases := []struct {
		name     string
		src, dst *PKCodec
		ok       bool
	}{
		{"0900_ai_ci varchar → text C", one(ci(false, false)), one(text), true},
		{"general_ci (PAD SPACE) → varchar C", one(ci(true, false)), one(varchar), true},
		{"general_ci (PAD SPACE) → bpchar C", one(ci(true, false)), one(bpchar), true},
		{"CHAR 0900_ai_ci (NO PAD) → bpchar C", one(ci(false, true)), one(bpchar), true},
		// VARCHAR NO PAD: 'a' ≠ 'a ' ở source nhưng bằng nhau ở bpchar — target THÔ hơn source.
		{"VARCHAR 0900_ai_ci → bpchar C", one(ci(false, false)), one(bpchar), false},
		{"_ci → bytea", one(ci(false, false)), one(bytea), false},
		{"PAD SPACE _bin → text C", one(PKColumn{Name: "k", Kind: PKPadBytes, Charset: "utf8mb4", Collation: "utf8mb4_bin"}), one(text), true},
		{"0900_bin → text C", one(PKColumn{Name: "k", Kind: PKBytes, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}), one(text), true},
		{"latin1 _ci → text C", one(PKColumn{Name: "k", Kind: PKWeight, Charset: "latin1", Collation: "latin1_swedish_ci"}), one(text), false},
		{"binary CHAR → text", one(PKColumn{Name: "k", Kind: PKBytes, Charset: "binary", Collation: "binary"}), one(text), false},
		{"no string column", one(PKColumn{Name: "k", Kind: PKInt}), one(PKColumn{Name: "k", Kind: PKInt}), false},
		{"(int, _ci) → (int8, text C)",
			&PKCodec{Cols: []PKColumn{{Name: "t", Kind: PKUint}, ci(false, false)}},
			&PKCodec{Cols: []PKColumn{{Name: "t", Kind: PKInt, SQLType: "int8"}, text}}, true},
		{"(decimal, _ci) → (numeric, text C)",
			&PKCodec{Cols: []PKColumn{{Name: "d", Kind: PKDecimal}, ci(false, false)}},
			&PKCodec{Cols: []PKColumn{{Name: "d", Kind: PKDecimal, SQLType: "numeric"}, text}}, true},
		// Thời gian: text của PG khác dạng MySQL — chưa chứng minh chuyển dạng, từ chối.
		{"(datetime, _ci) → (timestamp, text C)",
			&PKCodec{Cols: []PKColumn{{Name: "d", Kind: PKDateTime}, ci(false, false)}},
			&PKCodec{Cols: []PKColumn{{Name: "d", Kind: PKDateTime, SQLType: "timestamp"}, text}}, false},
		{"column name differs", one(ci(false, false)), one(PKColumn{Name: "x", Kind: PKBytes, SQLType: "text"}), false},
		{"column count differs", one(ci(false, false)), &PKCodec{Cols: []PKColumn{text, text}}, false},
	}
	for _, c := range cases {
		err := CrossEngineKeySet(c.src, c.dst)
		if (err == nil) != c.ok {
			t.Errorf("%s: CrossEngineKeySet = %v, want ok=%v", c.name, err, c.ok)
		}
	}
	// Cặp được nhận ở đây đều bị đường "cùng thứ tự" từ chối (không được dùng range ở target).
	if err := CrossEngineSameOrder(one(ci(false, false)), one(text)); err == nil {
		t.Fatal("CrossEngineSameOrder accepted _ci → C")
	}
}

func TestG2_MySQLPKColumnMarksFixedChar(t *testing.T) {
	for _, c := range []struct {
		dt, coll, pad string
		fixed         bool
	}{
		{"char", "utf8mb4_0900_ai_ci", "NO PAD", true},
		{"varchar", "utf8mb4_0900_ai_ci", "NO PAD", false},
		{"char", "utf8mb4_bin", "PAD SPACE", true},
		{"varchar", "utf8mb4_0900_bin", "NO PAD", false},
	} {
		col, err := MySQLPKColumn("k", c.dt, c.dt+"(10)", "utf8mb4", c.coll, c.pad)
		if err != nil {
			t.Fatal(err)
		}
		if col.FixedChar != c.fixed {
			t.Errorf("%s %s: FixedChar = %v, want %v", c.dt, c.coll, col.FixedChar, c.fixed)
		}
	}
}

func TestG2_InUnread(t *testing.T) {
	lo, hi := Bound{K: "c"}, Bound{K: "m"}
	excl := Bound{K: "c", Excl: true}
	reread := map[Key]bool{"z": true}
	for _, c := range []struct {
		k        Key
		lo       Bound
		hasRange bool
		want     bool
	}{
		{"c", lo, true, true}, {"c", excl, true, false}, {"d", excl, true, true}, {"m", lo, true, true},
		{"n", lo, true, false}, {"b", lo, true, false}, {"z", lo, true, true}, {"z", lo, false, true},
		{"d", lo, false, false},
	} {
		if got := InUnread(c.k, c.lo, hi, c.hasRange, reread); got != c.want {
			t.Errorf("InUnread(%q, lo=%+v, hasRange=%v) = %v, want %v", c.k, c.lo, c.hasRange, got, c.want)
		}
	}
}

func TestG2_ExactDeletesAreTombstonesWithExactBytes(t *testing.T) {
	codec := &PKCodec{Cols: []PKColumn{{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}},
		Weigh: func(_ PKColumn, v model.Value) ([]byte, error) { return []byte(strings.ToLower(v.(string))), nil }}
	k := func(s string) Key {
		key, err := codec.Encode([]model.Value{s})
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	ch := &Chunk{BufferedChanges: map[Key]*model.RowChange{
		k("Bb"): {Op: model.OpDelete, Key: []model.Value{"Bb"}},
		k("a"):  {Op: model.OpInsert, Key: []model.Value{"A"}},
		k("cC"): {Op: model.OpDelete, Key: []model.Value{"cC"}},
	}}
	got, err := exactDeletes(ch, codec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].V[0] != "Bb" || got[1].V[0] != "cC" || got[0].K != k("bb") {
		t.Fatalf("exactDeletes = %+v, want the two tombstones with their exact bytes, in key order", got)
	}
}

// Trước đây MergeLocked bỏ âm thầm row snapshot có khóa không mã hóa được (vd. WEIGHT_STRING timeout):
// đường range XÓA nó ở target, đường tập khóa không ghi nó. FlushChunk phải dừng, không ghi gì.
func TestG2_FlushStopsWhenASnapshotKeyCannotBeEncoded(t *testing.T) {
	fail := atomic.Bool{}
	codec := &PKCodec{Cols: []PKColumn{{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}},
		Weigh: func(_ PKColumn, v model.Value) ([]byte, error) {
			if fail.Load() && v.(string) == "b" {
				return nil, errors.New("weight string timeout")
			}
			return []byte(strings.ToLower(v.(string))), nil
		}}
	tbl := model.TableID{Schema: "app", Table: "t"}
	for _, exact := range []bool{false, true} {
		fail.Store(false)
		s := NewMySQLSnapshotter(DefaultChunkOptions())
		var flushed int
		s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
			return nil
		}
		s.FlushRange = func(context.Context, model.TableID, []string, []*model.RowChange, Bound, Bound) error {
			flushed++
			return nil
		}
		s.FlushExact = func(context.Context, model.TableID, []string, []*model.RowChange, []Bound) error {
			flushed++
			return nil
		}
		s.ExactKeys = func(model.TableID) bool { return exact }
		lo, _ := codec.BoundOf([]model.Value{"a"})
		hi, _ := codec.BoundOf([]model.Value{"z"})
		ch := &Chunk{Table: tbl, PKCol: "k", Lo: lo, Hi: hi, State: StatePending, BufferedChanges: map[Key]*model.RowChange{}}
		s.Manager.RegisterTableKeys(tbl, "k", codec, []*Chunk{ch}, &hi)
		c, err := s.Manager.CarveChunkKeys(tbl, lo, hi)
		if err != nil {
			t.Fatal(err)
		}
		s.Manager.FinishReading(c, nil, []*model.RowChange{
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{"a"}, Cols: []model.ColValue{{Name: "k", Value: "a"}}},
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{"b"}, Cols: []model.ColValue{{Name: "k", Value: "b"}}},
		})
		fail.Store(true)
		err = s.FlushChunk(context.Background(), nil, c, tbl, "k", []string{"k"})
		if err == nil || !strings.Contains(err.Error(), "weight string timeout") {
			t.Fatalf("exact=%v: FlushChunk = %v, want the encode error", exact, err)
		}
		if flushed != 0 || c.State == StateDone {
			t.Fatalf("exact=%v: chunk flushed (%d) / marked done (%s) despite a lost row", exact, flushed, c.State)
		}
	}
}

func TestG2_FlushChunkUsesExactFlushOnlyForExactKeyTables(t *testing.T) {
	codec := &PKCodec{Cols: []PKColumn{{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}},
		Weigh: func(_ PKColumn, v model.Value) ([]byte, error) { return []byte(strings.ToLower(v.(string))), nil }}
	tbl := model.TableID{Schema: "app", Table: "t"}
	for _, exact := range []bool{false, true} {
		s := NewMySQLSnapshotter(DefaultChunkOptions())
		var ranges, exacts int
		var dels []Bound
		var rows []*model.RowChange
		s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
			return nil
		}
		s.FlushRange = func(context.Context, model.TableID, []string, []*model.RowChange, Bound, Bound) error {
			ranges++
			return nil
		}
		s.FlushExact = func(_ context.Context, _ model.TableID, _ []string, r []*model.RowChange, d []Bound) error {
			exacts++
			rows, dels = r, d
			return nil
		}
		s.ExactKeys = func(model.TableID) bool { return exact }
		lo, _ := codec.BoundOf([]model.Value{"a"})
		hi, _ := codec.BoundOf([]model.Value{"z"})
		s.Manager.RegisterTableKeys(tbl, "k", codec, []*Chunk{{Table: tbl, PKCol: "k", Lo: lo, Hi: hi, State: StatePending,
			BufferedChanges: map[Key]*model.RowChange{}}}, &hi)
		c, _ := s.Manager.CarveChunkKeys(tbl, lo, hi)
		s.Manager.BeginReading(c)
		// Buffer: 'b' → 'B' (cùng lớp: tombstone 'b' + upsert 'B'), xóa 'c'.
		ren := &model.RowChange{Table: tbl, Op: model.OpUpdate, OldKey: []model.Value{"b"}, Key: []model.Value{"B"},
			Cols: []model.ColValue{{Name: "k", Value: "B"}}}
		s.Manager.HandleStreamEventWithPKSplit(ren, model.Pos{})
		s.Manager.HandleStreamEventWithPKSplit(&model.RowChange{Table: tbl, Op: model.OpDelete, Key: []model.Value{"c"}}, model.Pos{})
		s.Manager.FinishReading(c, nil, []*model.RowChange{
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{"b"}, Cols: []model.ColValue{{Name: "k", Value: "b"}}},
			{Table: tbl, Op: model.OpInsert, Key: []model.Value{"c"}, Cols: []model.ColValue{{Name: "k", Value: "c"}}},
		})
		if err := s.FlushChunk(context.Background(), nil, c, tbl, "k", []string{"k"}); err != nil {
			t.Fatal(err)
		}
		if exact {
			if ranges != 0 || exacts != 1 {
				t.Fatalf("exact table: range flushes %d, exact flushes %d", ranges, exacts)
			}
			if len(rows) != 1 || rows[0].Key[0] != "B" {
				t.Fatalf("exact flush rows = %v, want only the renamed image \"B\"", rows)
			}
			if len(dels) != 1 || dels[0].V[0] != "c" {
				t.Fatalf("exact flush deletes = %+v, want the tombstone \"c\"", dels)
			}
		} else if ranges != 1 || exacts != 0 {
			t.Fatalf("same-order table: range flushes %d, exact flushes %d", ranges, exacts)
		}
		if c.State != StateDone {
			t.Fatalf("chunk state %s after flush", c.State)
		}
	}
}

// --- weigher theo lô: driver giả trả weight "đơn vị 2 byte" (byte hoa, 0x01) cho mỗi byte của giá trị.

type g2WeighDriver struct{ queries, values *atomic.Int64 }

func (d g2WeighDriver) Open(string) (driver.Conn, error) { return g2WeighConn(d), nil }

type g2WeighConn g2WeighDriver

func (g2WeighConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (g2WeighConn) Close() error                        { return nil }
func (g2WeighConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c g2WeighConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Count(q, "WEIGHT_STRING(") != len(args) {
		return nil, fmt.Errorf("query has %d WEIGHT_STRING for %d args", strings.Count(q, "WEIGHT_STRING("), len(args))
	}
	c.queries.Add(1)
	c.values.Add(int64(len(args)))
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		s := a.Value.(string)
		if s == "NULL!" {
			continue
		}
		w := make([]byte, 0, 2*len(s))
		for _, b := range []byte(strings.ToUpper(s)) {
			w = append(w, b, 0x01)
		}
		vals[i] = w
	}
	return &g2Rows{vals: vals}, nil
}

type g2Rows struct {
	vals []driver.Value
	done bool
}

func (r *g2Rows) Columns() []string {
	out := make([]string, len(r.vals))
	for i := range out {
		out[i] = fmt.Sprintf("w%d", i)
	}
	return out
}
func (r *g2Rows) Close() error { return nil }
func (r *g2Rows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.vals)
	return nil
}

var g2Queries, g2Values atomic.Int64

func init() { sql.Register("g2weigh", g2WeighDriver{queries: &g2Queries, values: &g2Values}) }

func TestG2_WeighManyBatchesAndMatchesSingleWeigh(t *testing.T) {
	db, err := sql.Open("g2weigh", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, pad := range []bool{false, true} {
		col := PKColumn{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci", PadSpace: pad}
		var vals []model.Value
		for i := 0; i < 300; i++ {
			vals = append(vals, fmt.Sprintf("v%03d", i))
		}
		vals = append(vals, "a", "A", "a ", "a", "", " ")
		batch := newMySQLWeigherState(db)
		q0 := g2Queries.Load()
		if err := batch.weighMany(col, vals); err != nil {
			t.Fatal(err)
		}
		// 305 giá trị khác nhau ("a" lặp) → ⌈305/128⌉ = 3 truy vấn (+1 weight của space khi PAD SPACE).
		want := int64(3)
		if pad {
			want++
		}
		if got := g2Queries.Load() - q0; got != want {
			t.Fatalf("pad=%v: weighMany ran %d queries, want %d", pad, got, want)
		}
		q1 := g2Queries.Load()
		single := newMySQLWeigherState(db)
		for _, v := range vals {
			a, err := batch.weigh(col, v)
			if err != nil {
				t.Fatal(err)
			}
			b, err := single.weigh(col, v)
			if err != nil {
				t.Fatal(err)
			}
			if string(a) != string(b) {
				t.Fatalf("pad=%v: weight of %q: batch %x, single %x", pad, v, a, b)
			}
		}
		if got := g2Queries.Load() - q1; got == 0 {
			t.Fatal("single weigher did not query (test is vacuous)")
		}
		wa, _ := batch.weigh(col, "a")
		wsp, _ := batch.weigh(col, "a ")
		if pad != (string(wa) == string(wsp)) {
			t.Fatalf("pad=%v: weight('a') == weight('a ') is %v", pad, string(wa) == string(wsp))
		}
		// Codec: EncodeMany dùng lô rồi Encode trúng cache.
		codec := &PKCodec{Cols: []PKColumn{col}, Weigh: batch.weigh, WeighMany: batch.weighMany}
		q2 := g2Queries.Load()
		tuples := [][]model.Value{{"x1"}, {"x2"}, {"X1"}}
		ks, err := codec.EncodeMany(tuples)
		if err != nil {
			t.Fatal(err)
		}
		if got := g2Queries.Load() - q2; got != 1 {
			t.Fatalf("EncodeMany ran %d queries, want 1", got)
		}
		if ks[0] != ks[2] || ks[0] == ks[1] {
			t.Fatalf("EncodeMany keys %q", ks)
		}
		if err := batch.weighMany(col, []model.Value{"NULL!"}); err == nil {
			t.Fatal("NULL weight accepted")
		}
	}
}

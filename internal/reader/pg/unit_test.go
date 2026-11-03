package pg

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"strand/internal/model"
)

// buildMsg dựng message two-phase nhị phân theo đúng định dạng của pgoutput.
func buildMsg(tag byte, flags bool, lsns []uint64, times []int64, xid uint32, gid string) []byte {
	b := []byte{tag}
	if flags {
		b = append(b, 0)
	}
	for _, l := range lsns {
		b = binary.BigEndian.AppendUint64(b, l)
	}
	for _, t := range times {
		b = binary.BigEndian.AppendUint64(b, uint64(t))
	}
	b = binary.BigEndian.AppendUint32(b, xid)
	return append(append(b, gid...), 0)
}

func TestParsePrepareMsg(t *testing.T) {
	ts := int64(1_000_000) // 1s sau epoch PG
	cases := []struct {
		name string
		data []byte
		lsn  uint64
		end  uint64
	}{
		{"begin_prepare", buildMsg('b', false, []uint64{10, 20}, []int64{ts}, 7, "g1"), 10, 20},
		{"prepare", buildMsg('P', true, []uint64{11, 21}, []int64{ts}, 7, "g1"), 11, 21},
		{"stream_prepare", buildMsg('p', true, []uint64{12, 22}, []int64{ts}, 7, "g1"), 12, 22},
		{"commit_prepared", buildMsg('K', true, []uint64{13, 23}, []int64{ts}, 7, "g1"), 13, 23},
		{"rollback_prepared", buildMsg('r', true, []uint64{14, 24}, []int64{0, ts}, 7, "g1"), 14, 24},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := parsePrepareMsg(c.data)
			if err != nil {
				t.Fatal(err)
			}
			if uint64(m.LSN) != c.lsn || uint64(m.EndLSN) != c.end || m.Xid != 7 || m.GID != "g1" {
				t.Fatalf("giải mã sai: %+v", m)
			}
			if !m.Time.Equal(pgEpoch.Add(time.Second)) {
				t.Fatalf("thời gian sai: %v", m.Time)
			}
			if _, err := parsePrepareMsg(c.data[:len(c.data)-4]); err == nil {
				t.Fatal("message cụt phải báo lỗi")
			}
		})
	}
}

func TestFormatColValue(t *testing.T) {
	rc := &pglogrepl.RelationMessageColumn{Name: "c", DataType: TimestampOID}
	toast, err := formatColValue(&pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}, rc)
	if err != nil || toast != (model.ColValue{Name: "c", Unchanged: true}) {
		t.Fatalf("TOAST không đổi phải là ColValue{Unchanged:true}: %#v %v", toast, err)
	}
	if v, err := formatColValue(&pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}, rc); v != nil || err != nil {
		t.Fatalf("NULL phải là nil: %#v %v", v, err)
	}
	if _, err := formatColValue(&pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeBinary, Data: []byte{1}}, rc); err == nil {
		t.Fatal("binary phải báo lỗi")
	}

	ok := []struct {
		oid  uint32
		in   string
		want any
	}{
		{BoolOID, "t", true},
		{BoolOID, "f", false},
		{Int8OID, "-9223372036854775808", int64(-9223372036854775808)},
		{Float8OID, "NaN", nil}, // kiểm riêng bên dưới
		{Float8OID, "-Infinity", nil},
		{NumericOID, "NaN", "NaN"},
		{NumericOID, "0.10000000000000000000000001", "0.10000000000000000000000001"},
		{DateOID, "infinity", "infinity"},
		{DateOID, "0044-03-15 BC", "0044-03-15 BC"},
		{TimestampOID, "2026-11-03 13:37:49", time.Date(2026, 11, 3, 13, 37, 49, 0, time.UTC)},
		{TimestamptzOID, "2026-11-03 13:37:49.25+05:30", time.Date(2026, 11, 3, 8, 7, 49, 250000000, time.UTC)},
		{TimestamptzOID, "1900-01-01 00:00:00+06:42:04", time.Date(1899, 12, 31, 17, 17, 56, 0, time.UTC)},
		{IntervalOID, "1 day 02:00:00", "1 day 02:00:00"},
		{99999, "enum_val", "enum_val"},
	}
	for _, c := range ok {
		got, err := parseTextValue(c.oid, c.in)
		if err != nil {
			t.Fatalf("%d %q: %v", c.oid, c.in, err)
		}
		if c.want == nil {
			if f, isF := got.(float64); !isF || (f == f && f > -1e308) {
				t.Fatalf("%q phải là NaN/-Inf float64, được %#v", c.in, got)
			}
			continue
		}
		if wt, isT := c.want.(time.Time); isT {
			if gt, _ := got.(time.Time); !gt.Equal(wt) {
				t.Fatalf("%q: muốn %v được %v", c.in, wt, got)
			}
			continue
		}
		if got != c.want {
			t.Fatalf("%d %q: muốn %#v được %#v", c.oid, c.in, c.want, got)
		}
	}
	b, err := parseTextValue(ByteaOID, `\x00ff`)
	if err != nil || string(b.([]byte)) != "\x00\xff" {
		t.Fatalf("bytea: %#v %v", b, err)
	}
	for _, bad := range []struct {
		oid uint32
		in  string
	}{{BoolOID, "x"}, {Int4OID, "1.5"}, {DateOID, "03/11/2026"}} {
		if _, err := parseTextValue(bad.oid, bad.in); err == nil {
			t.Fatalf("%q phải báo lỗi", bad.in)
		}
	}
}

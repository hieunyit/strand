package spool

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"strand/internal/model"
)

func testIdentity() model.ArchiveIdentity {
	return model.ArchiveIdentity{Table: model.TableID{Schema: "app", Table: "items"}, Mode: "unique", Columns: []string{"a", "b"}, KeyColumns: []string{"b"}}
}
func TestPrefetchIdentityCodecRoundTrip(t *testing.T) {
	for _, origin := range []string{"archive", "source"} {
		for _, spilled := range []bool{false, true} {
			txn := &model.Txn{PrefetchOrigin: origin, ArchiveIdentities: []model.ArchiveIdentity{testIdentity()}, SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "00000000-0000-0000-0000-000000000001:1"}}
			if spilled {
				txn.SpillPath = "borrowed.spill"
				txn.RowCount = 1
				txn.SpilledTables = []model.TableID{testIdentity().Table}
			} else {
				txn.Rows = []model.RowChange{{Table: testIdentity().Table, Op: model.OpDelete, Key: []model.Value{int64(11)}}}
			}
			data, err := encodeTxn(txn)
			if err != nil {
				t.Fatal(err)
			}
			if string(data[:4]) != txnMagicV4 {
				t.Fatal("prefetch identity not durably tagged")
			}
			got, err := decodeTxn(data)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, txn) {
				t.Fatalf("prefetch identity lost across codec: got=%+v want=%+v", got, txn)
			}
		}
	}
}
func TestPrefetchCodecKeepsLegacyFormats(t *testing.T) {
	for _, pos := range []model.Pos{{Engine: model.EngineMySQL, GTID: "g:1"}, {Engine: model.EngineMySQL, File: "mysql-bin.1", BinlogPos: 10}} {
		txn := &model.Txn{SourcePos: pos}
		data, err := encodeTxn(txn)
		if err != nil {
			t.Fatal(err)
		}
		want := txnMagicV2
		if pos.File != "" {
			want = txnMagicV3
		}
		if string(data[:4]) != want {
			t.Fatalf("legacy generic spool encoding changed: %s", data[:4])
		}
		got, err := decodeTxn(data)
		if err != nil || !reflect.DeepEqual(got, txn) {
			t.Fatalf("legacy roundtrip: %+v %v", got, err)
		}
	}
}
func TestPrefetchCodecRefusesMissingOrMalformedContracts(t *testing.T) {
	for _, txn := range []*model.Txn{
		{PrefetchOrigin: "archive", Rows: []model.RowChange{{Op: model.OpDelete}}},
		{PrefetchOrigin: "source", Rows: []model.RowChange{{Op: model.OpDelete}}},
		{PrefetchOrigin: "unverified"},
		{ArchiveIdentities: []model.ArchiveIdentity{testIdentity()}},
		{PrefetchOrigin: "archive", ArchiveIdentities: []model.ArchiveIdentity{{Mode: "unique"}}},
	} {
		if _, err := encodeTxn(txn); err == nil {
			t.Fatalf("accepted malformed provenance: %+v", txn)
		}
	}
	raw := append([]byte(txnMagicV4), make([]byte, 4)...)
	binary.LittleEndian.PutUint32(raw[4:], maxArchiveIdentityBytes+1)
	if _, err := decodeTxn(raw); err == nil {
		t.Fatal("oversize identity header accepted")
	}
	raw = append([]byte(txnMagicV4), make([]byte, 4)...)
	raw = append(raw, []byte(txnMagicV4)...)
	if _, err := decodeTxn(raw); err == nil || !strings.Contains(err.Error(), "decode prefetch") {
		t.Fatalf("malformed JSON header accepted: %v", err)
	}
}

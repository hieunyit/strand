package model

import (
	"bytes"
	"testing"
)

// invariant: CO-1 — các giá trị collation _ci/_ai/PAD SPACE coi là bằng nhau phải cùng khóa chuẩn.
func TestFoldStringIsCoarserThanCommonCollations(t *testing.T) {
	eq := [][2]string{
		{"a", "A"}, {"café", "CAFE"}, {"straße", "STRASSE"}, {"ß", "s"}, {"abc", "abc   "},
		{"Ǆ", "ǆ"}, {"K", "K"}, {"ﬁ", "fi"}, {"Ｗ", "w"}, {"a​b", "ab"}, {"Æon", "aeon"},
		{"Đà Nẵng", "da nang"},
	}
	for _, p := range eq {
		if foldString(p[0]) != foldString(p[1]) {
			t.Errorf("fold(%q)=%q != fold(%q)=%q", p[0], foldString(p[0]), p[1], foldString(p[1]))
		}
	}
	if canonKey([]Value{"10"}) != canonKey([]Value{int64(10)}) || canonKey([]Value{[]byte("Ab")}) != canonKey([]Value{"ab"}) {
		t.Error("same value via different Go types must canonicalise equally")
	}
	if foldString("abc") == foldString("abd") || canonKey([]Value{"x y", "z"}) == canonKey([]Value{"x", "y z"}) {
		t.Error("distinct keys must stay distinct")
	}
}

// ForeignKey.ParentIndex phải sống qua spill (nếu mất, row con bị hash về namespace PK — R11).
func TestSpillRoundTripKeepsForeignKeyParentIndex(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSpillWriter(&buf)
	p := TableID{Schema: "app", Table: "parent"}
	in := []*RowChange{
		{Table: TableID{Schema: "app", Table: "child"}, Op: OpInsert, Key: []Value{int64(1)},
			ForeignKeys: []ForeignKey{{ParentTable: p, ParentIndex: "uk_code", ParentKey: []Value{"ABC"}}, {ParentTable: p, ParentKey: []Value{int64(7)}}}},
		{Table: TableID{Schema: "app", Table: "child"}, Op: OpInsert, Key: []Value{int64(2)},
			ForeignKeys: []ForeignKey{{ParentTable: p, ParentKey: []Value{int64(8)}}}},
	}
	for _, rc := range in {
		if err := sw.Write(rc); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}
	sr := NewSpillReader(&buf)
	for i, want := range in {
		got, err := sr.Next()
		if err != nil {
			t.Fatal(err)
		}
		for j := range want.ForeignKeys {
			if got.ForeignKeys[j].ParentIndex != want.ForeignKeys[j].ParentIndex || got.ForeignKeys[j].ParentTable != p {
				t.Fatalf("row %d fk %d = %+v, want %+v", i, j, got.ForeignKeys[j], want.ForeignKeys[j])
			}
		}
	}
}

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

// review G4: kana và ký tự ignorable/điều khiển mà utf8mb4_0900_ai_ci / collation ja coi là bằng nhau ở
// mức primary phải cùng bộ xương.
func TestG4_FoldStringKanaAndIgnorables(t *testing.T) {
	eq := [][2]string{
		{"カタカナ", "かたかな"},             // katakana ↔ hiragana
		{"ｶﾀｶﾅ", "かたかな"},             // katakana nửa độ rộng
		{"ガギグ", "かきく"},               // dấu kêu (secondary) bị bỏ
		{"ﾊﾞﾊﾟ", "はは"},               // dấu kêu nửa độ rộng
		{"ァィゥェォッャュョヮ", "あいうえおつやゆよわ"}, // kana nhỏ
		{"ヵヶ", "かけ"}, {"ㇰㇳ", "くと"},
		{"カー", "かあ"}, {"ｶｰ", "かあ"}, {"コーヒー", "こおひい"}, {"ラーメン", "らあめん"}, // dấu kéo dài
		{"いすゞ", "いすす"}, {"コヽロ", "こころ"}, // dấu lặp
		{"ab", "a\x00b"}, {"ab", "a\x07b"}, {"a\tb", "ab"}, {"ab\n", "ab"}, // Cc
		{"ab", "aㅤb"}, {"ab", "aﾠb"}, {"ab", "a\U000E0100b"}, {"ab", "a͏b"}, // default-ignorable
	}
	for _, p := range eq {
		if foldString(p[0]) != foldString(p[1]) {
			t.Errorf("fold(%q)=%q != fold(%q)=%q", p[0], foldString(p[0]), p[1], foldString(p[1]))
		}
	}
	for _, p := range [][2]string{{"かた", "かな"}, {"カー", "かい"}, {"ab", "a b"}} {
		if foldString(p[0]) == foldString(p[1]) {
			t.Errorf("fold(%q) == fold(%q): distinct keys must stay distinct", p[0], p[1])
		}
	}
}

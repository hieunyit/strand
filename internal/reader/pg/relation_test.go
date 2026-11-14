package pg

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"strand/internal/model"
	"strand/internal/reader"
)

func TestRelationDiffers(t *testing.T) {
	baseMeta := []ColumnMeta{
		{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 0},
		{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
	}

	makeRel := func(cols ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
		return &pglogrepl.RelationMessage{
			RelationID:   12345,
			Namespace:    "public",
			RelationName: "users",
			Columns:      cols,
		}
	}

	baseRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
	)

	// 1. Identical
	if relationDiffers(baseMeta, baseRel) {
		t.Fatal("identical relation must not differ")
	}

	// 2. Add column
	addRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "extra", DataType: TextOID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, addRel) {
		t.Fatal("added column must differ")
	}

	// 3. Drop column
	dropRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, dropRel) {
		t.Fatal("dropped column must differ")
	}

	// 4. Rename column
	renameRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "full_name", DataType: TextOID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, renameRel) {
		t.Fatal("renamed column must differ")
	}

	// 5. Change type OID (e.g. Int4 -> Int8)
	typeRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int8OID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, typeRel) {
		t.Fatal("changed type OID must differ")
	}

	// 6. Change type modifier (e.g. varchar(20) -> varchar(100))
	modRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: 104, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, modRel) {
		t.Fatal("changed typmod must differ")
	}

	// 7. Change PK flags
	flagRel := makeRel(
		&pglogrepl.RelationMessageColumn{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 0},
		&pglogrepl.RelationMessageColumn{Name: "name", DataType: TextOID, TypeModifier: -1, Flags: 1},
		&pglogrepl.RelationMessageColumn{Name: "age", DataType: Int4OID, TypeModifier: -1, Flags: 0},
	)
	if !relationDiffers(baseMeta, flagRel) {
		t.Fatal("changed flags must differ")
	}
}

func TestStreamState_TableMapSchemaChange(t *testing.T) {
	meta := []ColumnMeta{
		{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
		{Name: "val", DataType: TextOID, TypeModifier: -1, Flags: 0},
	}

	cfg := Config{
		TableSchema: func(schema, table string) ([]ColumnMeta, bool) {
			if schema == "public" && table == "t" {
				return meta, true
			}
			return nil, false
		},
	}
	r := &Reader{cfg: cfg}

	out := make(chan reader.Event, 16)
	st := &streamState{
		r:         r,
		ctx:       context.Background(),
		out:       out,
		rels:      make(map[uint32]*relState),
		announced: make(map[uint32]uint64),
		cur: &openTxn{
			pos: model.Pos{Engine: model.EnginePG, LSN: 100},
			ts:  time.Now(),
			xid: 1,
		},
	}

	// 1. Initial Relation matches current metadata
	relMsg := &pglogrepl.RelationMessage{
		RelationID:   1001,
		Namespace:    "public",
		RelationName: "t",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
			{Name: "val", DataType: TextOID, TypeModifier: -1, Flags: 0},
		},
	}
	st.relSeq++
	st.rels[1001] = &relState{msg: relMsg, ver: st.relSeq}

	// Emit row -> tableMapEvent should have SchemaChanged = false
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("hello")},
		},
	}
	if err := st.row(1, 1001, model.OpInsert, nil, tuple); err != nil {
		t.Fatalf("row failed: %v", err)
	}

	ev1 := <-out
	if ev1.Type != reader.EventTableMap {
		t.Fatalf("want EventTableMap, got %v", ev1.Type)
	}
	if ev1.SchemaChanged {
		t.Fatal("expected SchemaChanged=false for identical schema")
	}
	ev2 := <-out
	if ev2.Type != reader.EventRows {
		t.Fatalf("want EventRows, got %v", ev2.Type)
	}

	// 2. New Relation arrives with added column "extra"
	relMsg2 := &pglogrepl.RelationMessage{
		RelationID:   1001,
		Namespace:    "public",
		RelationName: "t",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: Int8OID, TypeModifier: -1, Flags: 1},
			{Name: "val", DataType: TextOID, TypeModifier: -1, Flags: 0},
			{Name: "extra", DataType: Int4OID, TypeModifier: -1, Flags: 0},
		},
	}
	st.relSeq++
	st.rels[1001] = &relState{msg: relMsg2, ver: st.relSeq}

	tuple2 := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("2")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("world")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
		},
	}
	if err := st.row(1, 1001, model.OpInsert, nil, tuple2); err != nil {
		t.Fatalf("row failed: %v", err)
	}

	ev3 := <-out
	if ev3.Type != reader.EventTableMap {
		t.Fatalf("want EventTableMap, got %v", ev3.Type)
	}
	if !ev3.SchemaChanged {
		t.Fatal("expected SchemaChanged=true for changed schema")
	}
	ev4 := <-out
	if ev4.Type != reader.EventRows {
		t.Fatalf("want EventRows, got %v", ev4.Type)
	}
}

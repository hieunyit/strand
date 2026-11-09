package schemagen

import (
	"context"
	"strand/internal/model"
	"strings"
	"testing"
)

// pg_get_constraintdef được phép bỏ schema của bảng visible theo search_path, dù bảng con khác schema.
func TestReviewPGCrossSchemaFKUsesCatalogIdentity(t *testing.T) {
	db := schemaDB(t, true)
	execSchema(t, db, `CREATE SCHEMA child_schema`, `CREATE TABLE public.customers(id int PRIMARY KEY)`, `CREATE TABLE child_schema.orders(id int PRIMARY KEY,customer int REFERENCES public.customers(id))`)
	p, err := Build(context.Background(), Request{Source: db, Target: db, SourceKind: "postgres", TargetKind: "postgres", Tables: []model.TableID{{Schema: "public", Table: "customers"}, {Schema: "child_schema", Table: "orders"}}, Rename: map[string]string{"public.customers": "target_schema.customers", "child_schema.orders": "target_schema.orders"}})
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(p.Statements, "\n")
	if !strings.Contains(text, `REFERENCES "target_schema"."customers"`) {
		t.Fatalf("FK references wrong table:\n%s", text)
	}
	if err := p.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var parent string
	if err := db.QueryRow(`SELECT confrelid::regclass::text FROM pg_constraint WHERE conrelid='target_schema.orders'::regclass AND contype='f'`).Scan(&parent); err != nil || parent != "target_schema.customers" {
		t.Fatalf("parent=%s err=%v", parent, err)
	}
}

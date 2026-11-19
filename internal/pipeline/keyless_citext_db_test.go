package pipeline

import (
	"context"
	"strand/internal/config"
	"strand/internal/model"
	"strand/internal/rowkey"
	"strand/internal/validate"
	"strings"
	"testing"
)

// This test starts its own disposable PostgreSQL, and is selected explicitly.
func TestNK07_PostgresCitextDomainArrayGate(t *testing.T) {
	db, _ := startTestDB(t, "postgres")
	ctx := context.Background()
	for _, q := range []string{
		`CREATE EXTENSION citext`,
		`CREATE DOMAIN public.folded AS citext`,
		`CREATE DOMAIN public.folded_array AS public.folded[]`,
		`CREATE DOMAIN public.nested_folded AS public.folded_array[]`,
		`CREATE TABLE public.unsafe_images (a citext[], b public.folded_array, c public.nested_folded)`,
		`ALTER TABLE public.unsafe_images REPLICA IDENTITY FULL`,
		`CREATE TABLE public.safe_images (a text[], b integer[])`,
		`ALTER TABLE public.safe_images REPLICA IDENTITY FULL`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	metas, err := loadSourceTablesPG(ctx, db, &tableFilter{include: []string{"public.*_images"}})
	if err != nil {
		t.Fatal(err)
	}
	unsafe := model.TableID{Schema: "public", Table: "unsafe_images"}
	safe := model.TableID{Schema: "public", Table: "safe_images"}
	for _, name := range []string{"a", "b", "c"} {
		if typ := metas[unsafe].ColTypes[name]; rowkey.KeylessColumnProblem(typ, true, false) == "" {
			t.Fatalf("nested citext equality accepted for %s (%s)", name, typ)
		}
	}
	if metas[safe].ColTypes["b"] != "_int4" {
		t.Fatalf("integer array misclassified as numeric scalar: %v", metas[safe].ColTypes)
	}
	p := &Pipeline{cfg: &config.Config{}, srcEngine: model.EnginePG, dstEngine: model.EnginePG, dst: db, filter: &tableFilter{}}
	p.cfg.Tables.Keyless = "full_row"
	p.markKeyless(metas)
	if metas[unsafe].Keyless || !metas[safe].Keyless {
		t.Fatalf("source gate unsafe=%+v safe=%+v", metas[unsafe], metas[safe])
	}
	if err := p.checkKeylessTargetCollations(ctx, unsafe, &tableMeta{Keyless: true}); err == nil || !strings.Contains(err.Error(), "citext") {
		t.Fatalf("target accepted nested citext equality: %v", err)
	}
	if err := p.checkKeylessTargetCollations(ctx, safe, &tableMeta{Keyless: true}); err != nil {
		t.Fatal(err)
	}
	_, _, err = validate.CheckPGRowKeyOpts(ctx, db, []model.TableID{unsafe}, validate.KeylessOpts{On: true, Seeded: func(string) bool { return true }})
	if err == nil || !strings.Contains(err.Error(), "citext") {
		t.Fatalf("validate accepted nested citext: %v", err)
	}
}

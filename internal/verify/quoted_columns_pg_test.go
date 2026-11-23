package verify_test

import (
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

func TestPGVerifyQuotedColumnsDifferingOnlyByCase(t *testing.T) {
	db := getPostgres(t)
	src := model.TableID{Schema: "public", Table: "quoted_verify_src"}
	dst := model.TableID{Schema: "public", Table: "quoted_verify_dst"}
	for _, table := range []model.TableID{src, dst} {
		q := verify.QuoteTable(model.EnginePG, table)
		mustExec(t, db, "DROP TABLE IF EXISTS "+q)
		mustExec(t, db, "CREATE TABLE "+q+` ("V" int, "v" int)`)
		mustExec(t, db, "INSERT INTO "+q+" VALUES (1,1)")
		t.Cleanup(func() { mustExec(t, db, "DROP TABLE "+q) })
	}

	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{
		Source: db, Target: db, SourceEngine: model.EnginePG, TargetEngine: model.EnginePG,
		TargetName: func(model.TableID) model.TableID { return dst },
	})
	checkMatch := func(want bool) {
		t.Helper()
		report, err := mv.VerifyTableKeyless(t.Context(), src)
		if err != nil || report.Match != want || !report.StableOnSource {
			t.Fatalf("keyless verify match=%v: report=%+v err=%v", want, report, err)
		}
	}
	checkMatch(true)
	mustExec(t, db, `UPDATE public.quoted_verify_dst SET "V" = 9`)
	checkMatch(false)
	mustExec(t, db, `ALTER TABLE public.quoted_verify_src ADD PRIMARY KEY ("V")`)
	cols, err := verify.GetTableColumns(t.Context(), db, model.EnginePG, src, "V")
	if err != nil || len(cols) != 2 || cols[0].Name != "V" || cols[1].Name != "v" {
		t.Fatalf("columns with quoted primary key: %+v, err=%v", cols, err)
	}
}

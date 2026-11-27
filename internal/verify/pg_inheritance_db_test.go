package verify_test

import (
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

// Review DBA: full verify PG đọc bảng cha inheritance thường kèm row bảng con, nên hai phía có cùng đa tập
// cha + con nhưng hoán đổi row giữa cha và con lại "khớp". Verify phải đọc như writer: chỉ chính relation.
// Gốc partition khai báo vẫn gồm mọi partition.
func TestPGVerifyFullScopesInheritanceParent(t *testing.T) {
	db := getPostgres(t)
	ctx := t.Context()
	for _, s := range []string{"inh_src", "inh_dst"} {
		mustExec(t, db, "DROP SCHEMA IF EXISTS "+s+" CASCADE")
		mustExec(t, db, "CREATE SCHEMA "+s)
		mustExec(t, db, "CREATE TABLE "+s+".p (id bigint PRIMARY KEY, v text)")
		mustExec(t, db, "CREATE TABLE "+s+".c () INHERITS ("+s+".p)")
		mustExec(t, db, "CREATE TABLE "+s+".parted (id bigint PRIMARY KEY, v text) PARTITION BY RANGE (id)")
		mustExec(t, db, "CREATE TABLE "+s+".parted_lo PARTITION OF "+s+".parted FOR VALUES FROM (0) TO (100)")
		mustExec(t, db, "CREATE TABLE "+s+".parted_hi PARTITION OF "+s+".parted FOR VALUES FROM (100) TO (1000)")
		mustExec(t, db, "INSERT INTO "+s+".parted VALUES (1,'a'),(150,'b')")
		s := s
		t.Cleanup(func() { mustExec(t, db, "DROP SCHEMA "+s+" CASCADE") })
	}
	// Cha khác nhau (src: id 1, dst: id 2), cha + con giống nhau.
	mustExec(t, db, "INSERT INTO inh_src.p VALUES (1,'a')")
	mustExec(t, db, "INSERT INTO inh_src.c VALUES (2,'b')")
	mustExec(t, db, "INSERT INTO inh_dst.p VALUES (2,'b')")
	mustExec(t, db, "INSERT INTO inh_dst.c VALUES (1,'a')")

	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{
		Source: db, Target: db, SourceEngine: model.EnginePG, TargetEngine: model.EnginePG,
		PipelineID: "inh",
		TargetName: func(id model.TableID) model.TableID { return model.TableID{Schema: "inh_dst", Table: id.Table} },
	})
	rep, err := mv.VerifyTableFull(ctx, model.TableID{Schema: "inh_src", Table: "p"}, "id")
	if err != nil {
		t.Fatal(err)
	}
	if rep.MismatchedChunks == 0 {
		t.Fatalf("inheritance parent differs (rows swapped with the child) but full verify matched: %+v", rep)
	}
	rep, err = mv.VerifyTableFull(ctx, model.TableID{Schema: "inh_src", Table: "parted"}, "id")
	if err != nil || rep.MismatchedChunks != 0 {
		t.Fatalf("declarative partition root must include its partitions and match: rep=%+v err=%v", rep, err)
	}
	mustExec(t, db, "UPDATE inh_dst.parted SET v = 'x' WHERE id = 150")
	rep, err = mv.VerifyTableFull(ctx, model.TableID{Schema: "inh_src", Table: "parted"}, "id")
	if err != nil || rep.MismatchedChunks == 0 {
		t.Fatalf("a changed row in a partition must mismatch: rep=%+v err=%v", rep, err)
	}
	// Keyless (đếm và tổng hash cả bảng) cũng chỉ đọc chính relation.
	krep, err := mv.VerifyTableKeyless(ctx, model.TableID{Schema: "inh_src", Table: "p"})
	if err != nil || krep.Match {
		t.Fatalf("keyless verify of the inheritance parent must mismatch: rep=%+v err=%v", krep, err)
	}
}

package verify_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

// invariant: verify không báo sạch sai — review R03. Hai row khác nhau mà nối cột bằng dấu phân cách
// (0x1f) thì trùng ảnh: so khớp row phải thấy khác.
func TestR03_LiveRowCompareIsUnambiguous(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	tid := model.TableID{Schema: "verify_src", Table: "r03_delim"}
	for _, db := range []*sql.DB{src, dst} {
		mustExec(t, db, "CREATE TABLE r03_delim(id BIGINT PRIMARY KEY, a TEXT, b TEXT)")
	}
	mustExec(t, src, "INSERT INTO r03_delim VALUES (1, ?, ?), (2, ?, ?)", "x\x1fy", "z", "\x00NULL", nil)
	mustExec(t, dst, "INSERT INTO r03_delim VALUES (1, ?, ?), (2, ?, ?)", "x", "y\x1fz", nil, "\x00NULL")
	lv := verify.NewLiveVerifier(verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: "r03", TargetName: toDst, SkipApplyWait: true})
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: dst, Meta: dst, PipelineID: "r03", TargetName: toDst,
		SourceEngine: model.EngineMySQL, TargetEngine: model.EngineMySQL, LiveVerifier: lv})
	rep, err := mv.VerifyTableFull(ctx, tid, "id")
	if err != nil {
		t.Fatal(err)
	}
	if rep.MismatchedChunks == 0 || len(rep.Mismatches) != 2 {
		t.Fatalf("mismatched_chunks=%d mismatches=%v: rows that differ only in column boundaries must be reported", rep.MismatchedChunks, rep.Mismatches)
	}
}

// review R12: lượt full verify chạy hết thì --resume sau đó không được tin hash cũ; chỉ lượt bị ngắt
// mới chạy tiếp, và chunk từng lệch luôn được hash lại.
func TestR12_ResumeOnlyContinuesAnInterruptedRun(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	tid := model.TableID{Schema: "verify_src", Table: "r12_resume"}
	for _, db := range []*sql.DB{src, dst} {
		mustExec(t, db, "CREATE TABLE r12_resume(id BIGINT PRIMARY KEY, v INT)")
		mustExec(t, db, "INSERT INTO r12_resume VALUES (1,10),(2,20),(3,30),(4,40)")
	}
	cfg := verify.FullVerifyConfig{Source: src, Target: dst, Meta: dst, PipelineID: "r12", TargetName: toDst,
		SourceEngine: model.EngineMySQL, TargetEngine: model.EngineMySQL, ChunkRows: 1}
	if _, err := verify.NewMerkleVerifier(cfg).VerifyTableFull(ctx, tid, "id"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, dst, "UPDATE r12_resume SET v = 999 WHERE id = 1")
	cfg.Resume = true
	rep, err := verify.NewMerkleVerifier(cfg).VerifyTableFull(ctx, tid, "id")
	if err != nil {
		t.Fatal(err)
	}
	if rep.ResumedChunks != 0 || len(rep.Mismatches) != 1 {
		t.Fatalf("after a completed run: resumed=%d mismatches=%v, want a fresh run that sees id=1", rep.ResumedChunks, rep.Mismatches)
	}

	// Lượt bị ngắt sau 2 chunk rồi chạy tiếp: 2 chunk đầu (id=1 vẫn lệch → hash lại; id=2 khớp → dùng lại).
	cfg.Resume, cfg.StopAfter = false, 2
	if _, err := verify.NewMerkleVerifier(cfg).VerifyTableFull(ctx, tid, "id"); !errors.Is(err, verify.ErrVerifyInterrupted) {
		t.Fatalf("interrupted run: %v", err)
	}
	cfg.Resume, cfg.StopAfter = true, 0
	rep, err = verify.NewMerkleVerifier(cfg).VerifyTableFull(ctx, tid, "id")
	if err != nil {
		t.Fatal(err)
	}
	if rep.ResumedChunks != 1 || len(rep.Mismatches) != 1 {
		t.Fatalf("resume of interrupted run: resumed=%d hashed=%d mismatches=%v", rep.ResumedChunks, rep.HashedChunks, rep.Mismatches)
	}
}

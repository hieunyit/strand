package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// invariant: CP-5. Strict song song (target.strict_parallel: auto): các batch không chung khóa được ghi
// trên nhiều writer nhưng commit đúng thứ tự source, nên target luôn là một prefix của source. Source
// chèn id 1, 2, 3, ... tuần tự (khóa khác nhau: batch ghi song song); một bộ đọc trên target kiểm liên tục
// COUNT(*) = MAX(id) trong một snapshot nhất quán — commit sai thứ tự sẽ lộ ra thành lỗ hổng.
func TestSP_E2E_StrictParallelKeepsSourcePrefix(t *testing.T) {
	src, dst := newPair(t)
	for _, n := range []*mysqlNode{src, dst} {
		n.exec(t, "CREATE TABLE app.seqlog (id BIGINT PRIMARY KEY, pad VARCHAR(64) NOT NULL)")
	}
	// Thủ tục sinh tải tạo trước khi Strand bắt đầu đọc binlog (CREATE PROCEDURE là DDL ngoài pipeline).
	src.exec(t, `CREATE PROCEDURE app.fill(IN n BIGINT)
BEGIN
  DECLARE i BIGINT DEFAULT 1;
  WHILE i <= n DO
    INSERT INTO app.seqlog VALUES (i, REPEAT('x', 32));
    SET i = i + 1;
  END WHILE;
END`)
	cfg := strandCfg{Name: "e2e-strict-par", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.seqlog"}, Dir: testDir(t), CommitOrder: "strict", Workers: 8}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	if !strings.Contains(p.output.String(), "strict commit order with parallel writers") {
		t.Fatalf("strict parallel apply is not on\n%s", p.tail())
	}

	// Dừng Strand rồi tạo một khối tồn đọng: mỗi id một transaction, theo thứ tự id. Khi Strand đọc lại
	// khối này, writer luôn có việc nên các batch thật sự được ghi song song.
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	total := int64(envInt("STRAND_E2E_SP_ROWS", 60000))
	src.exec(t, fmt.Sprintf("CALL app.fill(%d)", total))
	p = startStrand(t, cfg.write(t))
	var gaps []string
	var samples, nonEmpty int
	deadline := time.Now().Add(5 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("target did not catch up in 5 minutes\n%s", p.tail())
		}
		var n, maxID sql.NullInt64
		tx, err := dst.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.QueryRow("SELECT COUNT(*), MAX(id) FROM app.seqlog").Scan(&n, &maxID)
		_ = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		samples++
		if maxID.Valid {
			nonEmpty++
			if n.Int64 != maxID.Int64 && len(gaps) < 20 {
				gaps = append(gaps, fmt.Sprintf("count=%d max=%d", n.Int64, maxID.Int64))
			}
		}
		if p.exited() {
			t.Fatalf("strand exited (code %d)\n%s", p.exitCode(), p.tail())
		}
		if n.Int64 == total {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(gaps) > 0 {
		t.Fatalf("target is not a prefix of the source in %d samples (CP-5): %s", len(gaps), strings.Join(gaps, "; "))
	}
	t.Logf("%d rows applied, %d target samples (%d non-empty), all prefixes", total, samples, nonEmpty)
	if nonEmpty < 20 {
		t.Fatalf("only %d non-empty samples during catch-up: the check did not observe the apply", nonEmpty)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
}

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Failover khẩn cấp khi source chết hẳn (P1-17, câu 3 = "gộp"): lần dừng exit 10 vì không fence
// được source hiển thị mức dữ liệu đã apply ở target; xác nhận bằng --source-fenced-externally
// cho phép tiếp tục promote và mở quyền ghi cho ứng dụng.
func TestP1_E2E_FailoverPromoteShowsLossAndContinues(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	dst.exec(t,
		"CREATE USER 'app'@'%' IDENTIFIED BY 'app'",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'app'@'%'",
		"GRANT SELECT ON strand.* TO 'app'@'%'",
		"SET GLOBAL read_only = ON")

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)
	wl.Start(4, 2000)

	cfg := strandCfg{
		Name:      "e2e-failover",
		SourceDSN: src.RootDSN,
		TargetDSN: dst.RootDSN + "strand",
		Include:   []string{"app.*"},
		Exclude:   []string{"app.ignored"},
		Dir:       testDir(t),
	}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)

	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}

	// Dừng hẳn container source để giả lập primary chết không thể kết nối.
	if err := src.c.Stop(context.Background(), nil); err != nil {
		t.Fatalf("stop source container: %v", err)
	}

	out, code := runStrandCLI(t, 3*time.Minute, "promote",
		"--target", dst.RootDSN+"strand",
		"--fence", src.RootDSN+"?timeout=3s",
		"--pipeline", cfg.Name)
	if code != 10 {
		t.Fatalf("promote unfenced: exit %d, want 10\n%s", code, out)
	}
	if !strings.Contains(out, "Dữ liệu sẽ mất nếu tiếp tục") || !strings.Contains(out, "target applied up to") {
		t.Fatalf("promote output missing failover loss summary:\n%s", out)
	}

	out, code = runStrandCLI(t, 3*time.Minute, "promote",
		"--target", dst.RootDSN+"strand",
		"--fence", src.RootDSN+"?timeout=3s",
		"--pipeline", cfg.Name,
		"--source-fenced-externally")
	if code != 0 {
		t.Fatalf("promote with --source-fenced-externally: exit %d, want 0\n%s", code, out)
	}

	appDB, err := sql.Open("mysql", fmt.Sprintf("app:app@tcp(%s)/app", dst.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer appDB.Close()
	if _, err := appDB.Exec("INSERT INTO app.events VALUES (-1, 1, 'failover-ok')"); err != nil {
		t.Fatalf("application could not write to app.events after failover promote: %v", err)
	}
}

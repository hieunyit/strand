package e2e

import (
	"strings"
	"testing"
	"time"
)

// P1 (02 §10): `strand plan` từ chối chế độ A khi ETA > 50% retention, trừ khi có --force.
func TestP1_E2E_PlanRejectsWhenETAExceedsRetention(t *testing.T) {
	src, dst := newPair(t)
	wl := newWorkload(src.DB, 1)
	wl.Preload(t, 20000)
	src.exec(t, "ANALYZE TABLE app.accounts, app.orders, app.events")

	ok := strandCfg{Name: "e2e-plan", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Dir: testDir(t), SourceIOPS: 100000}
	if out, code := runStrandCLI(t, time.Minute, "plan", "-c", ok.write(t)); code != 0 {
		t.Fatalf("plan with ample IOPS: exit %d\n%s", code, out)
	}
	src.exec(t, "SET GLOBAL binlog_expire_logs_seconds = 10")
	slow := strandCfg{Name: "e2e-plan", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Dir: testDir(t), SourceIOPS: 1}
	path := slow.write(t)
	out, code := runStrandCLI(t, time.Minute, "plan", "-c", path)
	if code != 3 || !strings.Contains(out, "bootstrap_retention_risk") {
		t.Fatalf("plan with ETA > 50%% retention: exit %d, want 3\n%s", code, out)
	}
	if out, code := runStrandCLI(t, time.Minute, "plan", "-c", path, "--force"); code != 0 {
		t.Fatalf("plan --force: exit %d\n%s", code, out)
	}
}

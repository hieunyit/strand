package e2e

// Các test bắt buộc của Gate 1 (01 §6) chạy end-to-end trên MySQL thật.

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func runStrandCLI(t *testing.T, timeout time.Duration, args ...string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, strandBin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return string(out), ee.ExitCode()
		}
		t.Fatalf("run strand %v: %v\n%s", args, err, out)
	}
	return string(out), 0
}

func waitSynced(t *testing.T, cfg *strandCfg, p *strandProc, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !pipelineSynced(cfg.Socket) {
		if p.exited() {
			t.Fatalf("strand exited (code %d) before synced\n%s", p.exitCode(), p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipeline not synced after %s\n%s", timeout, p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func newPair(t *testing.T) (*mysqlNode, *mysqlNode) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, src, dst)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	return src, dst
}

// Gate 1: hai instance cùng pipeline_id thì instance thứ hai phải từ chối khởi động (SG-1);
// instance cũ bị đóng băng rồi chạy lại sau khi instance mới nhận pipeline thì bị fence.
func TestG1_E2E_TwoInstancesSamePipeline(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)
	wl.Start(4, 2000)
	defer wl.Stop()

	cfgA := strandCfg{Name: "e2e-two", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	a := startStrand(t, cfgA.write(t))
	waitSynced(t, &cfgA, a, 3*time.Minute)

	cfgB := cfgA
	cfgB.Dir, cfgB.Socket = testDir(t), ""
	b := startStrand(t, cfgB.write(t))
	select {
	case <-b.done:
	case <-time.After(60 * time.Second):
		b.kill9()
		t.Fatalf("second instance did not refuse to start")
	}
	if code := b.exitCode(); code != 4 || !strings.Contains(b.output.String(), "pipeline_locked") {
		t.Fatalf("second instance: exit %d, want 4 pipeline_locked\n%s", code, b.tail())
	}
	t.Logf("second instance refused with exit 4 pipeline_locked")

	// Đóng băng A, mất kết nối giữ khóa (phân vùng mạng), B nhận pipeline với epoch mới.
	var holder sql.NullInt64
	if err := dst.DB.QueryRow("SELECT IS_USED_LOCK('strand:e2e-two')").Scan(&holder); err != nil || !holder.Valid {
		t.Fatalf("lock holder not found: %v", err)
	}
	_ = a.cmd.Process.Signal(syscall.SIGSTOP)
	dst.exec(t, fmt.Sprintf("KILL %d", holder.Int64))
	b = startStrand(t, cfgB.write(t))
	waitSynced(t, &cfgB, b, 5*time.Minute)
	_ = a.cmd.Process.Signal(syscall.SIGCONT)
	select {
	case <-a.done:
	case <-time.After(90 * time.Second):
		a.kill9()
		t.Fatalf("frozen old instance was not fenced after resuming")
	}
	// Instance mới đã ngắt kết nối target của instance cũ (transaction dở bị rollback). Khi chạy
	// lại, instance cũ hoặc bị fence (exit 4), hoặc gặp lỗi kết nối (exit 1, systemd restart) và
	// lần khởi động lại bị từ chối pipeline_locked (exit 4). Cả hai đều đúng 04 §4.
	switch code := a.exitCode(); code {
	case 4:
		t.Logf("old instance fenced with exit 4 after SIGCONT")
	case 1:
		a2 := startStrand(t, cfgA.write(t))
		select {
		case <-a2.done:
		case <-time.After(60 * time.Second):
			a2.kill9()
			t.Fatalf("restarted old instance did not refuse to start")
		}
		if c := a2.exitCode(); c != 4 || !strings.Contains(a2.output.String(), "pipeline_locked") {
			t.Fatalf("restarted old instance: exit %d, want 4 pipeline_locked\n%s", c, a2.tail())
		}
		t.Logf("old instance lost its connections (exit 1); its restart was refused with pipeline_locked")
	default:
		t.Fatalf("old instance: exit %d, want 4 or 1\n%s", code, a.tail())
	}

	wl.Stop()
	waitCaughtUp(t, src, dst, &cfgB, b, 5*time.Minute)
	if code := b.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, b.tail())
	}
	compareFinal(t, src, dst, appTables...)
}

// Gate 1: đổi bộ lọc giữa các lần restart, 0 lệch. Bảng được thêm vào thì snapshot; bảng bị
// loại rồi thêm lại thì phải snapshot lại (event của nó đã bị bỏ qua khi bị loại).
func TestG1_E2E_FilterChangeBetweenRestarts(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 3000)
	wl.Start(6, 3000)

	cfg := strandCfg{Name: "e2e-filter", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Dir: testDir(t)}
	steps := []struct {
		include []string
		exclude []string
	}{
		{[]string{"app.accounts", "app.orders"}, nil},
		{[]string{"app.*"}, []string{"app.ignored"}},
		{[]string{"app.accounts", "app.orders"}, nil},
		{[]string{"app.*"}, []string{"app.ignored"}},
	}
	var p *strandProc
	for i, st := range steps {
		cfg.Include, cfg.Exclude = st.include, st.exclude
		p = startStrand(t, cfg.write(t))
		waitSynced(t, &cfg, p, 3*time.Minute)
		time.Sleep(5 * time.Second)
		if i < len(steps)-1 {
			if code := p.stop(t); code != 0 {
				t.Fatalf("step %d exit %d\n%s", i, code, p.tail())
			}
			t.Logf("step %d: include=%v exclude=%v ok", i, st.include, st.exclude)
		}
	}
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
}

// pkMover đổi PK của app.events qua lại giữa các range (chunk 5.000 khóa), kể cả chuỗi
// đổi liên tiếp 1→2→3 trên cùng một row (P1-11).
func pkMover(db *sql.DB, seed int64, span int64, stop *atomic.Bool, wg *sync.WaitGroup, moves *atomic.Int64) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))
	for !stop.Load() {
		from := 1 + r.Int63n(span)
		to := 1 + r.Int63n(span*2)
		if _, err := db.Exec("UPDATE app.events SET id = ?, ver = UUID_SHORT() WHERE id = ?", to, from); err == nil {
			moves.Add(1)
			// Đổi tiếp ngay (to → to2) trong Txn kế.
			to2 := 1 + r.Int63n(span*2)
			if _, err := db.Exec("UPDATE app.events SET id = ?, ver = UUID_SHORT() WHERE id = ?", to2, to); err == nil {
				moves.Add(1)
			}
		}
	}
}

// Gate 1: `snapshot add` khi stream đang chạy, dưới tải cập nhật dày, với UPDATE đổi PK qua
// hai range khác trạng thái: 0 lệch, ver không giảm.
func TestG1_E2E_SnapshotAddUnderLoadWithPKChangeAcrossRanges(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	const span = 30000
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, span)
	wl.Start(6, span)

	cfg := strandCfg{Name: "e2e-snapadd", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.accounts", "app.orders"}, Dir: testDir(t)}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)

	var stop atomic.Bool
	var wg sync.WaitGroup
	var moves atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go pkMover(src.DB, seed+int64(i), span, &stop, &wg, &moves)
	}
	vs := startVerSampler(dst, appTables...)

	time.Sleep(2 * time.Second)
	out, code := runStrandCLI(t, 30*time.Second, "snapshot", "add", "--table", "app.events", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add: exit %d: %s", code, out)
	}
	time.Sleep(time.Second)
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(5 * time.Second)

	stop.Store(true)
	wg.Wait()
	wl.Stop()
	t.Logf("PK moves during snapshot add: %d", moves.Load())
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	vs.check(t)
	compareFinal(t, src, dst, appTables...)
}

// Gate 1: promote dưới tải — ứng dụng không ghi được vào target trước khi setval xong (PR-1);
// source bị fence; target có đủ dữ liệu.
func TestG1_E2E_PromoteUnderLoad(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	dst.exec(t,
		"CREATE USER 'app'@'%' IDENTIFIED BY 'app'",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'app'@'%'",
		"GRANT SELECT ON strand.* TO 'app'@'%'",
		"SET GLOBAL read_only = ON")
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 3000)
	wl.Start(6, 3000)

	cfg := strandCfg{Name: "e2e-promote", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)

	appDB, err := sql.Open("mysql", fmt.Sprintf("app:app@tcp(%s)/app", dst.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer appDB.Close()
	var appStop atomic.Bool
	var appWG sync.WaitGroup
	var appOK, appEarly atomic.Int64
	appWG.Add(1)
	go func() {
		defer appWG.Done()
		id := int64(-1)
		for !appStop.Load() {
			if _, err := appDB.Exec("INSERT INTO app.events VALUES (?, 1, 'app')", id); err == nil {
				appOK.Add(1)
				// PR-1: lúc ứng dụng ghi được, bước setval phải đã xong.
				var steps string
				if err := appDB.QueryRow("SELECT completed_steps FROM strand._strand_promote WHERE pipeline_id = ?", cfg.Name).Scan(&steps); err != nil ||
					!strings.Contains(steps, "setval") {
					appEarly.Add(1)
				}
				id--
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(3 * time.Second)
	out, code := runStrandCLI(t, 10*time.Minute, "promote", "--target", dst.RootDSN+"strand", "--fence", src.RootDSN,
		"--pipeline", cfg.Name, "--catchup-timeout", "5m")
	if code != 0 {
		t.Fatalf("promote: exit %d\n%s\nstrand:\n%s", code, out, p.tail())
	}
	time.Sleep(2 * time.Second)
	appStop.Store(true)
	appWG.Wait()
	wl.Stop()
	if appEarly.Load() != 0 {
		t.Fatalf("PR-1 violated: application wrote %d rows before setval completed", appEarly.Load())
	}
	if appOK.Load() == 0 {
		t.Fatalf("application could not write after promote")
	}
	var sro int
	_ = src.DB.QueryRow("SELECT @@GLOBAL.super_read_only").Scan(&sro)
	if sro != 1 {
		t.Fatalf("old source not fenced (super_read_only=%d)", sro)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	// Bỏ các row ứng dụng ghi vào target sau promote rồi so trạng thái.
	dst.exec(t, "SET GLOBAL read_only = OFF", "DELETE FROM app.events WHERE id < 0")
	compareFinal(t, src, dst, appTables...)
	t.Logf("application wrote %d rows only after setval; source fenced", appOK.Load())
}

// Gate 1: `validate` từ chối partial JSON (exit 3).
func TestG1_E2E_ValidateRejectsPartialJSON(t *testing.T) {
	src, dst := newPair(t)
	cfg := strandCfg{Name: "e2e-validate", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.accounts", "app.orders"}, Dir: testDir(t)}
	path := cfg.write(t)
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate on a correct source: exit %d\n%s", code, out)
	}
	src.exec(t, "SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON'")
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 3 || !strings.Contains(out, "binlog_config_invalid") {
		t.Fatalf("validate with partial JSON: exit %d, want 3\n%s", code, out)
	}
	t.Logf("validate rejected partial JSON: %s", strings.TrimSpace(out))
}

// Gate 1: retention sát ngưỡng với lag_slo nhỏ: không tự khóa chết, có bootstrap_retention_risk.
func TestG1_E2E_RetentionRiskNoLivelock(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 60000)
	wl.Start(8, 60000)

	cfg := strandCfg{Name: "e2e-retention", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), LagSLO: "1ms"}
	path := cfg.write(t)
	p := startStrand(t, path)
	// Throttle cao để snapshot chậm và lag vượt lag_slo: governor phải tăng ưu tiên snapshot.
	time.Sleep(500 * time.Millisecond)
	_, _ = runStrandCLI(t, 10*time.Second, "throttle", "set", "--source-iops", "1", "--socket", cfg.Socket)
	waitSynced(t, &cfg, p, 10*time.Minute)
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	raw, _ := os.ReadFile(p.log)
	if !strings.Contains(string(raw), "bootstrap_retention_risk") {
		t.Fatalf("expected bootstrap_retention_risk warning in log")
	}
	compareFinal(t, src, dst, appTables...)
	t.Logf("bootstrap finished despite lag_slo=1ms; bootstrap_retention_risk reported")
}

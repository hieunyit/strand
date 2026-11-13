package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func uptime() string {
	b, _ := os.ReadFile("/proc/loadavg")
	return strings.TrimSpace(string(b))
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// ---------------------------------------------------------------------------
// Bộ chạy tải: các tiến trình client nối tiếp, mỗi tiến trình một đoạn hằng định của sóng.
// ---------------------------------------------------------------------------

type loadDriver struct {
	o     Opts
	ci    connInfo
	dir   string
	t0    time.Time
	stopC chan struct{}
	done  chan struct{}
	mu    sync.Mutex
	segs  []SegmentRun
	fail  error
}

func startLoad(o Opts, ci connInfo, dir string) (*loadDriver, error) {
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o755); err != nil {
		return nil, err
	}
	l := &loadDriver{o: o, ci: ci, dir: dir, t0: time.Now().UTC(), stopC: make(chan struct{}), done: make(chan struct{})}
	go l.loop()
	return l, nil
}

func (l *loadDriver) stopped() bool {
	select {
	case <-l.stopC:
		return true
	default:
		return false
	}
}

func (l *loadDriver) loop() {
	defer close(l.done)
	var at time.Duration
	for idx := 0; !l.stopped(); idx++ {
		seg := l.o.Wave.NextSegment(at, l.o.MaxChunk)
		if d := time.Until(l.t0.Add(seg.Start)); d > 0 {
			select {
			case <-time.After(d):
			case <-l.stopC:
				return
			}
		}
		if l.stopped() {
			return
		}
		sr := l.runSegment(idx, seg)
		l.mu.Lock()
		l.segs = append(l.segs, sr)
		if sr.Err != "" || sr.Exit != 0 {
			l.fail = fmt.Errorf("client segment %d failed (exit %d %s)", idx, sr.Exit, sr.Err)
		}
		failed := l.fail != nil
		l.mu.Unlock()
		if failed {
			return
		}
		at = seg.Start + seg.Dur
	}
}

func (l *loadDriver) runSegment(idx int, seg Segment) SegmentRun {
	prefix := filepath.Join(l.dir, "raw", fmt.Sprintf("seg%04d", idx))
	sr := SegmentRun{Index: idx, Plan: seg, Threads: l.o.Clients, Prefix: prefix,
		Stdout: prefix + ".stdout", Stderr: prefix + ".stderr"}
	argv := clientArgv(l.o, l.ci, seg, l.o.Clients, prefix, true)
	if l.o.ClientContainer != "" { // ghi file thô bằng uid của controller để đọc lại được
		argv = append([]string{"docker", "exec", "-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())}, argv[2:]...)
	}
	outF, e1 := os.Create(sr.Stdout)
	errF, e2 := os.Create(sr.Stderr)
	if e1 != nil || e2 != nil {
		sr.Err = fmt.Sprint(e1, e2)
		return sr
	}
	defer outF.Close()
	defer errF.Close()
	ctx, cancel := context.WithTimeout(context.Background(), seg.Dur+2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = outF, errF
	sr.Start = time.Now().UTC()
	err := cmd.Run()
	sr.End = time.Now().UTC()
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		sr.Exit = ee.ExitCode()
	default:
		sr.Err = err.Error()
		sr.Exit = -1
	}
	return sr
}

// Stop: không khởi động đoạn mới; chờ đoạn đang chạy kết thúc tự nhiên (log thô cần footer).
func (l *loadDriver) Stop() ([]SegmentRun, error) {
	close(l.stopC)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]SegmentRun(nil), l.segs...), l.fail
}

// ---------------------------------------------------------------------------
// Một lần chạy
// ---------------------------------------------------------------------------

func parseSegment(o Opts) func(SegmentRun) (SegmentRaw, error) {
	return func(sg SegmentRun) (SegmentRaw, error) {
		if o.Engine == "postgres" {
			return ParsePgbenchSegment(sg.Prefix, sg.Stdout, sg.Exit)
		}
		return ParseSysbenchSegment(sg.Prefix, sg.Threads, sg.Stdout, sg.Stderr, sg.Exit)
	}
}

func runOnce(ctx context.Context, o Opts, fx Fixture, job Job, name string, attempt int) (rec RunRecord) {
	rec = RunRecord{Name: name, Governor: job.Gov, Repeat: job.Rep, Attempt: attempt, Engine: o.Engine,
		Wave: o.Wave, SoftIOPS: o.SoftIOPS, HardIOPS: o.HardIOPS, Clients: o.Clients, UptimeBefore: uptime()}
	rec.Restore.SourceTreeWant, rec.Restore.TargetTreeWant, rec.Restore.ManifestWant = fx.SourceTree, fx.TargetTree, fx.ManifestSHA256
	defer func() { rec.UptimeAfter = uptime() }()
	fail := func(err error) RunRecord {
		rec.Error = strings.TrimSpace(rec.Error + "; " + err.Error())
		return rec
	}
	runDir, err := filepath.Abs(filepath.Join(o.Out, name))
	if err != nil {
		return fail(err)
	}
	_ = os.RemoveAll(runDir)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fail(err)
	}
	fixDir, _ := filepath.Abs(o.FixtureDir)
	ci, err := parseConn(o.Engine, o.SourceDSN)
	if err != nil {
		return fail(err)
	}

	// 1. Khôi phục source và target về cùng snapshot; hash cây datadir đo khi container còn dừng.
	sn := dockerNode{Container: o.SourceContainer, Image: o.HelperImage}
	tn := dockerNode{Container: o.TargetContainer, Image: o.HelperImage}
	if rec.Restore.SourceTreeGot, err = sn.Restore(ctx, fixDir, "source"); err != nil {
		return fail(fmt.Errorf("restore source: %w", err))
	}
	if rec.Restore.TargetTreeGot, err = tn.Restore(ctx, fixDir, "target"); err != nil {
		return fail(fmt.Errorf("restore target: %w", err))
	}
	// 2. Kiểm manifest logic của source đã khôi phục, rồi khởi động lại để buffer pool lạnh như nhau.
	if err := verifyLogical(ctx, o, fx, &rec); err != nil {
		return fail(err)
	}
	if _, err := docker(ctx, "restart", "-t", "120", o.SourceContainer); err != nil {
		return fail(err)
	}
	src, err := openDB(ctx, o.Engine, o.SourceDSN, 3*time.Minute)
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	dstDSN := o.TargetDSN
	if o.Engine == "mysql" {
		dstDSN += "strand"
	}
	dst, err := openDB(ctx, o.Engine, dstDSN, 3*time.Minute)
	if err != nil {
		return fail(err)
	}
	defer dst.Close()

	// 3. Tải ứng dụng bằng client ngoài.
	ld, err := startLoad(o, ci, runDir)
	if err != nil {
		return fail(err)
	}
	rec.LoadT0 = ld.t0
	var loadOnce sync.Once
	stopLoad := func() {
		loadOnce.Do(func() {
			segs, lerr := ld.Stop()
			rec.Segments, rec.LoadStopped = segs, time.Now().UTC()
			if lerr != nil {
				rec.Error = strings.TrimSpace(rec.Error + "; " + lerr.Error())
			}
		})
	}
	defer stopLoad()
	select {
	case <-time.After(o.Warmup):
	case <-ctx.Done():
		return fail(ctx.Err())
	}

	// 4. Strand.
	cfgPath := filepath.Join(runDir, "pipeline.yaml")
	if err := os.WriteFile(cfgPath, []byte(strandYAML(o, job.Gov, runDir)), 0o600); err != nil {
		return fail(err)
	}
	logF, err := os.Create(filepath.Join(runDir, "strand.log"))
	if err != nil {
		return fail(err)
	}
	defer logF.Close()
	cmd := exec.Command(o.Strand, "sync", "-c", cfgPath)
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	bootDone := make(chan time.Time, 1)
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(logF, line)
			if !sent && strings.Contains(line, "bootstrap finished") {
				sent = true
				bootDone <- time.Now().UTC()
			}
		}
	}()
	rec.StrandStart = time.Now().UTC()
	if err := cmd.Start(); err != nil {
		return fail(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait(); _ = pw.Close() }()
	smp := NewSampler(o.MetricsAddr, "bench", job.Gov)
	sctx, scancel := context.WithCancel(ctx)
	go smp.Run(sctx)
	defer scancel()
	saveTelemetry := func() { _ = WriteTelemetry(filepath.Join(runDir, "telemetry.csv"), smp.Snapshot()) }

	select {
	case t := <-bootDone:
		rec.BootstrapFinished = t
	case err := <-exited:
		scancel()
		saveTelemetry()
		rec.StrandExit, rec.StrandExitSet = exitCode(err), true
		return fail(fmt.Errorf("strand exited before bootstrap finished: %v", err))
	case <-time.After(o.SnapshotTimeout):
		_ = cmd.Process.Signal(syscall.SIGTERM)
		scancel()
		saveTelemetry()
		return fail(fmt.Errorf("snapshot did not finish within %s", o.SnapshotTimeout))
	}
	time.Sleep(o.StreamAfter)
	scancel()
	saveTelemetry()
	stopLoad()

	// 5. Chờ bắt kịp, dừng Strand (SIGTERM, phải exit 0).
	if err := waitCaughtUp(ctx, o, src, dst, o.CatchupTimeout); err != nil {
		rec.Error = strings.TrimSpace(rec.Error + "; " + err.Error())
	} else {
		rec.CaughtUp = true
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-exited:
		rec.StrandExit, rec.StrandExitSet = exitCode(err), true
	case <-time.After(2 * time.Minute):
		_ = cmd.Process.Kill()
		rec.StrandExit, rec.StrandExitSet = -1, true
	}

	// 6. So khớp cuối source ↔ target (đếm + checksum theo bảng).
	rec.CompareOK, rec.CompareDetail = finalCompare(ctx, o, src)
	return rec
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

func verifyLogical(ctx context.Context, o Opts, fx Fixture, rec *RunRecord) error {
	src, err := openDB(ctx, o.Engine, o.SourceDSN, 3*time.Minute)
	if err != nil {
		return err
	}
	defer src.Close()
	man, err := ComputeManifest(ctx, src, o)
	if err != nil {
		return err
	}
	rec.Restore.ManifestGot = man.Hash()
	if d := DiffManifest(fx.Manifest, man); len(d) > 0 {
		rec.Restore.Detail = strings.Join(d, "; ")
	}
	return nil
}

func finalCompare(ctx context.Context, o Opts, src *sql.DB) (bool, string) {
	dst, err := openDB(ctx, o.Engine, o.TargetDSN, time.Minute)
	if err != nil {
		return false, err.Error()
	}
	defer dst.Close()
	sm, err := ComputeManifest(ctx, src, o)
	if err != nil {
		return false, "source: " + err.Error()
	}
	dm, err := ComputeManifest(ctx, dst, o)
	if err != nil {
		return false, "target: " + err.Error()
	}
	return compareManifests(sm, dm)
}

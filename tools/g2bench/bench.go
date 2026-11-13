package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// OfficialProblems: điều kiện để một lần chạy được gọi là OFFICIAL. Rỗng = đủ.
// Giá trị cố định theo giao thức (Amendment 2026-11-13); không điều chỉnh sau khi thấy số.
func OfficialProblems(o Opts, gitClean bool) []string {
	var p []string
	add := func(c bool, s string) {
		if !c {
			p = append(p, s)
		}
	}
	gs := splitList(o.Governors)
	sorted := append([]string(nil), gs...)
	sort.Strings(sorted)
	add(strings.Join(sorted, ",") == "grace-v2,onoff,static", "governors must be exactly static,onoff,grace-v2")
	add(o.Repeats == 3, "repeats must be 3")
	add(o.Only == "", "--only is a smoke option")
	add(o.FixtureDir != "" && o.SourceContainer != "" && o.TargetContainer != "" && o.HelperImage != "", "identical-fixture docker restore (fixture-dir, containers, helper-image) is required")
	add(o.ClientBinary != "" && o.ClientSHA != "", "client binary and --client-sha256 are required")
	add(o.Wave.Period == 10*time.Minute && o.Wave.SpikeEvery == 3*time.Minute && o.Wave.SpikeDur == 10*time.Second && o.Wave.SpikeMult == 2,
		"wave must be period 10m, spike 2x high for 10s every 3m")
	add(o.Wave.Low > 0 && o.Wave.High > o.Wave.Low, "need 0 < low < high")
	add(o.Warmup >= 120*time.Second && o.StreamAfter >= 120*time.Second, "warmup and stream-after must be >= 120s")
	add(o.Clients >= 1 && o.SoftIOPS > 0 && o.HardIOPS > o.SoftIOPS, "need soft < hard iops limits")
	add(!o.RelaxBreaker, "--smoke-relax-breaker is dev-only")
	add(o.Attest, "--attest-dedicated (no other work on the machine) is required")
	add(gitClean, "git working tree must be clean (commit before measuring)")
	return p
}

type Report struct {
	Label     string                `json:"label"`
	Official  bool                  `json:"official"`
	Problems  []string              `json:"official_problems,omitempty"`
	Engine    string                `json:"engine"`
	Seed      int64                 `json:"seed"`
	Order     []string              `json:"order"`
	GitCommit string                `json:"git_commit"`
	GitClean  bool                  `json:"git_clean"`
	Client    string                `json:"client_version"`
	Fixture   *Fixture              `json:"fixture,omitempty"`
	Options   Opts                  `json:"options"`
	Started   time.Time             `json:"started"`
	Finished  time.Time             `json:"finished"`
	Runs      []RunResult           `json:"runs"`
	Summary   map[string]GovSummary `json:"summary,omitempty"`
	Verdict   Verdict               `json:"verdict"`
}

func gitState() (string, bool) {
	c, _ := exec.Command("git", "rev-parse", "HEAD").Output()
	st, err := exec.Command("git", "status", "--porcelain").Output()
	return strings.TrimSpace(string(c)), err == nil && len(strings.TrimSpace(string(st))) == 0
}

func clientVersion(ctx context.Context, o Opts) (string, error) {
	argv := wrapExec(o, nil, o.ClientBinary, []string{"--version"})
	out, err := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput()
	v := strings.TrimSpace(string(out))
	if err != nil {
		return v, fmt.Errorf("client --version: %w", err)
	}
	if o.ClientSHA != "" {
		argv = wrapExec(o, nil, "sha256sum", []string{o.ClientBinary})
		if o.ClientContainer == "" {
			argv = []string{"sha256sum", o.ClientBinary}
		}
		if argv[0] == "env" {
			argv = argv[2:]
		}
		sum, err := exec.CommandContext(ctx, argv[0], argv[1:]...).Output()
		if err != nil || len(strings.Fields(string(sum))) == 0 {
			return v, fmt.Errorf("cannot hash client binary: %v", err)
		}
		got := strings.Fields(string(sum))[0]
		if got != o.ClientSHA {
			return v, fmt.Errorf("client binary sha256 %s != --client-sha256 %s", got, o.ClientSHA)
		}
		v += " sha256=" + got
	}
	return v, nil
}

func Benchmark(ctx context.Context, o Opts) error {
	if err := os.MkdirAll(o.Out, 0o755); err != nil {
		return err
	}
	commit, clean := gitState()
	problems := OfficialProblems(o, clean)
	if o.Official && len(problems) > 0 {
		return fmt.Errorf("--official refused: %s", strings.Join(problems, "; "))
	}
	fx, err := LoadFixture(o.FixtureDir)
	if err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	if fx.Engine != o.Engine {
		return fmt.Errorf("fixture engine %s != %s", fx.Engine, o.Engine)
	}
	cv, err := clientVersion(ctx, o)
	if err != nil {
		return err
	}
	var jobs []Job
	if o.Only != "" {
		jobs = []Job{{o.Only, 1}}
	} else {
		jobs = RunOrder(splitList(o.Governors), o.Repeats, o.Seed)
	}
	rep := Report{Engine: o.Engine, Seed: o.Seed, GitCommit: commit, GitClean: clean, Client: cv, Fixture: &fx,
		Options: o.Redacted(), Started: time.Now().UTC(), Problems: problems}
	for _, j := range jobs {
		rep.Order = append(rep.Order, j.String())
	}
	log.Printf("order (seed %d): %v", o.Seed, rep.Order)
	if err := writeJSON(filepath.Join(o.Out, "plan.json"), rep); err != nil { // cố định kế hoạch trước khi đo
		return err
	}
	var failed []Job
	for i, j := range jobs {
		name := fmt.Sprintf("%02d-%s-%d", i+1, j.Gov, j.Rep)
		if !collect(ctx, o, fx, j, name, 1) {
			failed = append(failed, j)
		}
	}
	if o.RerunFailed && o.Only == "" { // giao thức §4.5: chạy bù một lần ở cuối thứ tự, ghi rõ
		for i, j := range failed {
			collect(ctx, o, fx, j, fmt.Sprintf("%02d-%s-%d-rerun", len(jobs)+i+1, j.Gov, j.Rep), 2)
		}
	}
	return AnalyzeAll(o)
}

// collect chạy một lần, ghi run.json + telemetry; trả true nếu bằng chứng đủ và lần chạy đạt.
func collect(ctx context.Context, o Opts, fx Fixture, j Job, name string, attempt int) bool {
	log.Printf("=== run %s (attempt %d)", name, attempt)
	rec := runOnce(ctx, o, fx, j, name, attempt)
	dir := filepath.Join(o.Out, name)
	if err := writeJSON(filepath.Join(dir, "run.json"), rec); err != nil {
		log.Printf("write run.json: %v", err)
	}
	res, _ := analyzeRun(o, dir)
	log.Printf("=== run %s: snapshot=%.1fs p99=%.2fms hard_exceed=%d p2p_max=%.3f ok=%v evidence=%q err=%q",
		name, float64(res.SnapshotSeconds), float64(res.AppP99Ms), res.HardLimitExceed, float64(res.P2PLogRMax), res.OK, res.Evidence, res.Error)
	return res.OK
}

// analyzeRun đọc lại run.json + telemetry.csv + log thô trong thư mục lần chạy và suy ra mọi số liệu.
func analyzeRun(o Opts, dir string) (RunResult, error) {
	var rec RunRecord
	b, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err == nil {
		err = json.Unmarshal(b, &rec)
	}
	if err != nil {
		return RunResult{Name: filepath.Base(dir), Evidence: "run.json missing: " + err.Error()}, err
	}
	samples, terr := ReadTelemetry(filepath.Join(dir, "telemetry.csv"))
	res, rows := Analyze(rec, samples, parseSegment(Opts{Engine: rec.Engine}))
	res.Dir = dir
	if terr != nil && res.Evidence == "" {
		res.Evidence = "telemetry missing: " + terr.Error()
	}
	if len(rows) > 0 {
		res.TimeseriesCSV = filepath.Join(dir, "timeseries.csv")
		if err := WriteTimeseriesCSV(res.TimeseriesCSV, rec, rows); err != nil {
			res.Evidence = firstNonEmpty(res.Evidence, "timeseries: "+err.Error())
		}
		if err := PlotRun(filepath.Join(dir, "chart.svg"), rec, rows); err != nil {
			log.Printf("plot %s: %v", dir, err)
		}
	}
	return res, nil
}

// AnalyzeAll tổng hợp mọi thư mục lần chạy dưới o.Out (kể cả lần chạy bù) → results.json + biểu đồ.
func AnalyzeAll(o Opts) error {
	var rep Report
	b, err := os.ReadFile(filepath.Join(o.Out, "plan.json"))
	if err != nil {
		return fmt.Errorf("plan.json: %w", err)
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		return err
	}
	dirs, _ := filepath.Glob(filepath.Join(o.Out, "[0-9][0-9]-*"))
	sort.Strings(dirs)
	rep.Runs = nil
	for _, d := range dirs {
		if st, err := os.Stat(filepath.Join(d, "run.json")); err != nil || st.IsDir() {
			continue
		}
		res, _ := analyzeRun(rep.Options, d)
		rep.Runs = append(rep.Runs, res)
	}
	if rep.Options.Only == "" {
		rep.Summary, rep.Verdict = Decide(rep.Runs, rep.Options.Repeats)
	} else {
		rep.Verdict = Verdict{Status: StatusIncomplete, Reasons: []string{"--only: single smoke run, no decision"}}
	}
	rep.Finished = time.Now().UTC()
	rep.Official = rep.Options.Official && len(rep.Problems) == 0 && rep.Verdict.Status != StatusIncomplete
	rep.Label = "NON-OFFICIAL (dev/smoke): not Gate 2 evidence"
	if rep.Official {
		rep.Label = "OFFICIAL Gate 2 evidence"
	} else if rep.Options.Official {
		rep.Label = "OFFICIAL run but evidence INCOMPLETE: no Gate 2 result"
	}
	if err := writeJSON(filepath.Join(o.Out, "results.json"), rep); err != nil {
		return err
	}
	log.Printf("%s | verdict: %s", rep.Label, rep.Verdict)
	return nil
}

// benchclient tạo lệnh dry-run cho client chuẩn và đọc log transaction pgbench.
// Chưa chạy tải; không coi plan đơn lẻ là benchmark Gate 2.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type clientPlan struct {
	Backend             string   `json:"backend"`
	Argv                []string `json:"argv"`
	RawLatencyAvailable bool     `json:"raw_latency_available"`
	OfficialReady       bool     `json:"official_ready"`
	Reasons             []string `json:"reasons"`
}

func planClient(backend, binary, logPrefix string, clients, jobs int, duration time.Duration, rate float64) (clientPlan, error) {
	if clients < 1 || jobs < 1 || jobs > clients || duration < time.Second || rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return clientPlan{}, errors.New("clients, jobs, duration and rate must be positive; jobs <= clients")
	}
	if binary == "" || logPrefix == "" {
		return clientPlan{}, errors.New("binary and log-prefix are required")
	}
	seconds := strconv.FormatInt(int64(math.Ceil(duration.Seconds())), 10)
	pace := strconv.FormatFloat(rate, 'f', -1, 64)
	switch backend {
	case "pgbench":
		return clientPlan{Backend: backend, RawLatencyAvailable: true,
			Argv: []string{binary, "-n", "-l", "--log-prefix=" + logPrefix,
				"-c", strconv.Itoa(clients), "-j", strconv.Itoa(jobs),
				"-T", seconds, "-R", pace, "-P", "1", "-b", "tpcb-like"},
			Reasons: []string{"Requires owned PG fixture and PG* connection environment", "One constant-rate segment; square wave and restore orchestration are not wired"}}, nil
	case "sysbench":
		return clientPlan{Backend: backend, RawLatencyAvailable: false,
			Argv: []string{binary, "--db-driver=mysql", "--threads=" + strconv.Itoa(clients),
				"--time=" + seconds, "--rate=" + pace, "--report-interval=1", "--percentile=99",
				"oltp_read_write", "run"},
			Reasons: []string{"Connection options and owned fixture are not wired", "Sysbench interval p99 is not raw per-transaction latency; exact snapshot-window p99 is unavailable", "Square wave and restore orchestration are not wired"}}, nil
	default:
		return clientPlan{}, fmt.Errorf("unsupported backend %q", backend)
	}
}

type pgRecord struct {
	At        time.Time
	LatencyUS int64
	Failed    bool
}

func parsePgbenchRecord(line string) (pgRecord, error) {
	f := strings.Fields(line)
	if len(f) < 6 {
		return pgRecord{}, fmt.Errorf("pgbench log needs at least six fields, got %d", len(f))
	}
	if _, err := strconv.ParseInt(f[0], 10, 64); err != nil {
		return pgRecord{}, fmt.Errorf("client id: %w", err)
	}
	if _, err := strconv.ParseInt(f[1], 10, 64); err != nil {
		return pgRecord{}, fmt.Errorf("transaction number: %w", err)
	}
	if _, err := strconv.ParseInt(f[3], 10, 64); err != nil {
		return pgRecord{}, fmt.Errorf("script number: %w", err)
	}
	sec, err := strconv.ParseInt(f[4], 10, 64)
	if err != nil {
		return pgRecord{}, fmt.Errorf("completion seconds: %w", err)
	}
	usec, err := strconv.ParseInt(f[5], 10, 64)
	if err != nil || usec < 0 || usec >= 1_000_000 {
		return pgRecord{}, fmt.Errorf("invalid completion microseconds %q", f[5])
	}
	r := pgRecord{At: time.Unix(sec, usec*1000).UTC()}
	if f[2] == "failed" || f[2] == "skipped" || f[2] == "serialization" || f[2] == "deadlock" {
		r.Failed = true
		return r, nil
	}
	r.LatencyUS, err = strconv.ParseInt(f[2], 10, 64)
	if err != nil || r.LatencyUS < 0 {
		return pgRecord{}, fmt.Errorf("invalid latency %q", f[2])
	}
	return r, nil
}

type pgWindow struct {
	Backend         string    `json:"backend"`
	Start           time.Time `json:"snapshot_start"`
	End             time.Time `json:"snapshot_end"`
	Transactions    int       `json:"transactions"`
	FailedTotal     int       `json:"failed_total"`
	P99Ms           float64   `json:"p99_ms"`
	RawLogFiles     []string  `json:"raw_log_files"`
	OfficialReady   bool      `json:"official_ready"`
	IncompleteNotes []string  `json:"incomplete_notes"`
}

func parsePgbenchWindow(paths []string, start, end time.Time) (pgWindow, error) {
	if !start.Before(end) || len(paths) == 0 {
		return pgWindow{}, errors.New("nonempty log files and start < end are required")
	}
	res := pgWindow{Backend: "pgbench", Start: start, End: end, RawLogFiles: append([]string(nil), paths...)}
	var lat []int64
	for _, p := range paths {
		file, err := os.Open(p)
		if err != nil {
			return pgWindow{}, err
		}
		sc := bufio.NewScanner(file)
		sc.Buffer(make([]byte, 1024), 1024*1024)
		lineNo := 0
		for sc.Scan() {
			lineNo++
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			r, err := parsePgbenchRecord(line)
			if err != nil {
				file.Close()
				return pgWindow{}, fmt.Errorf("%s:%d: %w", p, lineNo, err)
			}
			if r.Failed {
				res.FailedTotal++
				continue
			}
			if !r.At.Before(start) && r.At.Before(end) {
				lat = append(lat, r.LatencyUS)
			}
		}
		if err := sc.Err(); err != nil {
			file.Close()
			return pgWindow{}, fmt.Errorf("%s: %w", p, err)
		}
		if err := file.Close(); err != nil {
			return pgWindow{}, err
		}
	}
	if len(lat) == 0 {
		return pgWindow{}, errors.New("no successful transactions completed inside snapshot window")
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	res.Transactions = len(lat)
	res.P99Ms = float64(lat[int(math.Ceil(.99*float64(len(lat))))-1]) / 1000
	if res.FailedTotal > 0 {
		res.IncompleteNotes = append(res.IncompleteNotes, fmt.Sprintf("%d failed/skipped transactions in raw logs", res.FailedTotal))
	}
	res.IncompleteNotes = append(res.IncompleteNotes, "Raw p99 only; fixture restore, wave, Strand telemetry and final compare must be integrated")
	return res, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("benchclient", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var backend, binary, prefix, parsePrefix, startText, endText string
	var clients, jobs int
	var duration time.Duration
	var rate float64
	fs.StringVar(&backend, "backend", "pgbench", "pgbench or sysbench")
	fs.StringVar(&binary, "binary", "", "client executable path")
	fs.StringVar(&prefix, "log-prefix", "", "owned raw-log prefix")
	fs.StringVar(&parsePrefix, "parse-pgbench-prefix", "", "parse pgbench raw logs at prefix.*")
	fs.StringVar(&startText, "snapshot-start", "", "snapshot start RFC3339Nano")
	fs.StringVar(&endText, "snapshot-end", "", "snapshot end RFC3339Nano")
	fs.IntVar(&clients, "clients", 16, "client count")
	fs.IntVar(&jobs, "jobs", 8, "thread count")
	fs.DurationVar(&duration, "duration", 10*time.Minute, "one constant-rate segment")
	fs.Float64Var(&rate, "rate", 100, "target transactions/s")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if parsePrefix != "" {
		start, e1 := time.Parse(time.RFC3339Nano, startText)
		end, e2 := time.Parse(time.RFC3339Nano, endText)
		if e1 != nil || e2 != nil {
			fmt.Fprintln(stderr, "snapshot-start/end must be RFC3339Nano")
			return 2
		}
		paths, err := filepath.Glob(parsePrefix + ".*")
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		res, err := parsePgbenchWindow(paths, start, end)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := enc.Encode(res); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if binary == "" {
		binary = backend
	}
	plan, err := planClient(backend, binary, prefix, clients, jobs, duration, rate)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if err := enc.Encode(plan); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// Command g2bench chạy benchmark Gate 2 (G2-12) bằng client ngoài: sysbench (MySQL) / pgbench (PG).
// p99 ứng dụng tính ở phía client từ log thô từng transaction/event, không lấy từ Strand.
// File này: đọc log thô và tính p99 (nearest rank) trên cửa sổ [start, end).
package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Event là một transaction/event đã hoàn tất thành công phía client.
type Event struct {
	At    time.Time // thời điểm hoàn tất (đồng hồ thực)
	LatNS int64     // latency của chính event đó
}

// SegmentRaw là kết quả đọc log thô của một tiến trình client (một đoạn tải hằng định).
type SegmentRaw struct {
	Events     []Event
	Reported   uint64 // số event client tự báo (so khớp với log thô)
	RawFiles   []string
	FailedRecs int
}

// P99NearestRank: p99 nearest-rank của latency ns. Rỗng là lỗi (không có bằng chứng).
func P99NearestRank(lat []int64) (int64, error) {
	return PercentileNearestRank(lat, 0.99)
}

func PercentileNearestRank(lat []int64, p float64) (int64, error) {
	if len(lat) == 0 {
		return 0, errors.New("no events")
	}
	s := append([]int64(nil), lat...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	return s[idx], nil
}

// WindowLatencies lấy latency của event hoàn tất trong [start, end) (nửa mở).
func WindowLatencies(ev []Event, start, end time.Time) []int64 {
	var out []int64
	for _, e := range ev {
		if !e.At.Before(start) && e.At.Before(end) {
			out = append(out, e.LatNS)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// sysbench (bản patch sysbench-1.0.20-raw-events.patch)
// ---------------------------------------------------------------------------

var (
	reSbEvents  = regexp.MustCompile(`(?m)^\s*total number of events:\s+(\d+)`)
	reSbIgnored = regexp.MustCompile(`(?m)^\s*ignored errors:\s+(\d+)`)
	reSbReconn  = regexp.MustCompile(`(?m)^\s*reconnects:\s+(\d+)`)
)

// ParseSysbenchSegment đọc log thô một lần chạy sysbench. Mọi bất thường → lỗi (fail-closed):
// exit != 0, thiếu/ trùng marker, footer sai, số thứ tự không liền, timestamp lùi,
// số event thô lệch marker hoặc lệch bảng tổng kết của sysbench, ignored errors / reconnects != 0.
func ParseSysbenchSegment(prefix string, threads int, stdoutPath, stderrPath string, exitCode int) (SegmentRaw, error) {
	var res SegmentRaw
	if exitCode != 0 {
		return res, fmt.Errorf("sysbench exit code %d", exitCode)
	}
	if threads < 1 {
		return res, errors.New("threads must be positive")
	}
	marker, err := sysbenchMarker(stderrPath)
	if err != nil {
		return res, err
	}
	out, err := os.ReadFile(stdoutPath)
	if err != nil {
		return res, err
	}
	// Không dùng warmup-time nên bộ đếm tổng kết phải bằng số event ghi thô (phase khớp nhau).
	m := reSbEvents.FindSubmatch(out)
	if m == nil {
		return res, errors.New("sysbench stdout has no 'total number of events'")
	}
	summary, _ := strconv.ParseUint(string(m[1]), 10, 64)
	if summary != marker {
		return res, fmt.Errorf("sysbench summary events %d != raw marker %d", summary, marker)
	}
	for _, re := range []*regexp.Regexp{reSbIgnored, reSbReconn} {
		mm := re.FindSubmatch(out)
		if mm == nil {
			return res, errors.New("sysbench stdout lacks ignored errors / reconnects counters")
		}
		if string(mm[1]) != "0" {
			return res, fmt.Errorf("sysbench reported %s", strings.TrimSpace(string(mm[0])))
		}
	}
	paths, err := filepath.Glob(prefix + ".[0-9]*")
	if err != nil {
		return res, err
	}
	if len(paths) != threads {
		return res, fmt.Errorf("found %d worker raw files, expected %d", len(paths), threads)
	}
	sort.Strings(paths)
	res.RawFiles = paths
	seen := map[uint64]bool{}
	var total uint64
	for _, p := range paths {
		n, id, err := readSysbenchWorker(p, &res)
		if err != nil {
			return SegmentRaw{}, err
		}
		if seen[id] {
			return SegmentRaw{}, fmt.Errorf("%s: duplicate thread id %d", p, id)
		}
		seen[id] = true
		total += n
	}
	for i := 0; i < threads; i++ {
		if !seen[uint64(i)] {
			return SegmentRaw{}, fmt.Errorf("missing worker thread %d", i)
		}
	}
	if total != marker {
		return SegmentRaw{}, fmt.Errorf("raw events %d != marker %d", total, marker)
	}
	res.Reported = marker
	return res, nil
}

func sysbenchMarker(stderrPath string) (uint64, error) {
	f, err := os.Open(stderrPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var count uint64
	found := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "STRAND_RAW_EVENTS ") {
			continue
		}
		if found {
			return 0, errors.New("duplicate STRAND_RAW_EVENTS markers")
		}
		found = true
		f := strings.Fields(line)
		if len(f) != 4 || !strings.HasPrefix(f[1], "count=") || f[2] != "errors=0" || f[3] != "run_status=0" {
			return 0, fmt.Errorf("invalid or failing marker %q", line)
		}
		count, err = strconv.ParseUint(strings.TrimPrefix(f[1], "count="), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("bad marker count: %w", err)
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("missing STRAND_RAW_EVENTS marker")
	}
	return count, nil
}

// readSysbenchWorker đọc một file worker: "thread seq ts_ns dur_ns" + footer "# STRAND_RAW_THREAD ...".
func readSysbenchWorker(path string, res *SegmentRaw) (count, thread uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024), 1<<20)
	var prev, lastTS uint64
	footer := false
	line := 0
	for sc.Scan() {
		line++
		fl := strings.Fields(sc.Text())
		if len(fl) > 0 && fl[0] == "#" {
			if footer || len(fl) != 5 || fl[1] != "STRAND_RAW_THREAD" {
				return 0, 0, fmt.Errorf("%s:%d: invalid footer", path, line)
			}
			kv := map[string]uint64{}
			for _, x := range fl[2:] {
				p := strings.SplitN(x, "=", 2)
				if len(p) != 2 {
					return 0, 0, fmt.Errorf("%s:%d: bad footer field", path, line)
				}
				v, e := strconv.ParseUint(p[1], 10, 64)
				if e != nil {
					return 0, 0, fmt.Errorf("%s:%d: %w", path, line, e)
				}
				kv[p[0]] = v
			}
			id, ok1 := kv["thread_id"]
			cnt, ok2 := kv["count"]
			er, ok3 := kv["errors"]
			if len(kv) != 3 || !ok1 || !ok2 || !ok3 || cnt != count || er != 0 || (count > 0 && id != thread) {
				return 0, 0, fmt.Errorf("%s:%d: footer count/thread/error mismatch", path, line)
			}
			thread = id
			footer = true
			continue
		}
		if footer {
			return 0, 0, fmt.Errorf("%s:%d: records after footer", path, line)
		}
		if len(fl) != 4 {
			return 0, 0, fmt.Errorf("%s:%d: expected 4 fields", path, line)
		}
		var v [4]uint64
		for i, s := range fl {
			if v[i], err = strconv.ParseUint(s, 10, 64); err != nil {
				return 0, 0, fmt.Errorf("%s:%d: %w", path, line, err)
			}
		}
		if count == 0 {
			thread = v[0]
		}
		if v[0] != thread || v[1] != prev+1 || v[2] > math.MaxInt64 || v[3] > math.MaxInt64 {
			return 0, 0, fmt.Errorf("%s:%d: thread/sequence/value invalid", path, line)
		}
		if count > 0 && v[2] < lastTS {
			return 0, 0, fmt.Errorf("%s:%d: realtime clock moved backwards", path, line)
		}
		prev, lastTS = v[1], v[2]
		count++
		res.Events = append(res.Events, Event{At: time.Unix(0, int64(v[2])).UTC(), LatNS: int64(v[3])})
	}
	if err := sc.Err(); err != nil {
		return 0, 0, fmt.Errorf("%s: %w", path, err)
	}
	if !footer {
		return 0, 0, fmt.Errorf("%s: missing footer (truncated log)", path)
	}
	return count, thread, nil
}

// ---------------------------------------------------------------------------
// pgbench -l (không gộp)
// ---------------------------------------------------------------------------

var rePgProcessed = regexp.MustCompile(`(?m)number of transactions actually processed:\s+(\d+)`)

// ParsePgbenchSegment đọc mọi file prefix.* của một lần chạy pgbench. Cột: client tx latency_us script
// time_epoch time_us [schedule_lag]. Transaction failed/skipped/serialization/deadlock → lỗi;
// số dòng log phải bằng "number of transactions actually processed" trong stdout.
func ParsePgbenchSegment(prefix, stdoutPath string, exitCode int) (SegmentRaw, error) {
	var res SegmentRaw
	if exitCode != 0 {
		return res, fmt.Errorf("pgbench exit code %d", exitCode)
	}
	out, err := os.ReadFile(stdoutPath)
	if err != nil {
		return res, err
	}
	m := rePgProcessed.FindSubmatch(out)
	if m == nil {
		return res, errors.New("pgbench stdout has no 'number of transactions actually processed'")
	}
	processed, _ := strconv.ParseUint(string(m[1]), 10, 64)
	paths, err := filepath.Glob(prefix + ".[0-9]*") // pid là số; tránh .stdout/.stderr cùng prefix
	if err != nil {
		return res, err
	}
	if len(paths) == 0 {
		return res, errors.New("no pgbench log files")
	}
	sort.Strings(paths)
	res.RawFiles = paths
	var n uint64
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return SegmentRaw{}, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024), 1<<20)
		ln := 0
		for sc.Scan() {
			ln++
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			ev, failed, err := parsePgbenchLine(line)
			if err != nil {
				f.Close()
				return SegmentRaw{}, fmt.Errorf("%s:%d: %w", p, ln, err)
			}
			n++
			if failed {
				res.FailedRecs++
				continue
			}
			res.Events = append(res.Events, ev)
		}
		err = sc.Err()
		f.Close()
		if err != nil {
			return SegmentRaw{}, fmt.Errorf("%s: %w", p, err)
		}
	}
	if res.FailedRecs > 0 {
		return SegmentRaw{}, fmt.Errorf("%d failed/skipped transactions in pgbench log", res.FailedRecs)
	}
	if n != processed {
		return SegmentRaw{}, fmt.Errorf("pgbench log records %d != processed %d", n, processed)
	}
	res.Reported = processed
	return res, nil
}

func parsePgbenchLine(line string) (Event, bool, error) {
	f := strings.Fields(line)
	if len(f) < 6 {
		return Event{}, false, fmt.Errorf("need >= 6 fields, got %d", len(f))
	}
	sec, err := strconv.ParseInt(f[4], 10, 64)
	if err != nil {
		return Event{}, false, fmt.Errorf("time_epoch: %w", err)
	}
	us, err := strconv.ParseInt(f[5], 10, 64)
	if err != nil || us < 0 || us >= 1_000_000 {
		return Event{}, false, fmt.Errorf("bad time_us %q", f[5])
	}
	at := time.Unix(sec, us*1000).UTC()
	switch f[2] {
	case "failed", "skipped", "serialization", "deadlock":
		return Event{At: at}, true, nil
	}
	lat, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil || lat < 0 {
		return Event{}, false, fmt.Errorf("bad latency %q", f[2])
	}
	return Event{At: at, LatNS: lat * 1000}, false, nil
}

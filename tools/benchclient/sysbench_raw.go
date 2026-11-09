package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sysbenchRawWindow chỉ đọc log được patch sysbench-1.0.20-raw-events.patch ghi ra.
// Count phải khớp marker STRAND_RAW_EVENTS của instrumentation; phase counter
// trong báo cáo sysbench cần đối chiếu riêng vì warmup có thể reset thống kê.
type sysbenchRawWindow struct {
	Backend         string    `json:"backend"`
	Start           time.Time `json:"snapshot_start"`
	End             time.Time `json:"snapshot_end"`
	Transactions    int       `json:"transactions"`
	TotalRawEvents  uint64    `json:"total_raw_events"`
	ExpectedEvents  uint64    `json:"expected_events"`
	ExpectedThreads int       `json:"expected_threads"`
	ClientExitCode  int       `json:"client_exit_code"`
	MarkerPath      string    `json:"marker_path"`
	P99Ms           float64   `json:"p99_ms"`
	RawLogFiles     []string  `json:"raw_log_files"`
	OfficialReady   bool      `json:"official_ready"`
	IncompleteNotes []string  `json:"incomplete_notes"`
}

func parseSysbenchRawWindow(paths []string, start, end time.Time, expected uint64, expectedThreads int) (sysbenchRawWindow, error) {
	if !start.Before(end) || len(paths) == 0 || expected == 0 || expectedThreads < 1 {
		return sysbenchRawWindow{}, errors.New("nonempty log files, start < end, positive expected-events and threads are required")
	}
	if len(paths) != expectedThreads {
		return sysbenchRawWindow{}, fmt.Errorf("found %d worker files, expected %d", len(paths), expectedThreads)
	}
	res := sysbenchRawWindow{Backend: "sysbench-instrumented", Start: start, End: end,
		ExpectedEvents: expected, ExpectedThreads: expectedThreads,
		RawLogFiles: append([]string(nil), paths...)}
	var lat []uint64
	seenThreads := map[uint64]bool{}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return sysbenchRawWindow{}, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024), 1024*1024)
		var thread, previous, lastTimestamp uint64
		lineNo := 0
		footerSeen := false
		records := 0
		for sc.Scan() {
			lineNo++
			fields := strings.Fields(sc.Text())
			if len(fields) > 0 && fields[0] == "#" {
				if footerSeen || len(fields) != 5 || fields[1] != "STRAND_RAW_THREAD" {
					f.Close()
					return sysbenchRawWindow{}, fmt.Errorf("%s:%d: invalid thread footer", path, lineNo)
				}
				footerValues := map[string]uint64{}
				for _, field := range fields[2:] {
					pair := strings.SplitN(field, "=", 2)
					if len(pair) != 2 {
						f.Close()
						return sysbenchRawWindow{}, fmt.Errorf("%s:%d: invalid footer field", path, lineNo)
					}
					value, parseErr := strconv.ParseUint(pair[1], 10, 64)
					if parseErr != nil {
						f.Close()
						return sysbenchRawWindow{}, fmt.Errorf("%s:%d: invalid footer number: %w", path, lineNo, parseErr)
					}
					footerValues[pair[0]] = value
				}
				footerThread, hasThread := footerValues["thread_id"]
				footerCount, hasCount := footerValues["count"]
				footerErrors, hasErrors := footerValues["errors"]
				if len(footerValues) != 3 || !hasThread || !hasCount || !hasErrors ||
					(records > 0 && footerThread != thread) || footerCount != uint64(records) || footerErrors != 0 ||
					seenThreads[footerThread] {
					f.Close()
					return sysbenchRawWindow{}, fmt.Errorf("%s:%d: footer count, thread or error mismatch", path, lineNo)
				}
				seenThreads[footerThread] = true
				footerSeen = true
				continue
			}
			if footerSeen {
				f.Close()
				return sysbenchRawWindow{}, fmt.Errorf("%s:%d: records after thread footer", path, lineNo)
			}
			if len(fields) != 4 {
				f.Close()
				return sysbenchRawWindow{}, fmt.Errorf("%s:%d: expected four fields", path, lineNo)
			}
			vals := [4]uint64{}
			for i, value := range fields {
				vals[i], err = strconv.ParseUint(value, 10, 64)
				if err != nil {
					f.Close()
					return sysbenchRawWindow{}, fmt.Errorf("%s:%d: field %d: %w", path, lineNo, i+1, err)
				}
			}
			if records == 0 {
				thread = vals[0]
			}
			if vals[0] != thread || vals[1] != previous+1 || vals[2] > math.MaxInt64 {
				f.Close()
				return sysbenchRawWindow{}, fmt.Errorf("%s:%d: thread/sequence/timestamp invalid", path, lineNo)
			}
			if records > 0 && vals[2] < lastTimestamp {
				f.Close()
				return sysbenchRawWindow{}, fmt.Errorf("%s:%d: realtime completion timestamp moved backwards", path, lineNo)
			}
			previous = vals[1]
			lastTimestamp = vals[2]
			records++
			res.TotalRawEvents++
			at := time.Unix(0, int64(vals[2])).UTC()
			if !at.Before(start) && at.Before(end) {
				lat = append(lat, vals[3])
			}
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return sysbenchRawWindow{}, fmt.Errorf("%s: %w", path, err)
		}
		if err := f.Close(); err != nil {
			return sysbenchRawWindow{}, err
		}
		if !footerSeen {
			return sysbenchRawWindow{}, fmt.Errorf("%s: missing thread footer", path)
		}
	}
	for i := 0; i < expectedThreads; i++ {
		if !seenThreads[uint64(i)] {
			return sysbenchRawWindow{}, fmt.Errorf("missing worker thread %d", i)
		}
	}
	if res.TotalRawEvents != expected {
		return sysbenchRawWindow{}, fmt.Errorf("raw event count %d does not match expected %d", res.TotalRawEvents, expected)
	}
	if len(lat) == 0 {
		return sysbenchRawWindow{}, errors.New("no raw events completed inside snapshot window")
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	res.Transactions = len(lat)
	res.P99Ms = float64(lat[int(math.Ceil(.99*float64(len(lat))))-1]) / 1e6
	res.IncompleteNotes = []string{"Raw p99 only; audited binary build, timing overhead, restored fixture, wave, Strand telemetry and final compare remain required"}
	return res, nil
}

func parseSysbenchRawMarker(stderrPath string) (uint64, error) {
	file, err := os.Open(stderrPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var count uint64
	found := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "STRAND_RAW_EVENTS ") {
			continue
		}
		if found {
			return 0, errors.New("duplicate STRAND_RAW_EVENTS markers")
		}
		found = true
		fields := strings.Fields(line)
		if len(fields) != 4 || !strings.HasPrefix(fields[1], "count=") ||
			fields[2] != "errors=0" || fields[3] != "run_status=0" {
			return 0, fmt.Errorf("invalid or failing STRAND_RAW_EVENTS marker %q", line)
		}
		count, err = strconv.ParseUint(strings.TrimPrefix(fields[1], "count="), 10, 64)
		if err != nil || count == 0 {
			return 0, fmt.Errorf("invalid STRAND_RAW_EVENTS count %q", fields[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("missing STRAND_RAW_EVENTS marker")
	}
	return count, nil
}

package breaker

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// TelemetryParser parses and aligns telemetry data from CSV.
type TelemetryParser struct {
	missingMetricsCount int
}

func NewTelemetryParser() *TelemetryParser {
	return &TelemetryParser{}
}

// ParseCSV reads telemetry records from a CSV reader.
func (p *TelemetryParser) ParseCSV(r io.Reader) ([]TelemetryRow, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Allow variable fields if needed

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("csv is empty")
	}

	header := records[0]
	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	var rows []TelemetryRow
	for lineNum, rec := range records[1:] {
		row, ok := p.parseRow(rec, colIdx, lineNum+2)
		if ok {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// parseTimestamp handles Unix nanoseconds, Unix seconds, RFC3339, and custom time formats.
func parseTimestamp(val string) (time.Time, int64, error) {
	val = strings.TrimSpace(val)
	if val == "" {
		return time.Time{}, 0, fmt.Errorf("empty timestamp")
	}

	// Try numeric unix ns or unix sec
	if num, err := strconv.ParseInt(val, 10, 64); err == nil {
		if num > 1e16 {
			// Nanoseconds
			return time.Unix(0, num).UTC(), num, nil
		} else if num > 1e12 {
			// Milliseconds
			return time.Unix(num/1000, (num%1000)*1e6).UTC(), num * 1e6, nil
		} else {
			// Seconds
			return time.Unix(num, 0).UTC(), num * 1e9, nil
		}
	}

	// Try floating point unix seconds
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), sec*1e9 + nsec, nil
	}

	// Try RFC3339 / ISO8601
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
	}
	for _, layout := range formats {
		if t, err := time.Parse(layout, val); err == nil {
			return t.UTC(), t.UnixNano(), nil
		}
	}

	return time.Time{}, 0, fmt.Errorf("unrecognized timestamp format: %s", val)
}

func getField(rec []string, colIdx map[string]int, aliases ...string) string {
	for _, alias := range aliases {
		if idx, ok := colIdx[alias]; ok && idx < len(rec) {
			return strings.TrimSpace(rec[idx])
		}
	}
	return ""
}

func (p *TelemetryParser) parseRow(rec []string, colIdx map[string]int, lineNum int) (TelemetryRow, bool) {
	var row TelemetryRow

	// 1. Timestamp
	tsRaw := getField(rec, colIdx, "timestamp", "t_unix_ns", "t", "time")
	if tsRaw == "" {
		p.missingMetricsCount++
		return row, false
	}
	t, unixNs, err := parseTimestamp(tsRaw)
	if err != nil {
		p.missingMetricsCount++
		return row, false
	}
	row.TimestampRaw = tsRaw
	row.ParsedTime = t
	row.UnixNs = unixNs

	// 2. R (Rate)
	rRaw := getField(rec, colIdx, "r", "rate", "throughput")
	if rVal, err := strconv.ParseFloat(rRaw, 64); err == nil {
		row.R = rVal
	} else {
		p.missingMetricsCount++
		return row, false
	}

	// 3. Breaker
	bRaw := getField(rec, colIdx, "breaker", "breaker_open", "is_breaker")
	if bVal, err := strconv.Atoi(bRaw); err == nil {
		row.Breaker = bVal
	}

	// 4. Zone
	zRaw := getField(rec, colIdx, "zone")
	if zVal, err := strconv.Atoi(zRaw); err == nil {
		row.Zone = zVal
	}

	// 5. Pressure
	pRaw := getField(rec, colIdx, "pressure")
	if pVal, err := strconv.ParseFloat(pRaw, 64); err == nil {
		row.Pressure = pVal
	}

	// 6. Lag
	lagRaw := getField(rec, colIdx, "lag_s", "lag", "replica_lag")
	if lagVal, err := strconv.ParseFloat(lagRaw, 64); err == nil {
		row.LagSeconds = lagVal
	}

	// 7. P99
	p99Raw := getField(rec, colIdx, "p99", "p99_ms", "query_p99")
	if p99Val, err := strconv.ParseFloat(p99Raw, 64); err == nil {
		row.P99 = p99Val
	}

	// 8. Baseline
	blineRaw := getField(rec, colIdx, "baseline", "baseline_ms", "query_baseline")
	if blineVal, err := strconv.ParseFloat(blineRaw, 64); err == nil {
		row.Baseline = blineVal
	}

	// 9. Ratio
	ratioRaw := getField(rec, colIdx, "ratio", "latency_ratio")
	if ratioVal, err := strconv.ParseFloat(ratioRaw, 64); err == nil {
		row.Ratio = ratioVal
	} else if row.Baseline > 0 && row.P99 > 0 {
		row.Ratio = row.P99 / row.Baseline
	}

	// 10. Breaker Reason
	row.BreakerReason = getField(rec, colIdx, "breaker_reason", "reason")

	// 11. CPU and I/O Wait (optional)
	cpuRaw := getField(rec, colIdx, "cpu", "cpu_pct", "system_cpu")
	if cpuVal, err := strconv.ParseFloat(cpuRaw, 64); err == nil {
		row.CPUPct = &cpuVal
	}
	ioRaw := getField(rec, colIdx, "iowait", "io_wait", "iowait_pct")
	if ioVal, err := strconv.ParseFloat(ioRaw, 64); err == nil {
		row.IOWaitPct = &ioVal
	}

	return row, true
}

// AnalyzeP2P evaluates the peak-to-peak log ratio ln(R_max / R_min) on telemetry windows,
// strictly PRESERVING all breaker samples and formulas, and separating Direct Cause from Latency Cause.
func AnalyzeP2P(rows []TelemetryRow, windowSize int) (*P2PAnalysisResult, error) {
	if len(rows) < 2 {
		return nil, fmt.Errorf("insufficient rows for p2p analysis (%d)", len(rows))
	}
	if windowSize <= 0 || windowSize > len(rows) {
		windowSize = len(rows)
	}

	var bestRatio float64 = -1.0
	var bestWindowIdx int = -1
	var bestMax TelemetryRow
	var bestMin TelemetryRow

	// Slide window across samples WITHOUT altering formula or removing breaker samples
	for start := 0; start <= len(rows)-windowSize; start++ {
		win := rows[start : start+windowSize]
		var rMax, rMin float64 = -1.0, math.MaxFloat64
		var maxRow, minRow TelemetryRow

		for _, row := range win {
			if row.R <= 0 {
				continue
			}
			if row.R > rMax {
				rMax = row.R
				maxRow = row
			}
			if row.R < rMin {
				rMin = row.R
				minRow = row
			}
		}

		if rMax > 0 && rMin > 0 && rMin < math.MaxFloat64 {
			logRatio := math.Log(rMax / rMin)
			if logRatio > bestRatio {
				bestRatio = logRatio
				bestWindowIdx = start
				bestMax = maxRow
				bestMin = minRow
			}
		}
	}

	if bestRatio < 0 {
		return nil, fmt.Errorf("could not compute valid p2p ratio: all R values non-positive")
	}

	result := &P2PAnalysisResult{
		WorstLogRatio: bestRatio,
		WindowIndex:   bestWindowIdx,
		MaximumSample: bestMax,
		MinimumSample: bestMin,
	}

	// Evaluate Direct Cause vs Latency Cause
	if bestMin.Breaker == 1 || bestMin.Ratio > 3.0 || strings.Contains(bestMin.BreakerReason, "breaker") {
		result.DirectCause = fmt.Sprintf(
			"Circuit breaker opened: R reduced from %.1f to %.1f (ln(%.1f/%.1f) = %.4f). Breaker flag=%d, pressure=%.3f, zone=%d.",
			bestMax.R, bestMin.R, bestMax.R, bestMin.R, bestRatio, bestMin.Breaker, bestMin.Pressure, bestMin.Zone,
		)
		if bestMin.Ratio > 0 {
			result.DirectCause += fmt.Sprintf(" P99 latency ratio reached %.2f (threshold 3.0).", bestMin.Ratio)
		}
	} else {
		result.DirectCause = fmt.Sprintf(
			"Governor throttling or load variation: R shifted from %.1f to %.1f (ln ratio %.4f) without breaker flag.",
			bestMax.R, bestMin.R, bestRatio,
		)
	}

	// Latency Cause requires DB log evidence to close
	result.LatencyCauseStatus = "UNRESOLVED"
	result.LatencyCause = "Latency spike root cause cannot be declared without database diagnostic capture during incident"
	result.DiagnosisNotes = append(result.DiagnosisNotes,
		"Direct Cause confirmed: Governor circuit breaker trip halved/throttled rate R.",
		"Latency Cause status: UNRESOLVED. No DB SHOW ENGINE INNODB STATUS or counter deltas captured on host during incident.",
		"Minimal evidence required to close: Diagnostic snapshot (Innodb_log_waits delta, Innodb_buffer_pool_wait_free delta, or storage fsync stall) aligned with telemetry timestamp.",
	)

	return result, nil
}

// LoadTelemetryCSV loads a telemetry CSV file.
func LoadTelemetryCSV(path string) ([]TelemetryRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	parser := NewTelemetryParser()
	return parser.ParseCSV(f)
}

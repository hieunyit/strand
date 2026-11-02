package verify

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/zeebo/xxh3"
)

var (
	metricInitOnce sync.Once
	mismatchMetric *prometheus.CounterVec
	hotRowMetric   *prometheus.CounterVec
)

func initMetrics() {
	metricInitOnce.Do(func() {
		mismatchMetric = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "strand_verify_mismatch_total",
				Help: "Total count of data mismatches detected during verify",
			},
			[]string{"table"},
		)
		hotRowMetric = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "strand_verify_unverifiable_hot_total",
				Help: "Total count of hot rows reported as unverifiable_hot",
			},
			[]string{"table"},
		)
		_ = prometheus.Register(mismatchMetric)
		_ = prometheus.Register(hotRowMetric)
	})
}

// Verifier thực hiện việc so sánh và đối chiếu dữ liệu giữa source và target.
// spec: 02 §12, design.md §7
type Verifier struct {
	source       RowFetcher
	target       RowFetcher
	imageTracker *ImageTracker
}

// NewVerifier khởi tạo Verifier.
func NewVerifier(source, target RowFetcher, tracker *ImageTracker) *Verifier {
	initMetrics()
	if tracker == nil {
		tracker = NewImageTracker(5, 30*time.Second)
	}
	return &Verifier{
		source:       source,
		target:       target,
		imageTracker: tracker,
	}
}

// VerifyTable kiểm tra đối chiếu dữ liệu của một bảng theo chế độ chỉ định.
func (v *Verifier) VerifyTable(ctx context.Context, cfg TableConfig, mode Mode) (*VerifyResult, error) {
	startTime := time.Now()
	res := &VerifyResult{
		Mode:            mode,
		Table:           fmt.Sprintf("%s.%s", cfg.Schema, cfg.Table),
		Mismatches:      make([]Mismatch, 0),
		UnverifiableHot: make([]HotRowReport, 0),
	}

	pkCol := "id"
	if len(cfg.PKColumns) > 0 {
		pkCol = cfg.PKColumns[0]
	}

	// 1. Xác định các range cần kiểm tra
	var ranges [][2]any

	switch mode {
	case ModeDelta:
		// Chế độ delta: chỉ lấy các range PK vừa xuất hiện trong luồng CDC
		if len(cfg.DeltaRanges) > 0 {
			ranges = cfg.DeltaRanges
		} else if v.imageTracker != nil {
			ranges = v.imageTracker.GetDeltaRanges(cfg.Schema, cfg.Table)
		}

	case ModeSample:
		// Chế độ sample: lấy mẫu ngẫu nhiên một tỉ lệ các range
		minVal, maxVal, count, err := v.source.GetPKRange(ctx, cfg.Schema, cfg.Table, pkCol)
		if err != nil {
			return nil, fmt.Errorf("failed to get PK range for sample: %w", err)
		}
		if count > 0 && minVal != nil && maxVal != nil {
			allRanges := buildRanges(minVal, maxVal, count, cfg.ChunkSize)
			sampleRate := cfg.SampleRate
			if sampleRate <= 0 {
				sampleRate = 0.1 // Mặc định lấy mẫu 10%
			}
			ranges = sampleSubset(allRanges, sampleRate)
		}

	case ModeFull:
		// Chế độ full: quét toàn bộ các range
		minVal, maxVal, count, err := v.source.GetPKRange(ctx, cfg.Schema, cfg.Table, pkCol)
		if err != nil {
			return nil, fmt.Errorf("failed to get PK range for full: %w", err)
		}
		if count > 0 && minVal != nil && maxVal != nil {
			ranges = buildRanges(minVal, maxVal, count, cfg.ChunkSize)
		}
	}

	// 2. Đối chiếu từng range
	for _, rng := range ranges {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		res.RangesChecked++

		low, high := rng[0], rng[1]
		srcRows, err := v.source.FetchRange(ctx, cfg.Schema, cfg.Table, pkCol, low, high)
		if err != nil {
			return nil, fmt.Errorf("source fetch range failed [%v, %v]: %w", low, high, err)
		}
		dstRows, err := v.target.FetchRange(ctx, cfg.Schema, cfg.Table, pkCol, low, high)
		if err != nil {
			return nil, fmt.Errorf("target fetch range failed [%v, %v]: %w", low, high, err)
		}

		res.RowsChecked += len(srcRows)

		srcHash := hashRows(srcRows, cfg.ValueColumns)
		dstHash := hashRows(dstRows, cfg.ValueColumns)

		if srcHash == dstHash {
			// Hash khớp -> range sạch
			continue
		}

		// Hash lệch!
		// spec: design.md §7:
		// "1. Range nào lệch hash thì so từng PK thay vì so cả range."
		srcMap := indexRowsByPK(srcRows, pkCol)
		dstMap := indexRowsByPK(dstRows, pkCol)

		// Gom tập hợp tất cả các PK xuất hiện ở một trong hai phía
		allPKs := make(map[any]struct{})
		for pk := range srcMap {
			allPKs[pk] = struct{}{}
		}
		for pk := range dstMap {
			allPKs[pk] = struct{}{}
		}

		for pk := range allPKs {
			sRow, hasSource := srcMap[pk]
			dRow, hasTarget := dstMap[pk]

			if hasSource && hasTarget && matchRow(sRow, dRow) {
				continue
			}

			// Có sự khác biệt ở PK này!
			// spec: design.md §7:
			// "2. Với PK có event trong cửa sổ đọc [low, high]: kiểm tra giá trị đọc được ở source
			// có nằm trong tập {trạng thái trước cửa sổ, các image của event trong cửa sổ} hay không (image-membership check)."
			var isMember, isHot bool
			var eventCount int
			if v.imageTracker != nil {
				isMember, isHot, eventCount = v.imageTracker.Check(cfg.Schema, cfg.Table, pk, sRow, dRow)
			}

			if isHot {
				// spec: design.md §7:
				// "3. Row vẫn không kiểm được, vì event quá dày đến mức không có cửa sổ nào yên,
				// được báo riêng là unverifiable_hot, không báo là lệch. Nhờ đó người vận hành không phải chịu báo động giả."
				res.UnverifiableHot = append(res.UnverifiableHot, HotRowReport{
					Schema:     cfg.Schema,
					Table:      cfg.Table,
					PK:         pk,
					EventCount: eventCount,
					Reason:     "row receives rapid concurrent updates during verify window",
				})
				if hotRowMetric != nil {
					hotRowMetric.WithLabelValues(res.Table).Inc()
				}
				continue
			}

			if isMember {
				// Khớp với một image hợp lệ trong cửa sổ CDC -> row đang bay, hợp lệ, không phải lệch
				continue
			}

			// Không phải image hợp lệ và không phải hot row -> LỆCH THẬT (REAL MISMATCH)
			res.Mismatches = append(res.Mismatches, Mismatch{
				Schema:      cfg.Schema,
				Table:       cfg.Table,
				PK:          pk,
				SourceValue: sRow,
				TargetValue: dRow,
				Reason:      "content mismatch between source and target",
			})
			if mismatchMetric != nil {
				mismatchMetric.WithLabelValues(res.Table).Inc()
			}
		}
	}

	res.Duration = time.Since(startTime)
	return res, nil
}

func indexRowsByPK(rows []map[string]any, pkCol string) map[any]map[string]any {
	m := make(map[any]map[string]any, len(rows))
	for _, r := range rows {
		if pk, ok := r[pkCol]; ok {
			m[pkMapKey(pk)] = r
		}
	}
	return m
}

// pkMapKey chuẩn hóa giá trị PK để dùng làm khóa map: []byte (MySQL trả về khi scan
// vào any) không hash được nên đổi sang string.
func pkMapKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

func hashRows(rows []map[string]any, cols []string) uint64 {
	if len(rows) == 0 {
		return 0
	}
	// Sắp xếp rows để đảm bảo hash có tính tất định
	h := xxh3.New()
	for _, r := range rows {
		data, _ := json.Marshal(r)
		h.Write(data)
	}
	return h.Sum64()
}

func buildRanges(minVal, maxVal any, count int64, chunkSize int) [][2]any {
	if chunkSize <= 0 {
		chunkSize = 100
	}
	// Hỗ trợ chia range cho số nguyên
	minInt, ok1 := toInt64(minVal)
	maxInt, ok2 := toInt64(maxVal)
	if !ok1 || !ok2 || minInt > maxInt {
		return [][2]any{{minVal, maxVal}}
	}

	var ranges [][2]any
	curr := minInt
	for curr <= maxInt {
		next := curr + int64(chunkSize) - 1
		if next > maxInt {
			next = maxInt
		}
		ranges = append(ranges, [2]any{curr, next})
		curr = next + 1
	}
	return ranges
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func sampleSubset(ranges [][2]any, rate float64) [][2]any {
	if len(ranges) == 0 {
		return nil
	}
	targetCount := int(float64(len(ranges)) * rate)
	if targetCount < 1 {
		targetCount = 1
	}
	if targetCount >= len(ranges) {
		return ranges
	}

	// Chọn ngẫu nhiên mẫu con
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	indices := r.Perm(len(ranges))[:targetCount]
	sort.Ints(indices)

	selected := make([][2]any, 0, targetCount)
	for _, idx := range indices {
		selected = append(selected, ranges[idx])
	}
	return selected
}

// SQLRowFetcher hiện thực RowFetcher cho cơ sở dữ liệu quan hệ (MySQL / PostgreSQL).
type SQLRowFetcher struct {
	db *sql.DB
}

func NewSQLRowFetcher(db *sql.DB) *SQLRowFetcher {
	return &SQLRowFetcher{db: db}
}

func (f *SQLRowFetcher) FetchRange(ctx context.Context, schema, table string, pkCol string, low, high any) ([]map[string]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s >= ? AND %s <= ? ORDER BY %s", schema, table, pkCol, pkCol, pkCol)
	rows, err := f.db.QueryContext(ctx, query, low, high)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

func (f *SQLRowFetcher) FetchRowByPK(ctx context.Context, schema, table string, pkCol string, pk any) (map[string]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = ?", schema, table, pkCol)
	rows, err := f.db.QueryContext(ctx, query, pk)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

func (f *SQLRowFetcher) GetPKRange(ctx context.Context, schema, table string, pkCol string) (min, max any, count int64, err error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s.%s", pkCol, pkCol, schema, table)
	row := f.db.QueryRowContext(ctx, query)
	var minVal, maxVal sql.NullInt64
	if err := row.Scan(&minVal, &maxVal, &count); err != nil {
		return nil, nil, 0, err
	}
	if minVal.Valid {
		min = minVal.Int64
	}
	if maxVal.Valid {
		max = maxVal.Int64
	}
	return min, max, count, nil
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		m := make(map[string]any, len(cols))
		for i, col := range cols {
			val := vals[i]
			if b, ok := val.([]byte); ok {
				m[col] = string(b)
			} else {
				m[col] = val
			}
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// MemoryRowFetcher là in-memory fetcher cho kiểm thử nhanh và các kịch bản mô phỏng.
type MemoryRowFetcher struct {
	mu   sync.RWMutex
	rows []map[string]any
}

func NewMemoryRowFetcher(rows []map[string]any) *MemoryRowFetcher {
	return &MemoryRowFetcher{rows: rows}
}

func (m *MemoryRowFetcher) UpdateRow(pkCol string, pk any, update map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, r := range m.rows {
		if r[pkCol] == pk {
			cp := make(map[string]any)
			for k, v := range r {
				cp[k] = v
			}
			for k, v := range update {
				cp[k] = v
			}
			m.rows[i] = cp
			return
		}
	}
}

func (m *MemoryRowFetcher) DeleteRow(pkCol string, pk any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var filtered []map[string]any
	for _, r := range m.rows {
		if r[pkCol] != pk {
			filtered = append(filtered, r)
		}
	}
	m.rows = filtered
}

func (m *MemoryRowFetcher) FetchRange(ctx context.Context, schema, table string, pkCol string, low, high any) ([]map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lowInt, _ := toInt64(low)
	highInt, _ := toInt64(high)

	var res []map[string]any
	for _, r := range m.rows {
		val, ok := toInt64(r[pkCol])
		if ok && val >= lowInt && val <= highInt {
			cp := make(map[string]any)
			for k, v := range r {
				cp[k] = v
			}
			res = append(res, cp)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		a, _ := toInt64(res[i][pkCol])
		b, _ := toInt64(res[j][pkCol])
		return a < b
	})
	return res, nil
}

func (m *MemoryRowFetcher) FetchRowByPK(ctx context.Context, schema, table string, pkCol string, pk any) (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, r := range m.rows {
		if r[pkCol] == pk {
			return r, nil
		}
	}
	return nil, nil
}

func (m *MemoryRowFetcher) GetPKRange(ctx context.Context, schema, table string, pkCol string) (min, max any, count int64, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.rows) == 0 {
		return nil, nil, 0, nil
	}
	minInt, _ := toInt64(m.rows[0][pkCol])
	maxInt := minInt
	for _, r := range m.rows {
		v, ok := toInt64(r[pkCol])
		if ok {
			if v < minInt {
				minInt = v
			}
			if v > maxInt {
				maxInt = v
			}
		}
	}
	return minInt, maxInt, int64(len(m.rows)), nil
}

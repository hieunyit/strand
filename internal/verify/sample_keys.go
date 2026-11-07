package verify

// verify --mode sample (và mẫu ~1% của post-seed verify) cho PK tổng quát ở mọi cặp engine (PG→PG,
// MySQL→PG): chọn ngẫu nhiên điểm bắt đầu theo khóa SOURCE, mỗi điểm mở một range `rows` khóa (keyset
// trên source, cùng cận codec như full verify theo khóa), rồi so từng range bằng chuỗi chính tắc và kiểm
// lại row lệch (compareKeyChunks). Chi phí đọc bị chặn theo --rate:
//   - source PG: điểm bắt đầu lấy bằng TABLESAMPLE SYSTEM(rate×100%) — PG đọc khoảng rate số trang;
//   - source MySQL, cột PK đầu là số nguyên: giá trị ngẫu nhiên trong [min, max] của cột đầu rồi một lần
//     seek "cột đầu ≥ v ORDER BY PK LIMIT 1";
//   - source MySQL, cột PK đầu không phải số nguyên: không có cách chọn ngẫu nhiên bị chặn (MySQL không có
//     TABLESAMPLE) — lùi về keyset walk cả index PK như MySQL→MySQL (SampleKeyRanges).
// Số range n = est×rate/rows + 1 (est: reltuples / TABLE_ROWS), như SampleRangesEngine; tổng số row đọc
// ≈ n×rows ≈ rate × số row. Range chồng lên range trước bị bỏ (đã được so).

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// keySampleCount: số range mẫu cho bảng ước lượng est row, tỉ lệ rate, mỗi range rows khóa (≥ 1).
func keySampleCount(est int64, rate float64, rows int64) int {
	if est < 0 {
		est = 0
	}
	if rows <= 0 {
		rows = 1000
	}
	if rate <= 0 {
		return 1
	}
	f := float64(est) * rate / float64(rows)
	if f > 1e6 {
		f = 1e6 // chặn trên: tránh tràn int với est lỗi
	}
	return int(f) + 1
}

// tableSamplePercent: tham số của TABLESAMPLE SYSTEM cho tỉ lệ rate — rate×100 (%), trong (0, 100].
func tableSamplePercent(rate float64) float64 {
	p := rate * 100
	switch {
	case p > 100 || math.IsNaN(p):
		return 100
	case p < 0.0001:
		return 0.0001
	}
	return p
}

// randInt64In: số ngẫu nhiên trong [lo, hi] (lo ≤ hi), không tràn khi khoảng rộng hơn MaxInt64.
func randInt64In(rng *rand.Rand, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	span := uint64(hi) - uint64(lo)
	if span == math.MaxUint64 {
		return int64(rng.Uint64())
	}
	if span < math.MaxInt64 {
		return lo + rng.Int63n(int64(span)+1)
	}
	return lo + int64(rng.Uint64()%(span+1))
}

// randUint64In: số ngẫu nhiên trong [lo, hi] (lo ≤ hi).
func randUint64In(rng *rand.Rand, lo, hi uint64) uint64 {
	if hi <= lo {
		return lo
	}
	span := hi - lo
	if span == math.MaxUint64 {
		return rng.Uint64()
	}
	return lo + rng.Uint64()%(span+1)
}

// randomLead: giá trị ngẫu nhiên của cột PK đầu (số nguyên) giữa khóa nhỏ nhất mn và lớn nhất mx;
// ok=false nếu cột đầu không phải số nguyên (không chọn ngẫu nhiên được trong không gian giá trị).
func randomLead(rng *rand.Rand, codec *snapshot.PKCodec, mn, mx snapshot.Bound) (any, bool) {
	if len(codec.Cols) == 0 || len(mn.V) == 0 || len(mx.V) == 0 {
		return nil, false
	}
	switch codec.Cols[0].Kind {
	case snapshot.PKInt:
		lo, ok1 := mn.V[0].(int64)
		hi, ok2 := mx.V[0].(int64)
		if !ok1 || !ok2 {
			return nil, false
		}
		return randInt64In(rng, lo, hi), true
	case snapshot.PKUint:
		lo, ok1 := mn.V[0].(uint64)
		hi, ok2 := mx.V[0].(uint64)
		if !ok1 || !ok2 {
			return nil, false
		}
		return randUint64In(rng, lo, hi), true
	}
	return nil, false
}

// planKeyStarts: điểm bắt đầu đã sắp, bỏ trùng, tối đa n; rỗng thì bắt đầu ở khóa nhỏ nhất mn (luôn ít
// nhất một range).
func planKeyStarts(cands []snapshot.Bound, n int, mn snapshot.Bound) []snapshot.Bound {
	seen := map[snapshot.Key]bool{}
	var out []snapshot.Bound
	for _, b := range cands {
		if seen[b.K] {
			continue
		}
		seen[b.K] = true
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].K < out[j].K })
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	if len(out) == 0 {
		out = []snapshot.Bound{mn}
	}
	return out
}

// rangesFromStarts: mỗi điểm bắt đầu (đã sắp) thành range [start, end(start)]; điểm nằm trong range
// trước (≤ end trước) bị bỏ — các range không chồng nhau.
func rangesFromStarts(starts []snapshot.Bound, end func(lo snapshot.Bound) (snapshot.Bound, error)) ([]KeyRange, error) {
	var out []KeyRange
	for _, s := range starts {
		if len(out) > 0 && s.K <= out[len(out)-1].Hi.K {
			continue
		}
		e, err := end(s)
		if err != nil {
			return nil, err
		}
		out = append(out, KeyRange{Lo: s, Hi: e})
	}
	return out, nil
}

// SampleKeyRangesEngine chọn các range mẫu theo tuple PK (codec của source) cho source thuộc engine bất
// kỳ — xem đầu file. Bảng source rỗng: một range All (mọi row) để bắt row thừa ở target. rate ≥ 1: mọi
// range của keyset walk.
func SampleKeyRangesEngine(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, codec *snapshot.PKCodec, rate float64, rows int64, rng *rand.Rand) ([]KeyRange, error) {
	if rows <= 0 {
		rows = 1000
	}
	var bounds func() (snapshot.Bound, snapshot.Bound, bool, error)
	var end func(lo, max snapshot.Bound) (snapshot.Bound, error)
	var cs *snapshot.PGChunkSource
	mq := snapshot.MySQLKeySQL{Table: t, Codec: codec}
	if engine == model.EnginePG {
		cs = &snapshot.PGChunkSource{DB: db}
		bounds = func() (snapshot.Bound, snapshot.Bound, bool, error) { return cs.KeyBounds(ctx, t, codec) }
		end = func(lo, max snapshot.Bound) (snapshot.Bound, error) {
			return cs.KeyKeysetEnd(ctx, t, codec, lo, max, rows)
		}
	} else {
		bounds = func() (snapshot.Bound, snapshot.Bound, bool, error) { return mq.Bounds(ctx, db) }
		end = func(lo, max snapshot.Bound) (snapshot.Bound, error) { return mq.KeysetEnd(ctx, db, lo, max, rows) }
	}
	mn, mx, ok, err := bounds()
	if err != nil {
		return nil, fmt.Errorf("bounds of %s: %w", t, err)
	}
	if !ok {
		return []KeyRange{{All: true}}, nil
	}
	if rate >= 1 {
		var out []KeyRange
		lo := mn
		for {
			e, err := end(lo, mx)
			if err != nil {
				return nil, err
			}
			out = append(out, KeyRange{Lo: lo, Hi: e})
			if e.K >= mx.K {
				return out, nil
			}
			next, ok := snapshot.After(codec, e)
			if !ok {
				return out, nil
			}
			lo = next
		}
	}
	var est int64
	if engine == model.EnginePG {
		_ = db.QueryRowContext(ctx, "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = to_regclass($1)", QuoteTable(engine, t)).Scan(&est)
	} else {
		_ = db.QueryRowContext(ctx, "SELECT COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			t.Schema, t.Table).Scan(&est)
	}
	n := keySampleCount(est, rate, rows)
	var cands []snapshot.Bound
	if engine == model.EnginePG {
		if cands, err = cs.KeySample(ctx, t, codec, tableSamplePercent(rate), n); err != nil {
			return nil, err
		}
	} else {
		if _, ok := randomLead(rng, codec, mn, mx); !ok {
			// Cột PK đầu không phải số nguyên: keyset walk (không bị chặn theo rate, xem đầu file).
			return SampleKeyRanges(ctx, db, t, codec, rate, rows, rng)
		}
		for i := 0; i < n; i++ {
			v, _ := randomLead(rng, codec, mn, mx)
			b, ok, err := mq.LeadSeek(ctx, db, v)
			if err != nil {
				return nil, fmt.Errorf("sample keys of %s: %w", t, err)
			}
			if ok {
				cands = append(cands, b)
			}
		}
	}
	return rangesFromStarts(planKeyStarts(cands, n, mn), func(lo snapshot.Bound) (snapshot.Bound, error) { return end(lo, mx) })
}

// VerifyKeyRanges so từng row các range theo tuple PK (cận theo srcCodec) giữa hai phía bằng chuỗi chính
// tắc và kiểm lại row lệch — sample cho bảng PK tổng quát ở cặp engine mà LiveVerifier không so được.
// Cùng điều kiện với VerifyTableFullKeys: hai codec cùng cột, cùng thứ tự khóa (caller đã kiểm; bảng
// "khóa chính xác" khác thứ tự — merkle_cross.go — không so theo range được).
func (mv *MerkleVerifier) VerifyKeyRanges(ctx context.Context, t model.TableID, srcCodec, dstCodec *snapshot.PKCodec, ranges []KeyRange) (*FullVerifyReport, error) {
	ks, err := mv.keySides(ctx, t, srcCodec, dstCodec)
	if err != nil {
		return nil, err
	}
	chunks := make([]keyChunk, len(ranges))
	for i, r := range ranges {
		chunks[i] = keyChunk{id: i, lo: r.Lo, hi: r.Hi, all: r.All}
	}
	rep := &FullVerifyReport{Table: t.String(), TotalChunks: len(chunks)}
	if err := mv.compareKeyChunks(ctx, ks.src, ks.dst, ks.srcCols, ks.dstCols, ks.srcPK, ks.dstPK, chunks, rep); err != nil {
		return nil, err
	}
	return rep, nil
}

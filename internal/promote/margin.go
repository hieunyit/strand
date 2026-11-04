package promote

import "math"

// CalculateMargin tính margin cho sequence theo công thức tại spec/02 §8:
// margin = 2 * cache_size * |increment_by| + 2 * r_peak * Delta_t_sync * |increment_by|
// Margin luôn là độ lớn (>= 0); chiều cộng/trừ do sequence tăng hay giảm quyết định (xem stepAhead).
func CalculateMargin(cacheSize, incrementBy int64, peakRate, syncIntervalSec float64) int64 {
	if cacheSize < 1 {
		cacheSize = 1
	}
	if incrementBy < 0 {
		// Sequence giảm dần (INCREMENT âm): margin theo |increment|, không phải 1.
		if incrementBy == math.MinInt64 {
			incrementBy = math.MaxInt64
		} else {
			incrementBy = -incrementBy
		}
	}
	if incrementBy < 1 {
		incrementBy = 1
	}
	if peakRate < 0 {
		peakRate = 0
	}
	if syncIntervalSec < 0 {
		syncIntervalSec = 0
	}

	part1 := 2 * cacheSize * incrementBy
	part2 := math.Ceil(2.0 * peakRate * syncIntervalSec * float64(incrementBy))

	return part1 + int64(part2)
}

// isDescending: sequence giảm dần (INCREMENT âm) cấp giá trị ngày càng NHỎ.
func isDescending(seq SequenceInfo) bool { return seq.IncrementBy < 0 }

// stepAhead trả v đi thêm margin theo chiều của sequence (tăng: v+margin, giảm: v-margin), bão hòa ở
// biên int64 thay vì tràn.
func stepAhead(seq SequenceInfo, v, margin int64) int64 {
	if isDescending(seq) {
		if v < math.MinInt64+margin {
			return math.MinInt64
		}
		return v - margin
	}
	if v > math.MaxInt64-margin {
		return math.MaxInt64
	}
	return v + margin
}

// further trả giá trị đi xa hơn theo chiều của sequence (tăng: lớn hơn; giảm: nhỏ hơn), tức giá trị
// không bao giờ lùi về phía các giá trị đã cấp. Với sequence giảm dần, 0 nghĩa là "chưa đặt" (trường
// int64 mặc định) nên không được thắng một giá trị thật dương.
func further(seq SequenceInfo, a, b int64) int64 {
	if isDescending(seq) {
		if a == 0 {
			return b
		}
		if b == 0 {
			return a
		}
		if a < b {
			return a
		}
		return b
	}
	if a > b {
		return a
	}
	return b
}

// behind: a nằm "phía sau" b theo chiều của sequence (a gần các giá trị đã cấp hơn b).
func behind(seq SequenceInfo, a, b int64) bool {
	if isDescending(seq) {
		return a > b
	}
	return a < b
}

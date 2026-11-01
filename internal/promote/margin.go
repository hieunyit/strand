package promote

import "math"

// CalculateMargin tính margin cho sequence theo công thức tại spec/02 §8:
// margin = 2 * cache_size * increment_by + 2 * r_peak * Delta_t_sync * increment_by
func CalculateMargin(cacheSize, incrementBy int64, peakRate, syncIntervalSec float64) int64 {
	if cacheSize < 1 {
		cacheSize = 1
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

package checkpoint

import (
	"sort"
)

// Interval biểu diễn một khoảng [Start, End] (bao gồm cả hai đầu, Start <= End).
type Interval struct {
	Start uint64
	End   uint64
}

// RangeSet quản lý tập các số nguyên không âm không trùng lặp được nén thành các khoảng rời rạc đã sắp xếp.
type RangeSet struct {
	intervals []Interval
	base      uint64 // giá trị bắt đầu mong muốn của lwd (thường là 1)
}

// NewRangeSet tạo mới một RangeSet với base quy định (mặc định 1).
func NewRangeSet(base uint64) *RangeSet {
	if base == 0 {
		base = 1
	}
	return &RangeSet{
		intervals: make([]Interval, 0),
		base:      base,
	}
}

// Clone tạo một bản sao độc lập của RangeSet.
func (rs *RangeSet) Clone() *RangeSet {
	if rs == nil {
		return nil
	}
	res := &RangeSet{
		intervals: make([]Interval, len(rs.intervals)),
		base:      rs.base,
	}
	copy(res.intervals, rs.intervals)
	return res
}

// Intervals trả về bản sao các khoảng hiện có trong RangeSet.
func (rs *RangeSet) Intervals() []Interval {
	if rs == nil || len(rs.intervals) == 0 {
		return nil
	}
	res := make([]Interval, len(rs.intervals))
	copy(res, rs.intervals)
	return res
}

// IsEmpty kiểm tra tập có rỗng không.
func (rs *RangeSet) IsEmpty() bool {
	return rs == nil || len(rs.intervals) == 0
}

// Contains kiểm tra xem giá trị val có thuộc tập không.
func (rs *RangeSet) Contains(val uint64) bool {
	if rs.IsEmpty() {
		return false
	}

	// Tìm kiếm nhị phân khoảng chứa val
	idx := sort.Search(len(rs.intervals), func(i int) bool {
		return rs.intervals[i].End >= val
	})

	if idx < len(rs.intervals) {
		return val >= rs.intervals[idx].Start && val <= rs.intervals[idx].End
	}
	return false
}

// Add thêm một giá trị vào tập.
func (rs *RangeSet) Add(val uint64) {
	rs.AddRange(val, val)
}

// AddRange thêm một khoảng [start, end] vào tập và tự động gộp/chuẩn hóa.
func (rs *RangeSet) AddRange(start, end uint64) {
	if start > end {
		return
	}

	newInterval := Interval{Start: start, End: end}
	if len(rs.intervals) == 0 {
		rs.intervals = []Interval{newInterval}
		return
	}

	// Ghép newInterval vào danh sách và gộp
	result := make([]Interval, 0, len(rs.intervals)+1)
	inserted := false

	for _, cur := range rs.intervals {
		if inserted {
			result = append(result, cur)
			continue
		}

		if newInterval.End+1 < cur.Start {
			// newInterval nằm hoàn toàn trước cur
			result = append(result, newInterval, cur)
			inserted = true
		} else if cur.End+1 < newInterval.Start {
			// cur nằm hoàn toàn trước newInterval
			result = append(result, cur)
		} else {
			// Có giao nhau hoặc liền kề: mở rộng newInterval
			if cur.Start < newInterval.Start {
				newInterval.Start = cur.Start
			}
			if cur.End > newInterval.End {
				newInterval.End = cur.End
			}
		}
	}

	if !inserted {
		result = append(result, newInterval)
	}

	// Chuẩn hóa một lần nữa để gộp các khoảng liền kề nếu có
	rs.intervals = normalizeIntervals(result)
}

func normalizeIntervals(src []Interval) []Interval {
	if len(src) <= 1 {
		return src
	}
	out := make([]Interval, 0, len(src))
	cur := src[0]
	for i := 1; i < len(src); i++ {
		next := src[i]
		if cur.End+1 >= next.Start {
			if next.End > cur.End {
				cur.End = next.End
			}
		} else {
			out = append(out, cur)
			cur = next
		}
	}
	out = append(out, cur)
	return out
}

// Merge hợp nhất hai RangeSet (phép hội: rs = rs ∪ other).
func (rs *RangeSet) Merge(other *RangeSet) {
	if other.IsEmpty() {
		return
	}
	for _, it := range other.intervals {
		rs.AddRange(it.Start, it.End)
	}
}

// Lwd trả về mốc liên tục thấp nhất mà mọi giá trị <= lwd đều đã có mặt.
func (rs *RangeSet) Lwd() uint64 {
	if rs.IsEmpty() {
		return 0
	}
	first := rs.intervals[0]
	if first.Start <= rs.base {
		return first.End
	}
	return 0
}

// High trả về giá trị cao nhất đã có mặt trong tập.
func (rs *RangeSet) High() uint64 {
	if rs.IsEmpty() {
		return 0
	}
	return rs.intervals[len(rs.intervals)-1].End
}

// Holes trả về danh sách các giá trị còn thiếu trong khoảng (Lwd, High].
func (rs *RangeSet) Holes() []uint64 {
	if rs.IsEmpty() {
		return nil
	}

	lwd := rs.Lwd()
	high := rs.High()
	if lwd >= high {
		return nil
	}

	var holes []uint64
	expected := lwd + 1

	for _, it := range rs.intervals {
		if it.End <= lwd {
			continue
		}
		for expected < it.Start {
			holes = append(holes, expected)
			expected++
		}
		expected = it.End + 1
	}

	return holes
}

// Equal so sánh tính bằng nhau giữa hai RangeSet.
func (rs *RangeSet) Equal(other *RangeSet) bool {
	if rs == nil && other == nil {
		return true
	}
	if rs == nil || other == nil {
		return false
	}
	if len(rs.intervals) != len(other.intervals) {
		return false
	}
	for i := range rs.intervals {
		if rs.intervals[i] != other.intervals[i] {
			return false
		}
	}
	return true
}

// Subtract loại khỏi tập mọi giá trị thuộc other.
func (rs *RangeSet) Subtract(other *RangeSet) {
	if rs.IsEmpty() || other.IsEmpty() {
		return
	}
	var out []Interval
	for _, cur := range rs.intervals {
		pieces := []Interval{cur}
		for _, o := range other.intervals {
			var next []Interval
			for _, p := range pieces {
				if o.End < p.Start || o.Start > p.End {
					next = append(next, p)
					continue
				}
				if o.Start > p.Start {
					next = append(next, Interval{p.Start, o.Start - 1})
				}
				if o.End < p.End {
					next = append(next, Interval{o.End + 1, p.End})
				}
			}
			pieces = next
			if len(pieces) == 0 {
				break
			}
		}
		out = append(out, pieces...)
	}
	rs.intervals = out
}

// IsSubsetOf kiểm tra mọi giá trị của rs đều thuộc other.
func (rs *RangeSet) IsSubsetOf(other *RangeSet) bool {
	if rs.IsEmpty() {
		return true
	}
	c := rs.Clone()
	c.Subtract(other)
	return c.IsEmpty()
}

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
// Tìm vị trí bằng tìm kiếm nhị phân, sửa slice tại chỗ: O(log n) + memmove,
// O(1) khấu hao khi thêm vào cuối (trường hợp phổ biến).
func (rs *RangeSet) AddRange(start, end uint64) {
	if start > end {
		return
	}
	iv := rs.intervals
	// i: khoảng đầu tiên có thể chạm/giao [start, end] (End+1 >= start).
	i := sort.Search(len(iv), func(k int) bool { return iv[k].End >= start || iv[k].End+1 == start })
	// j: khoảng đầu tiên nằm hẳn sau [start, end] (Start > end+1).
	j := i + sort.Search(len(iv)-i, func(k int) bool { return end != ^uint64(0) && iv[i+k].Start > end+1 })

	if i == j {
		iv = append(iv, Interval{})
		copy(iv[i+1:], iv[i:])
		iv[i] = Interval{Start: start, End: end}
		rs.intervals = iv
		return
	}
	if iv[i].Start < start {
		start = iv[i].Start
	}
	if iv[j-1].End > end {
		end = iv[j-1].End
	}
	iv[i] = Interval{Start: start, End: end}
	rs.intervals = append(iv[:i+1], iv[j:]...)
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

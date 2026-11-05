package snapshot

import (
	"math"

	"strand/internal/model"
)

// Bound là một cận của range khóa (việc C): K là khóa đã mã hóa (pkey.go) — dùng để so trong Go —
// và V là giá trị gốc của từng cột PK để bind vào SQL (WHERE (a,b) > (?,?)). Excl: cận dưới LOẠI TRỪ
// (range bắt đầu ngay SAU khóa này) — dạng tự nhiên của keyset walk với khóa không có "khóa kế tiếp"
// (chuỗi, tuple); với PK một cột số nguyên, cận loại trừ luôn được chuẩn hóa thành cận gồm v+1 để các
// trường int64 cũ (StartPK/EndPK) vẫn đúng.
type Bound struct {
	K    Key
	V    []model.Value
	Excl bool
}

// intCodec: codec của PK một cột số nguyên có dấu (đường int64 cũ, mặc định khi bảng không đăng ký codec).
var intCodec = &PKCodec{Cols: []PKColumn{{Name: "pk", Kind: PKInt}}}

// IntBound là cận gồm khóa số nguyên v.
func IntBound(v int64) Bound {
	return Bound{K: EncodeInt64(v), V: []model.Value{v}}
}

// after trả cận dưới loại trừ ngay sau b; với codec số nguyên: cận gồm b+1 (ok=false nếu b = MaxInt64:
// không còn khóa nào sau b).
func after(codec *PKCodec, b Bound) (Bound, bool) {
	if codec.IsSingleInt() {
		v, err := toInt64(b.V[0])
		if err != nil || v == math.MaxInt64 {
			return Bound{}, false
		}
		return IntBound(v + 1), true
	}
	return Bound{K: b.K, V: b.V, Excl: true}, true
}

// geLo: khóa k nằm ở hoặc trên cận dưới lo.
func geLo(k Key, lo Bound) bool {
	if lo.Excl {
		return k > lo.K
	}
	return k >= lo.K
}

// codecOrInt trả codec của bảng (mặc định: số nguyên).
func codecOrInt(c *PKCodec) *PKCodec {
	if c == nil {
		return intCodec
	}
	return c
}

// syncIntFields cập nhật các trường int64 cũ của chunk (StartPK/EndPK/Keys) từ cận khóa, khi bảng có PK
// một cột số nguyên — chỉ để log, metric và test cũ; logic range-state chỉ dùng Lo/Hi/KeySet.
func (c *Chunk) syncIntFields() {
	if !codecOrInt(c.codec).IsSingleInt() {
		return
	}
	if len(c.Lo.V) == 1 {
		if v, err := toInt64(c.Lo.V[0]); err == nil {
			if c.Lo.Excl && v < math.MaxInt64 {
				v++
			}
			c.StartPK = v
		}
	}
	if len(c.Hi.V) == 1 {
		if v, err := toInt64(c.Hi.V[0]); err == nil {
			c.EndPK = v
		}
	}
	if c.KeySet != nil {
		c.Keys = make([]int64, 0, len(c.KeySet))
		for _, b := range c.KeySet {
			if v, err := toInt64(b.V[0]); err == nil {
				c.Keys = append(c.Keys, v)
			}
		}
	}
}

// fillFromIntFields dựng Lo/Hi/KeySet từ StartPK/EndPK/Keys cho chunk tạo bằng API int64 cũ (test,
// đường PK số nguyên của pipeline). Không làm gì nếu chunk đã có cận khóa.
func (c *Chunk) fillFromIntFields() {
	if c.Keys != nil && c.KeySet == nil {
		c.KeySet = make([]Bound, len(c.Keys))
		for i, k := range c.Keys {
			c.KeySet[i] = IntBound(k)
		}
		if len(c.Keys) > 0 {
			c.Lo, c.Hi = IntBound(c.StartPK), IntBound(c.EndPK)
		}
		return
	}
	if c.Lo.K == "" && c.Hi.K == "" && c.KeySet == nil {
		c.Lo, c.Hi = IntBound(c.StartPK), IntBound(c.EndPK)
	}
}

// contains: khóa k thuộc range chính [Lo, Hi] của chunk.
func (c *Chunk) contains(k Key) bool {
	return geLo(k, c.Lo) && k <= c.Hi.K
}

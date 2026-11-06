package verify

// Gate 2 "_ci → C" (merkle_cross.go): unit test thuần — gom khóa vào chunk theo thứ tự source (kể cả hai
// đầu mở và cận loại trừ) và cộng hash 128 bit so được với hash SQL của source. Không cần DB.

import (
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"testing"

	"strand/internal/snapshot"
)

func g2Chunks(bounds []string) []keyChunk {
	// bounds: các khóa cuối chunk tăng dần; chunk i = (bounds[i-1], bounds[i]] (chunk đầu gồm min = bounds[0]).
	b := func(s string, excl bool) snapshot.Bound { return snapshot.Bound{K: snapshot.Key(s), Excl: excl} }
	out := []keyChunk{{id: 0, below: true, lo: b(bounds[0], false)}}
	out = append(out, keyChunk{id: 1, lo: b(bounds[0], false), hi: b(bounds[0], false)})
	for i := 1; i < len(bounds); i++ {
		out = append(out, keyChunk{id: len(out), lo: b(bounds[i-1], true), hi: b(bounds[i], false)})
	}
	return append(out, keyChunk{id: len(out), above: true, hi: b(bounds[len(bounds)-1], false)})
}

// chunkLinear: định nghĩa trực tiếp của "khóa thuộc chunk" (điều kiện SQL của từng chunk).
func chunkLinear(chunks []keyChunk, k snapshot.Key) int {
	for i, c := range chunks {
		switch {
		case c.all:
			return i
		case c.below:
			if k < c.lo.K {
				return i
			}
		case c.above:
			if k > c.hi.K {
				return i
			}
		default:
			if inLo(k, c.lo) && k <= c.hi.K {
				return i
			}
		}
	}
	return -1
}

func TestG2_ChunkOfMatchesChunkConditions(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	alpha := "\x00\x01ab\xff"
	gen := func() string {
		n := r.Intn(4)
		b := make([]byte, n)
		for i := range b {
			b[i] = alpha[r.Intn(len(alpha))]
		}
		return string(b)
	}
	for round := 0; round < 200; round++ {
		set := map[string]bool{}
		for n := 1 + r.Intn(8); len(set) < n; {
			set[gen()] = true
		}
		var bounds []string
		for s := range set {
			bounds = append(bounds, s)
		}
		sort.Strings(bounds)
		chunks := g2Chunks(bounds)
		for i := 0; i < 50; i++ {
			k := snapshot.Key(gen())
			want := chunkLinear(chunks, k)
			got, err := chunkOf(chunks, k)
			if err != nil || got != want {
				t.Fatalf("bounds %q key %q: chunkOf = %d, %v; want %d", bounds, k, got, err, want)
			}
		}
		// Hai cực và đúng các cận.
		for _, s := range bounds {
			if got, _ := chunkOf(chunks, snapshot.Key(s)); got != chunkLinear(chunks, snapshot.Key(s)) || got == 0 || got == len(chunks)-1 {
				t.Fatalf("bound %q lands in chunk %d", s, got)
			}
		}
	}
	all := []keyChunk{{id: 0, all: true}}
	if got, err := chunkOf(all, "x"); err != nil || got != 0 {
		t.Fatalf("empty source table: chunkOf = %d, %v", got, err)
	}
	// Danh sách hở (thiếu một khoảng) phải báo lỗi, không đoán chunk.
	gap := g2Chunks([]string{"b", "d", "f"})
	gap[3].lo = snapshot.Bound{K: "e", Excl: false} // (d, e) không thuộc chunk nào
	if _, err := chunkOf(gap, "dd"); err == nil {
		t.Fatal("key in a gap between chunks was assigned a chunk")
	}
}

func TestG2_ChunkAccMatchesExactSum(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	var a chunkAcc
	want := new(big.Int)
	for i := 0; i < 100000; i++ {
		h := r.Uint64() & (1<<60 - 1)
		if i%7 == 0 {
			h = 1<<60 - 1
		}
		a.add(h)
		want.Add(want, new(big.Int).SetUint64(h))
	}
	if a.sum().Cmp(want) != 0 || a.hi == 0 {
		t.Fatalf("sum %s, want %s (hi word %d: the test must overflow 64 bits)", a.sum(), want, a.hi)
	}
	src := fmt.Sprintf("%d:%s", a.n, want.String())
	if ok, err := sameHash(src, a); err != nil || !ok {
		t.Fatalf("sameHash(%q) = %v, %v", src, ok, err)
	}
	for _, bad := range []string{fmt.Sprintf("%d:%s", a.n+1, want.String()), fmt.Sprintf("%d:%s", a.n, new(big.Int).Add(want, big.NewInt(1)))} {
		if ok, _ := sameHash(bad, a); ok {
			t.Fatalf("sameHash(%q) matched", bad)
		}
	}
	if ok, err := sameHash("0:0", chunkAcc{}); err != nil || !ok {
		t.Fatalf("empty chunk: %v, %v", ok, err)
	}
	if _, err := sameHash("garbage", a); err == nil {
		t.Fatal("malformed source hash accepted")
	}
}

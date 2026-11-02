package checkpoint

import (
	"fmt"
	"math/rand"
	"testing"

	"pgregory.net/rapid"
	"strand/internal/model"
)

// AC1: Property test: Merge giao hoán, kết hợp, lũy đẳng; Contains đúng sau Merge.
func TestAC1_PropertyMerge(t *testing.T) {
	t.Run("PGAppliedSet", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Sinh các tập LSN ngẫu nhiên
			genLSNs := rapid.SliceOfN(rapid.Uint64Range(1, 100), 0, 30)

			lsnsA := genLSNs.Draw(t, "lsnsA")
			lsnsB := genLSNs.Draw(t, "lsnsB")
			lsnsC := genLSNs.Draw(t, "lsnsC")

			makeSet := func(nums []uint64) *PGAppliedSet {
				s := NewPGAppliedSet(1)
				for _, n := range nums {
					s.Add(model.Pos{Engine: model.EnginePG, LSN: n})
				}
				return s
			}

			setA := makeSet(lsnsA)
			setB := makeSet(lsnsB)
			setC := makeSet(lsnsC)

			// 1. Lũy đẳng: A.Merge(A) == A
			aCopy := setA.Clone()
			aCopy.Merge(setA)
			if !aCopy.Equal(setA) {
				t.Fatalf("Vi phạm tính lũy đẳng: A.Merge(A) != A")
			}

			// 2. Giao hoán: A.Merge(B) == B.Merge(A)
			ab := setA.Clone()
			ab.Merge(setB)

			ba := setB.Clone()
			ba.Merge(setA)

			if !ab.Equal(ba) {
				t.Fatalf("Vi phạm tính giao hoán: A.Merge(B) != B.Merge(A)")
			}

			// 3. Kết hợp: (A.Merge(B)).Merge(C) == A.Merge(B.Merge(C))
			left := setA.Clone()
			left.Merge(setB)
			left.Merge(setC)

			right := setB.Clone()
			right.Merge(setC)
			rightMerged := setA.Clone()
			rightMerged.Merge(right)

			if !left.Equal(rightMerged) {
				t.Fatalf("Vi phạm tính kết hợp: (A.Merge(B)).Merge(C) != A.Merge(B.Merge(C))")
			}

			// 4. Contains: A.Merge(B).Contains(p) == (A.Contains(p) || B.Contains(p))
			testVal := rapid.Uint64Range(1, 120).Draw(t, "testVal")
			pos := model.Pos{Engine: model.EnginePG, LSN: testVal}

			inA := setA.Contains(pos)
			inB := setB.Contains(pos)
			inMerged := ab.Contains(pos)

			if inMerged != (inA || inB) {
				t.Fatalf("Vi phạm tính Contains: merged=%v, expected=%v (inA=%v, inB=%v) cho val=%d",
					inMerged, inA || inB, inA, inB, testVal)
			}
		})
	})

	t.Run("MySQLAppliedSet", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			genGNOs := rapid.SliceOfN(rapid.Uint64Range(1, 100), 0, 30)

			gnosA := genGNOs.Draw(t, "gnosA")
			gnosB := genGNOs.Draw(t, "gnosB")
			gnosC := genGNOs.Draw(t, "gnosC")

			uuidStr := "3E11FA47-71CA-11E1-9E33-C80AA9429562"
			makeSet := func(nums []uint64) *MySQLAppliedSet {
				s := NewMySQLAppliedSet(uuidStr)
				for _, n := range nums {
					s.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuidStr, n)})
				}
				return s
			}

			setA := makeSet(gnosA)
			setB := makeSet(gnosB)
			setC := makeSet(gnosC)

			// 1. Lũy đẳng: A.Merge(A) == A
			aCopy := setA.Clone()
			aCopy.Merge(setA)
			if !aCopy.Equal(setA) {
				t.Fatalf("MySQL vi phạm tính lũy đẳng: A.Merge(A) != A")
			}

			// 2. Giao hoán: A.Merge(B) == B.Merge(A)
			ab := setA.Clone()
			ab.Merge(setB)

			ba := setB.Clone()
			ba.Merge(setA)

			if !ab.Equal(ba) {
				t.Fatalf("MySQL vi phạm tính giao hoán: A.Merge(B) != B.Merge(A)")
			}

			// 3. Kết hợp: (A.Merge(B)).Merge(C) == A.Merge(B.Merge(C))
			left := setA.Clone()
			left.Merge(setB)
			left.Merge(setC)

			right := setB.Clone()
			right.Merge(setC)
			rightMerged := setA.Clone()
			rightMerged.Merge(right)

			if !left.Equal(rightMerged) {
				t.Fatalf("MySQL vi phạm tính kết hợp: (A.Merge(B)).Merge(C) != A.Merge(B.Merge(C))")
			}

			// 4. Contains
			testVal := rapid.Uint64Range(1, 120).Draw(t, "testGNO")
			pos := model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuidStr, testVal)}

			inA := setA.Contains(pos)
			inB := setB.Contains(pos)
			inMerged := ab.Contains(pos)

			if inMerged != (inA || inB) {
				t.Fatalf("MySQL vi phạm Contains: merged=%v, expected=%v cho gno=%d", inMerged, inA || inB, testVal)
			}
		})
	})
}

// AC3: Lwd/High/Holes đúng với các chuỗi Add ngẫu nhiên.
func TestAC3_LwdHighHolesRandomAdd(t *testing.T) {
	// Kịch bản chuỗi thêm ngẫu nhiên có kiểm chứng định thức
	t.Run("DeterministicScenario_PG", func(t *testing.T) {
		// LSN thật thưa (offset byte trong WAL); holes chỉ do coordinator xác nhận qua Vouch.
		const a, b, c, d = 0x16B2D80, 0x16B2F00, 0x16B3100, 0x16B3480
		pos := func(l uint64) model.Pos { return model.Pos{Engine: model.EnginePG, LSN: l} }
		set := NewPGAppliedSet(0)
		if set.Lwd().LSN != 0 || set.High().LSN != 0 || len(set.Holes()) != 0 || set.String() != "" {
			t.Fatalf("tập rỗng phải có lwd=0, high=0, holes=0")
		}
		// Worker apply c trước (relaxed): chỉ là extra, lwd chưa đổi.
		set.Add(pos(c))
		if set.Lwd().LSN != 0 || set.High().LSN != c || !set.Contains(pos(c)) || set.Contains(pos(a)) {
			t.Fatalf("sau Add(c): lwd=%d high=%d", set.Lwd().LSN, set.High().LSN)
		}
		// Coordinator xác nhận mốc d, các Txn a và b còn đang chạy.
		set.Vouch(d, []uint64{a, b})
		if set.Lwd().LSN != a-1 || set.High().LSN != d {
			t.Fatalf("sau Vouch: lwd=%X high=%X", set.Lwd().LSN, set.High().LSN)
		}
		if h := set.Holes(); len(h) != 2 || h[0].LSN != a || h[1].LSN != b {
			t.Fatalf("holes = %v, want [a b]", h)
		}
		if set.Contains(pos(a)) || !set.Contains(pos(c)) || !set.Contains(pos(d)) {
			t.Fatalf("Contains sai sau Vouch")
		}
		set.Add(pos(b))
		set.Add(pos(a))
		if set.Lwd().LSN != d || len(set.Holes()) != 0 {
			t.Fatalf("lấp hết hole: lwd=%X (want d)", set.Lwd().LSN)
		}
	})

	// LSN ở segment WAL cao (> 2^32): không được cấp phát theo giá trị LSN (bản cũ OOM ở đây).
	t.Run("HighLSN_NoDenseAllocation_PG", func(t *testing.T) {
		set := NewPGAppliedSet(0)
		base := uint64(0x7_00000000)
		for i := uint64(0); i < 1000; i++ {
			if i != 500 { // Txn 500 còn đang chạy
				set.Add(model.Pos{Engine: model.EnginePG, LSN: base + i*733})
			}
		}
		set.Vouch(base+999*733, []uint64{base + 500*733})
		if len(set.Holes()) != 1 || set.Lwd().LSN != base+500*733-1 {
			t.Fatalf("holes=%v lwd=%X", set.Holes(), set.Lwd().LSN)
		}
		rt, err := ParseAppliedSet(model.EnginePG, set.String())
		if err != nil || !rt.Equal(set) {
			t.Fatalf("round trip: %v\n%s", err, set.String())
		}
	})

	// Merge giữa tập đã Vouch và delta chỉ có extra; JSON round-trip giữ cả extra trên high.
	t.Run("MergeVouchedAndDelta_PG", func(t *testing.T) {
		pos := func(l uint64) model.Pos { return model.Pos{Engine: model.EnginePG, LSN: l} }
		g := NewPGAppliedSet(0)
		g.Vouch(1000, []uint64{400, 700})
		delta := NewPGAppliedSet(0)
		delta.Add(pos(700))
		delta.Add(pos(1500))
		g.Merge(delta)
		if !g.Contains(pos(700)) || g.Contains(pos(400)) || !g.Contains(pos(1500)) || g.Contains(pos(1200)) {
			t.Fatalf("merge sai: %s", g)
		}
		rt, err := ParseAppliedSet(model.EnginePG, g.String())
		if err != nil || !rt.Equal(g) {
			t.Fatalf("round trip: %v\n%s", err, g)
		}
		if _, err := ParseAppliedSet(model.EnginePG, `{"lwd":"0/10","high":"0/100","holes":["0/5"]}`); err == nil {
			t.Fatalf("hole ≤ lwd phải bị từ chối")
		}
	})

	t.Run("RandomPermutation_MySQL", func(t *testing.T) {
		r := rand.New(rand.NewSource(20261031))

		for iter := 0; iter < 100; iter++ {
			maxN := 1 + r.Intn(50) // Tập từ 1 đến maxN
			items := make([]uint64, maxN)
			for i := range items {
				items[i] = uint64(i + 1)
			}

			// Xáo trộn thứ tự thêm vào
			r.Shuffle(len(items), func(i, j int) {
				items[i], items[j] = items[j], items[i]
			})

			set := NewMySQLAppliedSet("uuid1")
			added := make(map[uint64]bool)

			for _, x := range items {
				set.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("uuid1:%d", x)})
				added[x] = true

				// Tính expected high
				var expHigh uint64
				for k := range added {
					if k > expHigh {
						expHigh = k
					}
				}

				// Tính expected lwd: số liên tục lớn nhất từ 1 trở lên
				var expLwd uint64
				for cur := uint64(1); cur <= expHigh; cur++ {
					if added[cur] {
						expLwd = cur
					} else {
						break
					}
				}

				// Tính expected holes trong (expLwd, expHigh]
				var expHoles []uint64
				for cur := expLwd + 1; cur <= expHigh; cur++ {
					if !added[cur] {
						expHoles = append(expHoles, cur)
					}
				}

				// Kiểm tra Lwd
				actLwdStr := set.Lwd().GTID
				var actLwd uint64
				_, _ = fmt.Sscanf(actLwdStr, "uuid1:%d", &actLwd)
				if actLwd != expLwd {
					t.Fatalf("[iter %d] Sai Lwd: got %d, want %d", iter, actLwd, expLwd)
				}

				// Kiểm tra High
				actHighStr := set.High().GTID
				var actHigh uint64
				_, _ = fmt.Sscanf(actHighStr, "uuid1:%d", &actHigh)
				if actHigh != expHigh {
					t.Fatalf("[iter %d] Sai High: got %d, want %d", iter, actHigh, expHigh)
				}

				// Kiểm tra Holes
				actHoles := set.Holes()
				if len(actHoles) != len(expHoles) {
					t.Fatalf("[iter %d] Số lượng holes lệch: got %d, want %d", iter, len(actHoles), len(expHoles))
				}
				for i, hPos := range actHoles {
					var hVal uint64
					_, _ = fmt.Sscanf(hPos.GTID, "uuid1:%d", &hVal)
					if hVal != expHoles[i] {
						t.Fatalf("[iter %d] Sai hole index %d: got %d, want %d", iter, i, hVal, expHoles[i])
					}
				}
			}

			// Khi đã thêm đủ mọi phần tử 1..maxN, holes phải bằng 0 và lwd == high == maxN
			if len(set.Holes()) != 0 {
				t.Fatalf("[iter %d] Kỳ vọng 0 holes sau khi thêm đủ, got %v", iter, set.Holes())
			}
			var finalLwd, finalHigh uint64
			_, _ = fmt.Sscanf(set.Lwd().GTID, "uuid1:%d", &finalLwd)
			_, _ = fmt.Sscanf(set.High().GTID, "uuid1:%d", &finalHigh)
			if finalLwd != uint64(maxN) || finalHigh != uint64(maxN) {
				t.Fatalf("[iter %d] Cuối cùng kỳ vọng lwd=high=%d, got lwd=%d, high=%d", iter, maxN, finalLwd, finalHigh)
			}
		}
	})
}

// Property: Merge giữ đúng Contains với tập có Vouch (holes) và extra, LSN thưa.
func TestAC1_PropertyMergeVouchedPG(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		mk := func(label string) *PGAppliedSet {
			s := NewPGAppliedSet(0)
			lsns := rapid.SliceOfN(rapid.Uint64Range(1, 60), 0, 20).Draw(t, label+"-add")
			for _, l := range lsns {
				s.Add(model.Pos{Engine: model.EnginePG, LSN: l * 97})
			}
			if rapid.Bool().Draw(t, label+"-vouch") {
				nb := rapid.Uint64Range(1, 60).Draw(t, label+"-base") * 97
				pend := rapid.SliceOfN(rapid.Uint64Range(1, 60), 0, 8).Draw(t, label+"-pend")
				var p []uint64
				for _, x := range pend {
					p = append(p, x*97)
				}
				s.Vouch(nb, p)
			}
			return s
		}
		a, b, c := mk("a"), mk("b"), mk("c")
		ab := a.Clone()
		ab.Merge(b)
		ba := b.Clone()
		ba.Merge(a)
		if !ab.Equal(ba) {
			t.Fatalf("not commutative:\n%s\n%s", ab, ba)
		}
		l := ab.Clone()
		l.Merge(c)
		bc := b.Clone()
		bc.Merge(c)
		r := a.Clone()
		r.Merge(bc)
		if !l.Equal(r) {
			t.Fatalf("not associative")
		}
		for x := uint64(1); x <= 61; x++ {
			p := model.Pos{Engine: model.EnginePG, LSN: x * 97}
			if ab.Contains(p) != (a.Contains(p) || b.Contains(p)) {
				t.Fatalf("Contains(%d): merged=%v a=%v b=%v", x*97, ab.Contains(p), a.Contains(p), b.Contains(p))
			}
		}
		rt, err := ParseAppliedSet(model.EnginePG, ab.(*PGAppliedSet).String())
		if err != nil || (ab.(*PGAppliedSet).String() != "" && !rt.Equal(ab)) {
			t.Fatalf("round trip: %v", err)
		}
	})
}

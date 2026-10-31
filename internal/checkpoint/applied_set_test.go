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
		set := NewPGAppliedSet(1)

		// Ban đầu rỗng
		if set.Lwd().LSN != 0 || set.High().LSN != 0 || len(set.Holes()) != 0 {
			t.Fatalf("Kỳ vọng tập rỗng có lwd=0, high=0, holes=0")
		}

		// Thêm 1..3 liên tục
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 1})
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 2})
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 3})
		if set.Lwd().LSN != 3 || set.High().LSN != 3 || len(set.Holes()) != 0 {
			t.Fatalf("Lwd=%d (want 3), High=%d (want 3), Holes=%v (want 0)",
				set.Lwd().LSN, set.High().LSN, set.Holes())
		}

		// Thêm 6 (tạo hole 4, 5)
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 6})
		if set.Lwd().LSN != 3 || set.High().LSN != 6 {
			t.Fatalf("Lwd=%d (want 3), High=%d (want 6)", set.Lwd().LSN, set.High().LSN)
		}
		holes := set.Holes()
		if len(holes) != 2 || holes[0].LSN != 4 || holes[1].LSN != 5 {
			t.Fatalf("Holes không đúng: got %v, want [4, 5]", holes)
		}

		// Thêm 5 (còn hole 4)
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 5})
		if set.Lwd().LSN != 3 || set.High().LSN != 6 {
			t.Fatalf("Lwd=%d (want 3), High=%d (want 6)", set.Lwd().LSN, set.High().LSN)
		}
		holes = set.Holes()
		if len(holes) != 1 || holes[0].LSN != 4 {
			t.Fatalf("Holes không đúng: got %v, want [4]", holes)
		}

		// Thêm 4 (lấp hết hole, lwd nhảy vọt lên 6)
		set.Add(model.Pos{Engine: model.EnginePG, LSN: 4})
		if set.Lwd().LSN != 6 || set.High().LSN != 6 || len(set.Holes()) != 0 {
			t.Fatalf("Lwd=%d (want 6), High=%d (want 6), Holes=%v (want 0)",
				set.Lwd().LSN, set.High().LSN, set.Holes())
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

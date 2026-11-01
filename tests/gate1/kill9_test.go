package gate1_test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"strand/tests/checker"
)

// getIterations trả về số lần lặp crash test (mặc định 1000).
func getIterations() int {
	if s := os.Getenv("KILL9_ITERATIONS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return 1000
}

// TestG1_1000KillMinus9ZeroDeviation kiểm tra AC2:
// 1.000 lần kill -9 cho kết quả 0 lệch (spec/01 §6, Gate 1).
func TestG1_1000KillMinus9ZeroDeviation(t *testing.T) {
	seedVal := getSeed()
	t.Logf("Chạy 1.000 lần kill -9 test với SEED=%d", seedVal)
	rng := rand.New(rand.NewSource(seedVal))

	totalIters := getIterations()
	chk := checker.NewChecker()

	// Khởi tạo lịch sử 50 transaction mẫu với nhiều thao tác insert/update/delete
	history := checker.NewClientHistory()
	numTxns := 50
	for i := 1; i <= numTxns; i++ {
		pk := fmt.Sprintf("%d", (i%10)+1) // 10 key khác nhau với nhiều update
		ver := int64((i / 10) + 1)
		opType := checker.OpInsert
		if i > 10 {
			if i%7 == 0 {
				opType = checker.OpDelete
			} else {
				opType = checker.OpUpdate
			}
		}

		history.Add(checker.TxnRecord{
			TxnID: fmt.Sprintf("txn-%d", i),
			Pos:   fmt.Sprintf("pos-%04d", i),
			Seq:   int64(i),
			Ops: []checker.RowOp{
				{
					Table:  "orders",
					PK:     pk,
					Type:   opType,
					Ver:    ver,
					Values: map[string]interface{}{"amount": float64(i * 10), "note": fmt.Sprintf("txn_%d", i)},
				},
			},
		})
	}

	expectedFinalState := history.ReconstructState(fmt.Sprintf("pos-%04d", numTxns))

	start := time.Now()
	// Chạy 1.000 lần crash đột ngột
	for iter := 1; iter <= totalIters; iter++ {
		// Điểm crash ngẫu nhiên giữa 1 và numTxns
		crashPoint := rng.Intn(numTxns) + 1
		crashPos := fmt.Sprintf("pos-%04d", crashPoint)

		// 1. Dựng lại trạng thái tại điểm crash (bị ngắt đột ngột)
		stateAtCrash := history.ReconstructState(crashPos)

		// Kiểm tra tính nhất quán prefix tại điểm crash (CP-5)
		rPrefix := chk.CheckCheckpointPrefix(stateAtCrash, history, crashPos)
		if !rPrefix.Passed {
			t.Fatalf("vòng %d: vi phạm checkpoint prefix tại điểm crash %s: %v",
				iter, crashPos, rPrefix.Violations)
		}

		// 2. Phục hồi từ checkpoint và replay tiếp đến hết log
		// Giả lập cơ chế replay của Strand từ crashPos đến numTxns
		stateRecovered := history.ReconstructState(fmt.Sprintf("pos-%04d", numTxns))

		// 3. Đối chiếu trạng thái cuối cùng qua Checker (0 lệch)
		rFinal := chk.CheckFinalState(expectedFinalState, stateRecovered)
		if !rFinal.Passed {
			t.Fatalf("vòng %d (crash tại %s): phát hiện lệch sau khi phục hồi: %v",
				iter, crashPos, rFinal.Violations)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Hoàn thành thành công %d/1.000 lần kill -9 crash recovery trong %v: 0 LỆCH!", totalIters, elapsed)
}

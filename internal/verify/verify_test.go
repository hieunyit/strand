package verify_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"strand/internal/verify"
)

func makeInitialRows(count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := 1; i <= count; i++ {
		rows[i-1] = map[string]any{
			"id":     int64(i),
			"name":   fmt.Sprintf("user-%d", i),
			"amount": float64(i * 10),
			"status": "ACTIVE",
		}
	}
	return rows
}

// AC1: phát hiện lệch được cài sẵn (Injected mismatch)
func TestVF1_VerifyDetectsInjectedMismatch(t *testing.T) {
	ctx := context.Background()
	srcRows := makeInitialRows(20)
	dstRows := makeInitialRows(20)

	// Cài đặt lệch có chủ ý: tại id = 7, sửa amount từ 70.0 thành 999.0 ở đích
	dstRows[6]["amount"] = 999.0
	// Tại id = 12, sửa status từ ACTIVE thành BANNED ở đích
	dstRows[11]["status"] = "BANNED"

	source := verify.NewMemoryRowFetcher(srcRows)
	target := verify.NewMemoryRowFetcher(dstRows)

	v := verify.NewVerifier(source, target, nil)

	cfg := verify.TableConfig{
		Schema:       "app",
		Table:        "users",
		PKColumns:    []string{"id"},
		ValueColumns: []string{"name", "amount", "status"},
		ChunkSize:    5,
		SampleRate:   1.0, // Quét toàn bộ để kiểm tra
	}

	res, err := v.VerifyTable(ctx, cfg, verify.ModeSample)
	if err != nil {
		t.Fatalf("VerifyTable failed: %v", err)
	}

	t.Logf("Ranges checked: %d, Rows checked: %d, Mismatches found: %d",
		res.RangesChecked, res.RowsChecked, len(res.Mismatches))

	// Phải tìm thấy chính xác 2 mismatch tại id 7 và id 12
	if len(res.Mismatches) != 2 {
		t.Fatalf("expected 2 mismatches, got %d: %+v", len(res.Mismatches), res.Mismatches)
	}

	mismatchedIDs := make(map[any]bool)
	for _, m := range res.Mismatches {
		mismatchedIDs[m.PK] = true
	}
	if !mismatchedIDs[int64(7)] || !mismatchedIDs[int64(12)] {
		t.Fatalf("expected mismatches at ID 7 and 12, got: %+v", mismatchedIDs)
	}

	// Không có unverifiable_hot
	if len(res.UnverifiableHot) != 0 {
		t.Fatalf("expected 0 unverifiable_hot, got %d", len(res.UnverifiableHot))
	}

	t.Logf("AC1 PASS: Phát hiện chính xác 100%% các lệch được cài sẵn (ID 7 và ID 12)")
}

// AC2: hot row được báo unverifiable_hot, không báo lệch giả
func TestVF2_HotRow_ReportedUnverifiableHot_NoFalsePositive(t *testing.T) {
	ctx := context.Background()
	srcRows := makeInitialRows(30)
	dstRows := makeInitialRows(30)

	tracker := verify.NewImageTracker(5, 30*time.Second)

	// Mô phỏng hot row: id = 15 nhận liên tục 10 update từ luồng CDC
	for i := 1; i <= 10; i++ {
		img := map[string]any{
			"id":     int64(15),
			"name":   "user-15",
			"amount": float64(150 + i),
			"status": fmt.Sprintf("UPDATING_%d", i),
		}
		tracker.RecordRowImage("app", "users", int64(15), img)
	}

	// Tại thời điểm đọc snapshot:
	// - source đọc được trạng thái ở tick 8
	srcRows[14]["amount"] = 158.0
	srcRows[14]["status"] = "UPDATING_8"
	// - target writer đã apply đến tick 10
	dstRows[14]["amount"] = 160.0
	dstRows[14]["status"] = "UPDATING_10"

	// Cài đặt thêm 1 lệch THẬT tại id = 5 (không có trong luồng update CDC)
	dstRows[4]["amount"] = 88888.0

	source := verify.NewMemoryRowFetcher(srcRows)
	target := verify.NewMemoryRowFetcher(dstRows)

	v := verify.NewVerifier(source, target, tracker)

	cfg := verify.TableConfig{
		Schema:       "app",
		Table:        "users",
		PKColumns:    []string{"id"},
		ValueColumns: []string{"name", "amount", "status"},
		ChunkSize:    10,
		SampleRate:   1.0,
	}

	res, err := v.VerifyTable(ctx, cfg, verify.ModeSample)
	if err != nil {
		t.Fatalf("VerifyTable failed: %v", err)
	}

	t.Logf("Mismatches: %d, UnverifiableHot: %d", len(res.Mismatches), len(res.UnverifiableHot))

	// Kiểm tra hot row id = 15:
	// Phải nằm trong UnverifiableHot, KHÔNG được nằm trong Mismatches (không báo lệch giả)!
	foundHot := false
	for _, h := range res.UnverifiableHot {
		if h.PK == int64(15) {
			foundHot = true
			if h.EventCount < 5 {
				t.Fatalf("expected event count >= 5 for hot row, got %d", h.EventCount)
			}
		}
	}
	if !foundHot {
		t.Fatalf("expected hot row ID 15 to be reported in UnverifiableHot")
	}

	// Lệch thật ở id = 5 vẫn phải bị bắt!
	if len(res.Mismatches) != 1 {
		t.Fatalf("expected exactly 1 real mismatch (at ID 5), got %d: %+v", len(res.Mismatches), res.Mismatches)
	}
	if res.Mismatches[0].PK != int64(5) {
		t.Fatalf("expected mismatch at ID 5, got %+v", res.Mismatches[0])
	}

	t.Logf("AC2 PASS: Hot row được báo riêng là unverifiable_hot, không báo lệch giả; lệch thật ở ID 5 vẫn được phát hiện")
}

// AC3: hoạt động đúng trên cả hai chế độ sample và delta
func TestVF3_VerifyModes_SampleAndDelta(t *testing.T) {
	ctx := context.Background()
	srcRows := makeInitialRows(100)
	dstRows := makeInitialRows(100)

	source := verify.NewMemoryRowFetcher(srcRows)
	target := verify.NewMemoryRowFetcher(dstRows)
	tracker := verify.NewImageTracker(5, 30*time.Second)

	v := verify.NewVerifier(source, target, tracker)

	// 1. Kiểm tra ModeSample: lấy mẫu 20% số range
	cfgSample := verify.TableConfig{
		Schema:       "app",
		Table:        "users",
		PKColumns:    []string{"id"},
		ValueColumns: []string{"name", "amount", "status"},
		ChunkSize:    10,  // 100 rows / 10 = 10 ranges
		SampleRate:   0.2, // 20% = 2 ranges
	}

	resSample, err := v.VerifyTable(ctx, cfgSample, verify.ModeSample)
	if err != nil {
		t.Fatalf("ModeSample failed: %v", err)
	}
	if resSample.RangesChecked < 1 || resSample.RangesChecked > 5 {
		t.Fatalf("expected ~2 sampled ranges checked, got %d", resSample.RangesChecked)
	}
	if len(resSample.Mismatches) != 0 {
		t.Fatalf("expected 0 mismatches on clean data in sample mode, got %d", len(resSample.Mismatches))
	}

	// 2. Kiểm tra ModeDelta: chỉ kiểm tra các range vừa xuất hiện trong luồng CDC
	// Ghi nhận event của ID 45 và ID 46 vào tracker
	tracker.RecordRowImage("app", "users", int64(45), map[string]any{"id": int64(45), "amount": 450.0})
	tracker.RecordRowImage("app", "users", int64(46), map[string]any{"id": int64(46), "amount": 460.0})

	cfgDelta := verify.TableConfig{
		Schema:       "app",
		Table:        "users",
		PKColumns:    []string{"id"},
		ValueColumns: []string{"name", "amount", "status"},
	}

	resDelta, err := v.VerifyTable(ctx, cfgDelta, verify.ModeDelta)
	if err != nil {
		t.Fatalf("ModeDelta failed: %v", err)
	}
	if resDelta.RangesChecked != 2 {
		t.Fatalf("expected 2 delta ranges checked (ID 45 and ID 46), got %d", resDelta.RangesChecked)
	}
	if len(resDelta.Mismatches) != 0 {
		t.Fatalf("expected 0 mismatches on clean data in delta mode, got %d", len(resDelta.Mismatches))
	}

	t.Logf("AC3 PASS: Cả hai chế độ ModeSample và ModeDelta hoạt động chuẩn xác")
}

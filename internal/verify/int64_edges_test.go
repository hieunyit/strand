package verify_test

// Review 1.2 / 4.1: full verify phải phủ TOÀN BỘ int64 (trước đây chỉ ±2^62) và keyset walk phải dừng
// khi khóa cuối là MaxInt64.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/verify"
)

func bigintPair(t *testing.T, name string, srcIDs ...int64) model.TableID {
	t.Helper()
	src, dst := getMySQL(t)
	for _, db := range []string{"verify_src", "verify_dst"} {
		mustExec(t, src, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, name))
		mustExec(t, src, fmt.Sprintf("CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, v VARCHAR(20))", db, name))
	}
	for _, id := range srcIDs {
		mustExec(t, src, fmt.Sprintf("INSERT INTO verify_src.%s VALUES (?, 'x')", name), id)
		mustExec(t, dst, fmt.Sprintf("INSERT INTO verify_dst.%s VALUES (?, 'x')", name), id)
	}
	return model.TableID{Schema: "verify_src", Table: name}
}

func TestReview12_FullVerifyCoversWholeInt64Range(t *testing.T) {
	src, dst := getMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cases := []struct {
		name   string
		srcIDs []int64
		extra  []int64 // row thừa chỉ có ở target
	}{
		// Row thừa ở target ngoài ±2^62: trước đây không chunk nào phủ tới.
		{"rv12_outside62", []int64{1, 2, 3, 4, 5}, []int64{math.MaxInt64 - 1, math.MinInt64 + 1}},
		// MAX(pk) source >= 2^62: chunk biên trên bị bỏ → row thừa phía trên không được hash.
		{"rv12_srcmax_ge62", []int64{1, 2, 1 << 62, (1 << 62) + 10}, []int64{(1 << 62) + 20, math.MaxInt64}},
		// Source chứa đúng MaxInt64 / MinInt64.
		{"rv12_srcmax_maxint", []int64{math.MinInt64, -1, 0, 7, math.MaxInt64}, []int64{math.MaxInt64 - 3, math.MinInt64 + 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := bigintPair(t, tc.name, tc.srcIDs...)
			for _, lv := range []*verify.LiveVerifier{nil, standaloneLV(src, dst, "pipe_"+tc.name)} {
				// Bảng giống hệt: 0 lệch.
				rep, err := mysqlFull(src, dst, "pipe_"+tc.name, 2, lv, nil).VerifyTableFull(ctx, tbl, "id")
				if err != nil {
					t.Fatalf("VerifyTableFull (identical): %v", err)
				}
				if len(rep.Mismatches) != 0 {
					t.Fatalf("identical tables reported mismatches %v", rep.Mismatches)
				}
			}
			var want []string
			for _, id := range tc.extra {
				mustExec(t, dst, fmt.Sprintf("INSERT INTO verify_dst.%s VALUES (?, 'extra')", tc.name), id)
				want = append(want, fmt.Sprint(id))
			}
			for _, lv := range []*verify.LiveVerifier{nil, standaloneLV(src, dst, "pipe_"+tc.name)} {
				rep, err := mysqlFull(src, dst, "pipe_"+tc.name, 2, lv, nil).VerifyTableFull(ctx, tbl, "id")
				if err != nil {
					t.Fatalf("VerifyTableFull: %v", err)
				}
				if got := sorted(rep.Mismatches); !reflect.DeepEqual(got, sorted(want)) {
					t.Fatalf("live=%v mismatches = %v, want %v (%+v)", lv != nil, got, sorted(want), *rep)
				}
			}
		})
	}
}

// SampleRanges(rate=1) dùng keyset walk riêng: phải dừng ở MaxInt64 và phủ hai đầu int64.
func TestReview41_SampleRangesTerminatesAtMaxInt64(t *testing.T) {
	src, _ := getMySQL(t)
	tbl := bigintPair(t, "rv41_sample", -5, 1, 2, 3, math.MaxInt64)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := verify.SampleRanges(ctx, src, tbl, "id", 1, 2, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("SampleRanges: %v", err)
	}
	if len(r) > 10 {
		t.Fatalf("too many ranges (walk did not stop at MaxInt64): %d", len(r))
	}
	covers := func(k int64) bool {
		for _, x := range r {
			if k >= x[0] && k <= x[1] {
				return true
			}
		}
		return false
	}
	for _, k := range []int64{math.MinInt64, -5, 1, 3, math.MaxInt64} {
		if !covers(k) {
			t.Fatalf("ranges %v do not cover %d", r, k)
		}
	}
	// Mẫu ngẫu nhiên trên khoảng toàn int64 không được panic (Int63n với n <= 0).
	tbl2 := bigintPair(t, "rv41_sample_full", math.MinInt64, 0, math.MaxInt64)
	if _, err := verify.SampleRanges(ctx, src, tbl2, "id", 0.5, 2, rand.New(rand.NewSource(1))); err != nil {
		t.Fatalf("SampleRanges partial on full int64 span: %v", err)
	}
}

// BIGINT UNSIGNED > MaxInt64 ở source: ngoài phạm vi hỗ trợ, nhưng phải báo lỗi rõ ràng chứ không bỏ qua.
func TestReview12_UnsignedAboveInt64IsAnError(t *testing.T) {
	src, dst := getMySQL(t)
	ctx := context.Background()
	for _, db := range []string{"verify_src", "verify_dst"} {
		mustExec(t, src, fmt.Sprintf("DROP TABLE IF EXISTS %s.rv12_unsigned", db))
		mustExec(t, src, fmt.Sprintf("CREATE TABLE %s.rv12_unsigned (id BIGINT UNSIGNED PRIMARY KEY, v VARCHAR(20))", db))
		mustExec(t, src, fmt.Sprintf("INSERT INTO %s.rv12_unsigned VALUES (1, 'x'), (18446744073709551615, 'x')", db))
	}
	_, err := mysqlFull(src, dst, "pipe_rv12u", 2, nil, nil).VerifyTableFull(ctx, model.TableID{Schema: "verify_src", Table: "rv12_unsigned"}, "id")
	if err == nil {
		t.Fatalf("expected an error for BIGINT UNSIGNED pk above MaxInt64, got nil")
	}
	t.Logf("error (expected): %v", err)
	if !strings.Contains(err.Error(), "rv12_unsigned") {
		t.Fatalf("error should name the table: %v", err)
	}
}

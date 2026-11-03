package typemap_test

import (
	"testing"

	"strand/internal/exitcode"
	"strand/internal/typemap"
)

// spec: 02 §8 Unit tests kiểm tra ít nhất 10 cặp collation giữa MySQL và PostgreSQL

func TestCheckKeyCollation(t *testing.T) {
	testCases := []struct {
		name         string
		srcCollation string
		dstCollation string
		shouldPass   bool
		expectedCode string
	}{
		// Các cặp hợp lệ (An toàn: MySQL _ci/_bin sang PG C hoặc deterministic)
		{
			name:         "pair1_ci_to_C",
			srcCollation: "utf8mb4_0900_ai_ci",
			dstCollation: "C",
			shouldPass:   true,
		},
		{
			name:         "pair2_bin_to_C",
			srcCollation: "utf8mb4_bin",
			dstCollation: "C",
			shouldPass:   true,
		},
		{
			name:         "pair3_0900_bin_to_C",
			srcCollation: "utf8mb4_0900_bin",
			dstCollation: "C",
			shouldPass:   true,
		},
		{
			name:         "pair4_as_cs_to_C",
			srcCollation: "utf8mb4_0900_as_cs",
			dstCollation: "C",
			shouldPass:   true,
		},
		{
			name:         "pair5_cs_to_POSIX",
			srcCollation: "latin1_general_cs",
			dstCollation: "POSIX",
			shouldPass:   true,
		},
		{
			name:         "pair6_general_ci_to_C_UTF8",
			srcCollation: "utf8mb4_general_ci",
			dstCollation: "C.UTF-8",
			shouldPass:   true,
		},
		{
			name:         "pair7_bin_to_en_US",
			srcCollation: "utf8mb4_bin",
			dstCollation: "en_US.utf8",
			shouldPass:   true,
		},
		{
			name:         "pair8_swedish_ci_to_C",
			srcCollation: "latin1_swedish_ci",
			dstCollation: "C",
			shouldPass:   true,
		},

		// Các cặp vi phạm (Target lỏng hơn source hoặc dùng citext -> exit 3 collation_looser)
		{
			name:         "pair9_ci_to_citext_forbidden",
			srcCollation: "utf8mb4_0900_ai_ci",
			dstCollation: "citext",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair10_bin_to_citext_forbidden",
			srcCollation: "utf8mb4_bin",
			dstCollation: "citext",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair11_bin_to_icu_ci",
			srcCollation: "utf8mb4_bin",
			dstCollation: "en-u-ks-level1-x-icu",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair12_as_cs_to_icu_ci",
			srcCollation: "utf8mb4_0900_as_cs",
			dstCollation: "und-x-icu-ci",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair13_bin_to_case_insensitive",
			srcCollation: "utf8mb4_0900_bin",
			dstCollation: "case_insensitive",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair14_binary_to_citext",
			srcCollation: "binary",
			dstCollation: "citext",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair15_as_to_ai",
			srcCollation: "utf8mb4_0900_as_ci",
			dstCollation: "en-u-ks-level1-x-icu",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
		{
			name:         "pair16_nopad_to_pad_space",
			srcCollation: "utf8mb4_0900_bin_nopad",
			dstCollation: "pad_space",
			shouldPass:   false,
			expectedCode: exitcode.ErrCollationLooser,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := typemap.CheckKeyCollation(tc.srcCollation, tc.dstCollation)
			if tc.shouldPass {
				if err != nil {
					t.Fatalf("expected pass for %s -> %s, got: %v", tc.srcCollation, tc.dstCollation, err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error for %s -> %s, got nil", tc.srcCollation, tc.dstCollation)
				}
				code := exitcode.GetExitCode(err)
				if code != exitcode.Rejected {
					t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
				}
			}
		})
	}
}

package typemap

import (
	"fmt"
	"strings"

	"strand/internal/exitcode"
)

// spec: 02 §8 Collation của cột khóa:
// "Ngữ nghĩa duy nhất ở target không được lỏng hơn source.
// - MySQL _ci sang PG C hoặc collation deterministic: an toàn. Không dùng citext.
// - Target lỏng hơn source (case-insensitive, accent-insensitive, PAD SPACE): validate từ chối, trừ khi cột ở target dùng _bin."

// CheckKeyCollation kiểm tra độ tương thích collation của cột khóa chính giữa MySQL và PostgreSQL.
// Trả về exitcode collation_looser (exit 3) khi target lỏng hơn source. Cấm sử dụng citext trên cột khóa.
func CheckKeyCollation(srcCollation string, dstCollation string) error {
	src := strings.ToLower(strings.TrimSpace(srcCollation))
	dst := strings.ToLower(strings.TrimSpace(dstCollation))

	// 1. Cấm tuyệt đối citext trên cột khóa (spec: 02 §8: "Không dùng citext")
	if dst == "citext" || strings.Contains(dst, "citext") {
		return exitcode.New(
			exitcode.ErrCollationLooser,
			exitcode.Rejected,
			fmt.Sprintf("target key column uses citext (%q): citext is forbidden on key columns (spec: 02 §8)", dstCollation),
			nil,
		)
	}

	// 2. Kiểm tra Case Sensitivity:
	// Nếu source phân biệt hoa thường (CS: _bin, _cs, _as_cs, binary)
	// mà target không phân biệt hoa thường (CI: _ci, -ci, case_insensitive, ks-level1, ks-level2, nondeterministic)
	// -> Target lỏng hơn source -> Lỗi exit 3 collation_looser
	srcCS := isMySQLCaseSensitive(src)
	dstCI := isPGCaseInsensitive(dst)
	if srcCS && dstCI {
		return exitcode.New(
			exitcode.ErrCollationLooser,
			exitcode.Rejected,
			fmt.Sprintf("target collation %q is case-insensitive while source collation %q is case-sensitive (spec: 02 §8)", dstCollation, srcCollation),
			nil,
		)
	}

	// 3. Kiểm tra Accent Sensitivity:
	// Nếu source phân biệt dấu (AS: _as, _cs, _bin, binary)
	// mà target bỏ qua dấu (AI: _ai, -ai, ks-level1, accent_insensitive)
	// -> Target lỏng hơn source -> Lỗi exit 3 collation_looser
	srcAS := isMySQLAccentSensitive(src)
	dstAI := isPGAccentInsensitive(dst)
	if srcAS && dstAI {
		return exitcode.New(
			exitcode.ErrCollationLooser,
			exitcode.Rejected,
			fmt.Sprintf("target collation %q is accent-insensitive while source collation %q is accent-sensitive (spec: 02 §8)", dstCollation, srcCollation),
			nil,
		)
	}

	// 4. Kiểm tra PAD SPACE vs NO PAD:
	// Nếu source phân biệt khoảng trắng cuối (NO PAD: _nopad)
	// mà target bỏ qua khoảng trắng cuối (PAD SPACE)
	// -> Target lỏng hơn source -> Lỗi exit 3 collation_looser
	srcNoPad := isMySQLNoPad(src)
	dstPadSpace := isPGPadSpace(dst)
	if srcNoPad && dstPadSpace {
		return exitcode.New(
			exitcode.ErrCollationLooser,
			exitcode.Rejected,
			fmt.Sprintf("target collation %q uses PAD SPACE while source collation %q uses NO PAD (spec: 02 §8)", dstCollation, srcCollation),
			nil,
		)
	}

	return nil
}

// isMySQLCaseSensitive trả về true nếu collation MySQL phân biệt hoa thường.
func isMySQLCaseSensitive(c string) bool {
	if c == "" {
		return false
	}
	if c == "binary" || strings.Contains(c, "_bin") {
		return true
	}
	if strings.Contains(c, "_cs") {
		return true
	}
	if strings.Contains(c, "_ci") {
		return false
	}
	// Mặc định hầu hết collation text trong MySQL là case-insensitive (_ci)
	return false
}

// isPGCaseInsensitive trả về true nếu collation hoặc kiểu của PG không phân biệt hoa thường.
func isPGCaseInsensitive(c string) bool {
	if c == "" {
		return false
	}
	if strings.Contains(c, "citext") {
		return true
	}
	if strings.Contains(c, "_ci") || strings.Contains(c, "-ci") || strings.Contains(c, "case_insensitive") {
		return true
	}
	// ICU strength primary/secondary: ks-level1 hoặc ks-level2 bỏ qua hoa/thường
	if strings.Contains(c, "ks-level1") || strings.Contains(c, "ks-level2") {
		return true
	}
	if strings.Contains(c, "strength=primary") || strings.Contains(c, "strength=secondary") {
		return true
	}
	if strings.Contains(c, "nondeterministic") || strings.Contains(c, "deterministic=false") {
		return true
	}
	return false
}

// isMySQLAccentSensitive trả về true nếu collation MySQL phân biệt dấu.
func isMySQLAccentSensitive(c string) bool {
	if c == "" {
		return true
	}
	if strings.Contains(c, "_ai") {
		return false
	}
	if strings.Contains(c, "_as") || strings.Contains(c, "_bin") || strings.Contains(c, "_cs") || c == "binary" {
		return true
	}
	// Mặc định các collation MySQL truyền thống (vd: utf8mb4_general_ci, latin1_swedish_ci) là accent-sensitive
	return true
}

// isPGAccentInsensitive trả về true nếu collation PG bỏ qua dấu.
func isPGAccentInsensitive(c string) bool {
	if c == "" {
		return false
	}
	if strings.Contains(c, "_ai") || strings.Contains(c, "-ai") || strings.Contains(c, "accent_insensitive") {
		return true
	}
	if strings.Contains(c, "ks-level1") {
		return true
	}
	return false
}

// isMySQLNoPad trả về true nếu collation MySQL sử dụng ngữ nghĩa NO PAD.
func isMySQLNoPad(c string) bool {
	return strings.Contains(c, "nopad") || strings.Contains(c, "no_pad")
}

// isPGPadSpace trả về true nếu collation PG sử dụng ngữ nghĩa PAD SPACE.
func isPGPadSpace(c string) bool {
	return strings.Contains(c, "pad space") || strings.Contains(c, "pad_space")
}

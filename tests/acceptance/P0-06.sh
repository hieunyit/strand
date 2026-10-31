#!/usr/bin/env bash
# Test nghiệm thu P0-06: Thí nghiệm watermark (RS-2)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-06 ==="

# 1. AC1: Chạy N vòng (mặc định 10.000) trên MySQL 8.4 và 9.7; xuất JSON {iterations, violations}
echo "--- AC1: Kiểm chứng watermark low -> SELECT trên MySQL 8.4 và MySQL 9.7 ---"
if go test -v -count=1 ./tests/experiments/watermark -run "TestAC1_Watermark_MySQL84|TestAC1_Watermark_MySQL97"; then
  pass "AC1: Watermark visible 100% trên cả MySQL 8.4 và MySQL 9.7 (0 violations)"
else
  fail "AC1: Thí nghiệm watermark trên MySQL 8.4 hoặc 9.7 thất bại"
fi

# 2. AC2: Tự kiểm: bản cố ý đọc low SAU câu SELECT phải phát hiện violations > 0
echo "--- AC2: Tự kiểm (negative control): đọc low SAU SELECT phải phát hiện violations > 0 ---"
if go test -v -count=1 ./tests/experiments/watermark -run TestAC2_Watermark_NegativeSelfCheck; then
  pass "AC2: Tự kiểm tra phát hiện violations > 0 đúng như thiết kế khi đọc low sau SELECT"
else
  fail "AC2: TestAC2_Watermark_NegativeSelfCheck thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

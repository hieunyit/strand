#!/usr/bin/env bash
# Test nghiệm thu P0-07: Mô hình TLA+ (S02 §2, §12)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-07: Mô hình TLA+ ==="

# 1. AC1: Mỗi module kiểm đúng các bất biến ghi ở S02 §12, tên bất biến trùng mã (CP1, RS1…)
echo "--- AC1: Tên bất biến trùng mã quy định trong spec ---"
REQUIRED_INVARIANTS=(
  "tla/Scheduler.tla:CO1"
  "tla/Checkpoint.tla:CP1"
  "tla/Checkpoint.tla:CP2"
  "tla/Checkpoint.tla:CP4"
  "tla/RangeState.tla:RS1"
)

AC1_OK=1
for item in "${REQUIRED_INVARIANTS[@]}"; do
  file="${item%%:*}"
  inv="${item##*:}"
  if [ ! -f "$file" ]; then
    echo "  Thiếu file: $file"
    AC1_OK=0
  elif ! grep -q -E "^[[:space:]]*${inv}[[:space:]]*==" "$file"; then
    echo "  $file thiếu định nghĩa bất biến $inv"
    AC1_OK=0
  fi
done

# Kiểm tra module Handover có bất biến
if [ ! -f "tla/Handover.tla" ]; then
  echo "  Thiếu file: tla/Handover.tla"
  AC1_OK=0
elif ! grep -q -E "^[[:space:]]*(HO1|HD1|NoDuplicates|NoGaps)[[:space:]]*==" tla/Handover.tla; then
  echo "  tla/Handover.tla thiếu định nghĩa bất biến"
  AC1_OK=0
fi

if [ "$AC1_OK" -eq 1 ]; then
  pass "AC1: Mỗi module TLA+ định nghĩa đúng các bất biến (CO1, CP1, CP2, CP4, RS1, HO1)"
else
  fail "AC1: Chưa định nghĩa đủ các bất biến chuẩn trong các module TLA+"
fi

# 2. AC2: TLC không tìm thấy vi phạm ở biên đã ghi (chạy tla/run.sh)
echo "--- AC2: TLC kiểm chứng mô hình không vi phạm ở biên S02 §12 ---"
if [ -f "tla/run.sh" ] && bash tla/run.sh check; then
  pass "AC2: TLC xác nhận 4 module không có vi phạm bất biến ở biên đã ghi"
else
  fail "AC2: Chạy TLC thất bại hoặc tìm thấy vi phạm bất biến"
fi

# 3. AC3: Mỗi module có một biến thể cố ý cài lỗi, và TLC PHẢI tìm ra phản ví dụ
echo "--- AC3: TLC tìm ra phản ví dụ với các biến thể lỗi (mutation) ---"
if [ -f "tla/run.sh" ] && bash tla/run.sh mutants; then
  pass "AC3: TLC phát hiện phản ví dụ cho tất cả các biến thể cài lỗi"
else
  fail "AC3: TLC không tìm ra phản ví dụ trên các biến thể cài lỗi"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

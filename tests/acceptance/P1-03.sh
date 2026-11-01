#!/usr/bin/env bash
# Test nghiệm thu P1-03: Txn Assembler (S02 §3)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-03: Txn Assembler ==="

# 1. AC1: Lắp đúng Txn nhiều bảng
echo "--- AC1: Lắp đúng Txn nhiều bảng ---"
if go test -v -count=1 ./internal/assemble -run TestAC1_MultiTableTxn; then
  pass "AC1: Lắp đúng Txn nhiều bảng"
else
  fail "AC1: Lắp đúng Txn nhiều bảng thất bại"
fi

# 2. AC2: UPDATE đổi PK có OldKey
echo "--- AC2: UPDATE đổi PK có OldKey ---"
if go test -v -count=1 ./internal/assemble -run TestAC2_UpdatePKWithOldKey; then
  pass "AC2: UPDATE đổi PK có OldKey thành công"
else
  fail "AC2: UPDATE đổi PK có OldKey thất bại"
fi

# 3. AC3: Txn 50 triệu row không giữ hết trong RAM
echo "--- AC3: Txn 50 triệu row không giữ hết trong RAM ---"
if go test -v -count=1 -timeout=180s ./internal/assemble -run TestAC3_LargeTxnSpill_50MillionRows; then
  pass "AC3: Txn 50 triệu row không giữ hết trong RAM (RAM duy trì ở mức an toàn)"
else
  fail "AC3: TestAC3_LargeTxnSpill_50MillionRows thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

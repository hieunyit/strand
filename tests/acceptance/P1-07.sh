#!/usr/bin/env bash
# Test nghiệm thu P1-07: Writer ghi delta có điều kiện epoch (S02 §4 quy tắc 3, 6)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-07: Writer ghi delta có điều kiện epoch ==="

# 1. AC1: TestSG1_StaleEpochRollsBack: epoch cũ thì rollback, trả fenced
echo "--- AC1: TestSG1_StaleEpochRollsBack: epoch cũ thì rollback, trả fenced ---"
out1=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestSG1_StaleEpochRollsBack 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestSG1_StaleEpochRollsBack"; then
  pass "AC1: TestSG1_StaleEpochRollsBack: epoch cũ thì rollback, trả fenced"
else
  echo "$out1"
  fail "AC1: TestSG1_StaleEpochRollsBack thất bại"
fi

# 2. AC2: TestCP1_DeltaSameTxn: delta ghi cùng transaction với dữ liệu
echo "--- AC2: TestCP1_DeltaSameTxn: delta ghi cùng transaction với dữ liệu ---"
out2=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestCP1_DeltaSameTxn 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestCP1_DeltaSameTxn"; then
  pass "AC2: TestCP1_DeltaSameTxn: delta ghi cùng transaction với dữ liệu"
else
  echo "$out2"
  fail "AC2: TestCP1_DeltaSameTxn thất bại"
fi

# 3. AC3: TestAC3_MySQLClientFoundRows: MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng
echo "--- AC3: TestAC3_MySQLClientFoundRows: MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng ---"
out3=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestAC3_MySQLClientFoundRows 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestAC3_MySQLClientFoundRows"; then
  pass "AC3: TestAC3_MySQLClientFoundRows: MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng"
else
  echo "$out3"
  fail "AC3: TestAC3_MySQLClientFoundRows thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
# Test nghiệm thu P1-08: Scheduler writeset đầy đủ (S02 §6)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-08: Scheduler writeset đầy đủ ==="

# 1. AC1: TestCO1: writeset gồm PK, unique key, FK
echo "--- AC1: writeset gồm PK, unique key, FK ---"
out1=$(go test -v -count=1 -timeout=180s ./internal/sched -run TestCO1_FullWriteset_PK_UK_FK 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestCO1_FullWriteset_PK_UK_FK"; then
  pass "AC1: writeset gồm PK, unique key, FK"
else
  echo "$out1"
  fail "AC1: writeset gồm PK, unique key, FK thất bại"
fi

# 2. AC2: TestCO2: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng
echo "--- AC2: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng (TestCO2) ---"
out2=$(go test -v -count=1 -timeout=180s ./internal/sched -run TestCO2_WritesetBudgetTableLevel 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestCO2_WritesetBudgetTableLevel"; then
  pass "AC2: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng (TestCO2)"
else
  echo "$out2"
  fail "AC2: vượt writeset_budget thì bảng lớn nhất chuyển sang cấp bảng (TestCO2) thất bại"
fi

# 3. AC3: 100 Txn × 49.999 khóa không OOM
echo "--- AC3: 100 Txn × 49.999 khóa không OOM ---"
out3=$(go test -v -count=1 -timeout=300s ./internal/sched -run TestCO2_100Txn_49999Keys_NoOOM 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestCO2_100Txn_49999Keys_NoOOM"; then
  pass "AC3: 100 Txn × 49.999 khóa không OOM"
else
  echo "$out3"
  fail "AC3: 100 Txn × 49.999 khóa không OOM thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
# Test nghiệm thu P1-09: Writer MySQL và group commit (S02 §6, CP-5)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-09: Writer MySQL và group commit ==="

# 1. AC1: multi-row upsert
echo "--- AC1: multi-row upsert ---"
out1=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestAC1_MySQLMultiRowUpsert 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestAC1_MySQLMultiRowUpsert"; then
  pass "AC1: multi-row upsert"
else
  echo "$out1"
  fail "AC1: multi-row upsert thất bại"
fi

# 2. AC2: một Txn nguồn không bị tách
echo "--- AC2: một Txn nguồn không bị tách ---"
out2=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestAC2_TxnNeverSplit 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestAC2_TxnNeverSplit"; then
  pass "AC2: một Txn nguồn không bị tách"
else
  echo "$out2"
  fail "AC2: một Txn nguồn không bị tách thất bại"
fi

# 3. AC3: chế độ strict cho đích luôn là prefix của source (TestCP5)
echo "--- AC3: chế độ strict cho đích luôn là prefix của source (TestCP5) ---"
out3=$(go test -v -count=1 -timeout=180s ./internal/writer -run TestCP5_StrictCommitOrderPrefix 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestCP5_StrictCommitOrderPrefix"; then
  pass "AC3: chế độ strict cho đích luôn là prefix của source (TestCP5)"
else
  echo "$out3"
  fail "AC3: chế độ strict cho đích luôn là prefix của source (TestCP5) thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

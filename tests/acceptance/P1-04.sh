#!/usr/bin/env bash
# Test nghiệm thu P1-04: Spool Pebble (tầng local) (S02 §3, D §1)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-04: Spool Pebble (tầng local) ==="

# 1. AC1: Append/Iter/Truncate đúng sau restart
echo "--- AC1: Append/Iter/Truncate đúng sau restart ---"
if go test -v -count=1 ./internal/spool -run TestAC1_RestartAppendIterTruncate; then
  pass "AC1: Append/Iter/Truncate đúng sau restart"
else
  fail "AC1: Append/Iter/Truncate đúng sau restart thất bại"
fi

# 2. AC2: Usage báo đúng số byte
echo "--- AC2: Usage báo đúng số byte ---"
if go test -v -count=1 ./internal/spool -run TestAC2_UsageBytesAccuracy; then
  pass "AC2: Usage báo đúng số byte"
else
  fail "AC2: Usage báo đúng số byte thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

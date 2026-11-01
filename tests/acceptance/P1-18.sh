#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-18: Admin socket và lệnh vận hành ==="
FAIL=0

echo "--- AC1: top hoạt động qua socket ---"
out1=$(go test -race -v -run ^TestAD1_TopOverSocket$ ./internal/admin 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestAD1_TopOverSocket"; then
  echo "  [PASS] AC1: top hoạt động qua socket"
else
  echo "$out1"
  echo "  [FAIL] AC1: top hoạt động qua socket"
  FAIL=1
fi

echo "--- AC2: pause và resume --snapshot-only hoạt động qua socket ---"
out2=$(go test -race -v -run ^TestAD2_PauseAndResumeOverSocket$ ./internal/admin 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestAD2_PauseAndResumeOverSocket"; then
  echo "  [PASS] AC2: pause và resume --snapshot-only hoạt động qua socket"
else
  echo "$out2"
  echo "  [FAIL] AC2: pause và resume --snapshot-only hoạt động qua socket"
  FAIL=1
fi

echo "--- AC3: throttle set hoạt động qua socket ---"
out3=$(go test -race -v -run ^TestAD3_ThrottleSetOverSocket$ ./internal/admin 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestAD3_ThrottleSetOverSocket"; then
  echo "  [PASS] AC3: throttle set hoạt động qua socket"
else
  echo "$out3"
  echo "  [FAIL] AC3: throttle set hoạt động qua socket"
  FAIL=1
fi

echo "--- AC4: checkpoint show|rewind hoạt động qua socket ---"
out4=$(go test -race -v -run ^TestAD4_CheckpointShowAndRewindOverSocket$ ./internal/admin 2>&1 || true)
if echo "$out4" | grep -q "PASS: TestAD4_CheckpointShowAndRewindOverSocket"; then
  echo "  [PASS] AC4: checkpoint show|rewind hoạt động qua socket"
else
  echo "$out4"
  echo "  [FAIL] AC4: checkpoint show|rewind hoạt động qua socket"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

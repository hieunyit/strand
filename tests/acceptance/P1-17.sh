#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-17: Promote ==="
FAIL=0

echo "--- AC1: ứng dụng không ghi được trước khi setval xong ---"
out1=$(go test -race -v -run ^TestPM1_AppCannotWriteBeforeSetval$ ./internal/promote 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestPM1_AppCannotWriteBeforeSetval"; then
  echo "  [PASS] AC1: ứng dụng không ghi được trước khi setval xong"
else
  echo "$out1"
  echo "  [FAIL] AC1: ứng dụng không ghi được trước khi setval xong"
  FAIL=1
fi

echo "--- AC2: chạy lại được từ bước đang dở ---"
out2=$(go test -race -v -run ^TestPM2_ResumableFromInterruptedStep$ ./internal/promote 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestPM2_ResumableFromInterruptedStep"; then
  echo "  [PASS] AC2: chạy lại được từ bước đang dở"
else
  echo "$out2"
  echo "  [FAIL] AC2: chạy lại được từ bước đang dở"
  FAIL=1
fi

echo "--- AC3: source không truy cập được thì trả exit 10 promote_needs_decision ---"
out3=$(go test -race -v -run ^TestPM3_UnreachableSource_Exit10PromoteNeedsDecision$ ./internal/promote 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestPM3_UnreachableSource_Exit10PromoteNeedsDecision"; then
  echo "  [PASS] AC3: source không truy cập được thì trả exit 10 promote_needs_decision"
else
  echo "$out3"
  echo "  [FAIL] AC3: source không truy cập được thì trả exit 10 promote_needs_decision"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

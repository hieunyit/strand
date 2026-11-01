#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-16: Governor onoff + breaker ==="
FAIL=0

echo "--- AC1: tín hiệu đọc trực tiếp từ DB, không phụ thuộc metrics_source ---"
out1=$(go test -race -v -run ^TestGV1_DirectDBSignals_NoMetricsSourceDependency$ ./internal/governor 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestGV1_DirectDBSignals_NoMetricsSourceDependency"; then
  echo "  [PASS] AC1: tín hiệu đọc trực tiếp từ DB, không phụ thuộc metrics_source"
else
  echo "$out1"
  echo "  [FAIL] AC1: tín hiệu đọc trực tiếp từ DB, không phụ thuộc metrics_source"
  FAIL=1
fi

echo "--- AC2: P >= 1 thì dừng background, P < 0.9 trong 3 tick thì chạy lại ---"
out2=$(go test -race -v -run ^TestGV2_Onoff_PressurePauseAndResumeAfter3Ticks$ ./internal/governor 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestGV2_Onoff_PressurePauseAndResumeAfter3Ticks"; then
  echo "  [PASS] AC2: P >= 1 thì dừng background, P < 0.9 trong 3 tick thì chạy lại"
else
  echo "$out2"
  echo "  [FAIL] AC2: P >= 1 thì dừng background, P < 0.9 trong 3 tick thì chạy lại"
  FAIL=1
fi

echo "--- AC3: breaker cắt R còn 50% ---"
out3=$(go test -race -v -run ^TestGV3_Breaker_CutsRate50Percent$ ./internal/governor 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestGV3_Breaker_CutsRate50Percent"; then
  echo "  [PASS] AC3: breaker cắt R còn 50%"
else
  echo "$out3"
  echo "  [FAIL] AC3: breaker cắt R còn 50%"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

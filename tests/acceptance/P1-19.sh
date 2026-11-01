#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-19: Metric và /healthz ==="
FAIL=0

echo "--- AC1: đủ metric trong S02 §11, tên đúng ---"
out1=$(go test -race -v -run ^TestMT1_AllMetricsRegisteredAndScraped$ ./internal/metrics 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestMT1_AllMetricsRegisteredAndScraped"; then
  echo "  [PASS] AC1: đủ metric trong S02 §11, tên đúng"
else
  echo "$out1"
  echo "  [FAIL] AC1: đủ metric trong S02 §11, tên đúng"
  FAIL=1
fi

echo "--- AC2: /healthz trả 200 khi vòng lặp chính còn tiến ---"
out2=$(go test -race -v -run ^TestMT2_HealthzReturns200WhenProgressing$ ./internal/metrics 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestMT2_HealthzReturns200WhenProgressing"; then
  echo "  [PASS] AC2: /healthz trả 200 khi vòng lặp chính còn tiến"
else
  echo "$out2"
  echo "  [FAIL] AC2: /healthz trả 200 khi vòng lặp chính còn tiến"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-15: Verify sample/delta ==="
FAIL=0

echo "--- AC1: phát hiện lệch được cài sẵn ---"
out1=$(go test -race -v -run ^TestVF1_VerifyDetectsInjectedMismatch$ ./internal/verify 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestVF1_VerifyDetectsInjectedMismatch"; then
  echo "  [PASS] AC1: phát hiện lệch được cài sẵn"
else
  echo "$out1"
  echo "  [FAIL] AC1: phát hiện lệch được cài sẵn"
  FAIL=1
fi

echo "--- AC2: hot row được báo unverifiable_hot, không báo lệch giả ---"
out2=$(go test -race -v -run ^TestVF2_HotRow_ReportedUnverifiableHot_NoFalsePositive$ ./internal/verify 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestVF2_HotRow_ReportedUnverifiableHot_NoFalsePositive"; then
  echo "  [PASS] AC2: hot row được báo unverifiable_hot, không báo lệch giả"
else
  echo "$out2"
  echo "  [FAIL] AC2: hot row được báo unverifiable_hot, không báo lệch giả"
  FAIL=1
fi

echo "--- AC3: hoạt động đúng trên cả hai chế độ sample và delta ---"
out3=$(go test -race -v -run ^TestVF3_VerifyModes_SampleAndDelta$ ./internal/verify 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestVF3_VerifyModes_SampleAndDelta"; then
  echo "  [PASS] AC3: hoạt động đúng trên cả hai chế độ sample và delta"
else
  echo "$out3"
  echo "  [FAIL] AC3: hoạt động đúng trên cả hai chế độ sample và delta"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

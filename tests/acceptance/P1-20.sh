#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-20: Unit systemd và manifest FKE ==="
FAIL=0

echo "--- AC1: unit có RestartPreventExitStatus=2 3 4 5 10 và manifest FKE đúng spec ---"
out1=$(go test -race -v -run ^TestSD1_SystemdUnitAndFKEManifest$ ./cmd/strand 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestSD1_SystemdUnitAndFKEManifest"; then
  echo "  [PASS] AC1: unit có RestartPreventExitStatus=2 3 4 5 10 và manifest FKE đúng spec"
else
  echo "$out1"
  echo "  [FAIL] AC1: unit có RestartPreventExitStatus=2 3 4 5 10 và manifest FKE đúng spec"
  FAIL=1
fi

echo "--- AC2: SIGTERM thì ghi checkpoint rồi thoát trong thời gian cho phép ---"
out2=$(go test -race -v -run ^TestSD2_SIGTERM_FlushesCheckpointAndExitsInTime$ ./cmd/strand 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestSD2_SIGTERM_FlushesCheckpointAndExitsInTime"; then
  echo "  [PASS] AC2: SIGTERM thì ghi checkpoint rồi thoát trong thời gian cho phép"
else
  echo "$out2"
  echo "  [FAIL] AC2: SIGTERM thì ghi checkpoint rồi thoát trong thời gian cho phép"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

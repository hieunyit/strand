#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-21: Checker độc lập + bộ fault injection ==="
FAIL=0

echo "--- AC1: 5 phép kiểm của S04 §3 ---"
out1=$(go test -race -v -run ^TestCK1_FiveVerificationChecks$ ./tests/checker 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestCK1_FiveVerificationChecks"; then
  echo "  [PASS] AC1: 5 phép kiểm của S04 §3"
else
  echo "$out1"
  echo "  [FAIL] AC1: 5 phép kiểm của S04 §3"
  FAIL=1
fi

echo "--- AC2: checker bắt được 3 bản cài lỗi ---"
out2=$(go test -race -v -run ^TestCK2_CheckerCatchesThreeInjectedBugs$ ./tests/checker 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestCK2_CheckerCatchesThreeInjectedBugs"; then
  echo "  [PASS] AC2: checker bắt được 3 bản cài lỗi"
else
  echo "$out2"
  echo "  [FAIL] AC2: checker bắt được 3 bản cài lỗi"
  FAIL=1
fi

echo "--- AC3: các kịch bản fault chạy được bằng make fault ---"
# make fault chạy các kịch bản fault thật trong tests/e2e (Toxiproxy, kill -9, hai instance).
out3=$(make fault 2>&1 || true)
if echo "$out3" | grep -q -- "--- PASS: TestG1_E2E_NetworkFaultsSourceAndTarget" \
  && echo "$out3" | grep -q -- "--- PASS: TestG1_E2E_Kill9RandomCrashes" \
  && echo "$out3" | grep -q -- "--- PASS: TestG1_E2E_TwoInstancesSamePipeline" \
  && ! echo "$out3" | grep -q -- "--- FAIL"; then
  echo "  [PASS] AC3: các kịch bản fault chạy được bằng make fault"
else
  echo "$out3"
  echo "  [FAIL] AC3: các kịch bản fault chạy được bằng make fault"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

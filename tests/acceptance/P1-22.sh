#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-22: Bộ test Gate 1 ==="
FAIL=0

echo "--- AC1: mỗi test Gate 1 có một test tự động ---"
# Bằng chứng Gate 1 là các test e2e thật TestG1_E2E_* (process thật, MySQL thật).
out1=$(go test -v -timeout 180m -run '^TestG1_E2E_' ./tests/e2e 2>&1 || true)
if echo "$out1" | grep -q -- "--- PASS: TestG1_E2E_" && ! echo "$out1" | grep -q -- "--- FAIL"; then
  echo "  [PASS] AC1: mỗi test Gate 1 có một test tự động"
else
  echo "$out1"
  echo "  [FAIL] AC1: mỗi test Gate 1 có một test tự động"
  FAIL=1
fi

echo "--- AC2: 1.000 lần kill -9 cho kết quả 0 lệch ---"
out2=$(STRAND_E2E_KILL9_ITERATIONS=1000 go test -v -timeout 180m -run '^TestG1_E2E_Kill9RandomCrashes$' ./tests/e2e 2>&1 || true)
if echo "$out2" | grep -q -- "--- PASS: TestG1_E2E_Kill9RandomCrashes"; then
  echo "  [PASS] AC2: 1.000 lần kill -9 cho kết quả 0 lệch"
else
  echo "$out2"
  echo "  [FAIL] AC2: 1.000 lần kill -9 cho kết quả 0 lệch"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

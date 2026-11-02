#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-15: Verify sample/delta ==="
FAIL=0

# Verifier/ImageTracker cũ (chỉ chạy trên dữ liệu trong RAM) đã bị xóa; verify thật là
# LiveVerifier, kiểm bằng e2e trên MySQL thật dưới tải có hot row:
# AC1 lệch cài vào target bị phát hiện (exit 5), AC2 không báo lệch giả dưới tải nóng,
# AC3 chạy cả --mode sample và --mode delta.
out=$(go test -v -timeout 60m -run '^TestP1_E2E_VerifySampleDeltaRepair$' ./tests/e2e 2>&1 || true)
if echo "$out" | grep -q -- "--- PASS: TestP1_E2E_VerifySampleDeltaRepair"; then
  echo "  [PASS] AC1: phát hiện lệch được cài sẵn"
  echo "  [PASS] AC2: hot row được báo unverifiable_hot, không báo lệch giả"
  echo "  [PASS] AC3: hoạt động đúng trên cả hai chế độ sample và delta"
else
  echo "$out"
  echo "  [FAIL] AC1-AC3: TestP1_E2E_VerifySampleDeltaRepair"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

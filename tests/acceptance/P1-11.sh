#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-11: UPDATE đổi PK qua hai range ==="
FAIL=0

echo "--- AC1: PK cũ và PK mới ở hai range khác trạng thái: đích không còn PK cũ, 0 lệch ---"
out1=$(go test -race -v -run ^TestRS_UpdatePK_AcrossDifferentRangeStates$ ./internal/snapshot 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestRS_UpdatePK_AcrossDifferentRangeStates"; then
  echo "  [PASS] AC1: PK cũ và PK mới ở hai range khác trạng thái: đích không còn PK cũ, 0 lệch"
else
  echo "$out1"
  echo "  [FAIL] AC1: PK cũ và PK mới ở hai range khác trạng thái"
  FAIL=1
fi

echo "--- AC2: đổi liên tiếp 1->2->3 đúng ---"
out2=$(go test -race -v -run ^TestRS_UpdatePK_SequentialChain$ ./internal/snapshot 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestRS_UpdatePK_SequentialChain"; then
  echo "  [PASS] AC2: đổi liên tiếp 1->2->3 đúng"
else
  echo "$out2"
  echo "  [FAIL] AC2: đổi liên tiếp 1->2->3 đúng"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

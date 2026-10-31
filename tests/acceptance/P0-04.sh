#!/usr/bin/env bash
# Test nghiệm thu P0-04: Kiểu dữ liệu lõi và schema seed.json
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-04 ==="

# 1. AC1: Property test: Merge giao hoán, kết hợp, lũy đẳng; Contains đúng sau Merge
echo "--- AC1: Property test: Merge giao hoán, kết hợp, lũy đẳng; Contains đúng sau Merge ---"
if go test -v -count=1 ./internal/checkpoint -run TestAC1_PropertyMerge; then
  pass "AC1: Property test Merge (giao hoán, kết hợp, lũy đẳng, Contains) thành công"
else
  fail "AC1: TestAC1_PropertyMerge thất bại"
fi

# 2. AC2: TestCP3: serialize Txn/Checkpoint không có trường seq
echo "--- AC2: TestCP3: serialize Txn/Checkpoint không có trường seq ---"
if go test -v -count=1 ./internal/model -run TestCP3_NoSeqInSerializedTxnOrCheckpoint; then
  pass "AC2: TestCP3: serialize Txn/Checkpoint không chứa trường seq"
else
  fail "AC2: TestCP3_NoSeqInSerializedTxnOrCheckpoint thất bại"
fi

# 3. AC3: Lwd/High/Holes đúng với các chuỗi Add ngẫu nhiên
echo "--- AC3: Lwd/High/Holes đúng với các chuỗi Add ngẫu nhiên ---"
if go test -v -count=1 ./internal/checkpoint -run TestAC3_LwdHighHolesRandomAdd; then
  pass "AC3: Lwd/High/Holes hoạt động chính xác với chuỗi Add ngẫu nhiên"
else
  fail "AC3: TestAC3_LwdHighHolesRandomAdd thất bại"
fi

# 4. AC4: Schema chấp nhận ví dụ seed.json ở S02 §9, từ chối khi thiếu trường bắt buộc
echo "--- AC4: Schema chấp nhận ví dụ seed.json ở S02 §9, từ chối khi thiếu trường bắt buộc ---"
if go test -v -count=1 ./schema -run TestAC4_SeedSchemaValidation; then
  pass "AC4: Schema seed.schema.json kiểm thực đúng định dạng seed.json"
else
  fail "AC4: TestAC4_SeedSchemaValidation thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
# Test nghiệm thu P0-05: Prototype writer theo writeset + công cụ replay
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-05 ==="

# 1. AC1: TestCO1: hai Txn có writeset giao nhau commit đúng thứ tự source
echo "--- AC1: TestCO1: hai Txn có writeset giao nhau commit đúng thứ tự source ---"
if go test -v -count=1 ./internal/sched -run TestCO1; then
  pass "AC1: TestCO1 hai Txn có writeset giao nhau commit đúng thứ tự source thành công"
else
  fail "AC1: TestCO1 thất bại"
fi

# 2. AC2: Txn vượt 50.000 khóa được nâng lên khóa cấp bảng; map row được dọn
echo "--- AC2: Txn vượt 50.000 khóa được nâng lên khóa cấp bảng; map row được dọn ---"
if go test -v -count=1 ./internal/sched -run TestAC2_TableLevelLock; then
  pass "AC2: Nâng khóa cấp bảng và dọn map row thành công khi vượt ngưỡng"
else
  fail "AC2: TestAC2_TableLevelLock thất bại"
fi

# 3. AC3 & AC4: Replay tải genload từ mysql-src sang mysql-dst, checksum bằng nhau, chạy --workers 4, 8, 16
echo "--- AC3 & AC4: Replay tải genload từ mysql-src sang mysql-dst với --workers 4, 8, 16 và xuất JSON ---"
if go test -v -count=1 ./tests/replay -run TestAC3_AC4_ReplayBenchmark; then
  pass "AC3 & AC4: Replay checksum bằng nhau trên mọi bảng và xuất đầy đủ số liệu JSON với workers 4, 8, 16"
else
  fail "AC3 & AC4: TestAC3_AC4_ReplayBenchmark thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

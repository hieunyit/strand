#!/usr/bin/env bash
# Test nghiệm thu P1-02: Reader MySQL (S02 §3, D §1)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-02: Reader MySQL ==="

# 1. AC1: Đọc stream có tagged GTID
echo "--- AC1: Đọc stream có tagged GTID (MySQL 8.4+) ---"
if go test -v -count=1 -timeout=300s ./internal/reader/mysql -run TestAC1_TaggedGTIDStream; then
  pass "AC1: Đọc stream có tagged GTID thành công"
else
  fail "AC1: Đọc stream có tagged GTID thất bại"
fi

# 2. AC2: Resume từ AppliedSet đúng
echo "--- AC2: Resume từ AppliedSet đúng ---"
if go test -v -count=1 -timeout=300s ./internal/reader/mysql -run TestAC2_ResumeFromAppliedSet; then
  pass "AC2: Resume từ AppliedSet đúng"
else
  fail "AC2: Resume từ AppliedSet thất bại"
fi

# 3. AC3: Đổi replica giữa hai lần chạy vẫn resume đúng (TestCP2)
echo "--- AC3: Đổi replica giữa hai lần chạy vẫn resume đúng (TestCP2) ---"
if go test -v -count=1 -timeout=300s ./internal/reader/mysql -run TestCP2_SwitchReplicaResume; then
  pass "AC3: TestCP2 đổi replica giữa hai lần chạy vẫn resume đúng, không lặp Txn"
else
  fail "AC3: TestCP2 đổi replica thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

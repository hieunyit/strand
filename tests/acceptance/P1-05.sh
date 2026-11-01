#!/usr/bin/env bash
# Test nghiệm thu P1-05: Bảng metadata, khóa pipeline, epoch (S02 §4)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-05: Bảng metadata, khóa pipeline, epoch ==="

# 1. AC1: Tạo đủ 3 bảng trên MySQL và PG target
echo "--- AC1: Tạo đủ 3 bảng trên MySQL và PG target ---"
out1=$(go test -v -count=1 -timeout=180s ./internal/checkpoint -run TestAC1_EnsureMetadataTables 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestAC1_EnsureMetadataTables"; then
  pass "AC1: Tạo đủ 3 bảng trên MySQL và PG target"
else
  echo "$out1"
  fail "AC1: Tạo đủ 3 bảng trên MySQL và PG target thất bại"
fi

# 2. AC2: TestSG1: instance thứ hai trả pipeline_locked
echo "--- AC2: TestSG1: instance thứ hai trả pipeline_locked ---"
out2=$(go test -v -count=1 -timeout=180s ./internal/checkpoint -run TestSG1_PipelineLockSecondInstance 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestSG1_PipelineLockSecondInstance"; then
  pass "AC2: TestSG1: instance thứ hai trả pipeline_locked"
else
  echo "$out2"
  fail "AC2: TestSG1: instance thứ hai trả pipeline_locked thất bại"
fi

# 3. AC3: Epoch tăng mỗi lần khởi động
echo "--- AC3: Epoch tăng mỗi lần khởi động ---"
out3=$(go test -v -count=1 -timeout=180s ./internal/checkpoint -run TestAC3_EpochIncreasesOnStartup 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestAC3_EpochIncreasesOnStartup"; then
  pass "AC3: Epoch tăng mỗi lần khởi động"
else
  echo "$out3"
  fail "AC3: Epoch tăng mỗi lần khởi động thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

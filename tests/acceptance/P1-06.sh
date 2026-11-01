#!/usr/bin/env bash
# Test nghiệm thu P1-06: Checkpoint toàn cục và compaction (S02 §4 quy tắc 4, D §1)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-06: Checkpoint toàn cục và compaction ==="

# 1. AC1: TestCP1 kill ở mọi bước compaction đều không mất
echo "--- AC1: TestCP1 kill ở mọi bước compaction đều không mất ---"
out1=$(go test -v -count=1 -timeout=180s ./internal/checkpoint -run TestCP1_KillDuringCompaction 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestCP1_KillDuringCompaction"; then
  pass "AC1: TestCP1 kill ở mọi bước compaction đều không mất"
else
  echo "$out1"
  fail "AC1: TestCP1 kill ở mọi bước compaction đều không mất thất bại"
fi

# 2. AC2: delta ≤ 64 KB, vượt thì compaction đồng bộ
echo "--- AC2: delta ≤ 64 KB, vượt thì compaction đồng bộ ---"
out2=$(go test -v -count=1 -timeout=180s ./internal/checkpoint -run TestCP4_DeltaSyncCompaction 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestCP4_DeltaSyncCompaction"; then
  pass "AC2: delta ≤ 64 KB, vượt thì compaction đồng bộ"
else
  echo "$out2"
  fail "AC2: delta ≤ 64 KB, vượt thì compaction đồng bộ thất bại"
fi

# 3. AC3: holes > 4.096 thì scheduler ngừng nhận (TestCP4)
echo "--- AC3: holes > 4.096 thì scheduler ngừng nhận (TestCP4) ---"
out3=$(go test -v -count=1 -timeout=180s ./internal/sched -run TestCP4_SchedulerStopsOnMaxHoles 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestCP4_SchedulerStopsOnMaxHoles"; then
  pass "AC3: holes > 4.096 thì scheduler ngừng nhận (TestCP4)"
else
  echo "$out3"
  fail "AC3: holes > 4.096 thì scheduler ngừng nhận (TestCP4) thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-14: Archive catch-up, prefetch, spool tràn ==="
FAIL=0

echo "--- AC1: xóa archive trong lúc seed apply thì prefetch vẫn cứu được ---"
out1=$(go test -race -v -run ^TestPF1_PrefetchRescuesDeletedArchive$ ./internal/seed 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestPF1_PrefetchRescuesDeletedArchive"; then
  echo "  [PASS] AC1: xóa archive trong lúc seed apply thì prefetch vẫn cứu được"
else
  echo "$out1"
  echo "  [FAIL] AC1: xóa archive trong lúc seed apply thì prefetch vẫn cứu được"
  FAIL=1
fi

echo "--- AC2: tắt prefetch thì báo log_gap ---"
out2=$(go test -race -v -run ^TestPF2_DisabledPrefetch_ReportsLogGap$ ./internal/seed 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestPF2_DisabledPrefetch_ReportsLogGap"; then
  echo "  [PASS] AC2: tắt prefetch thì báo log_gap"
else
  echo "$out2"
  echo "  [FAIL] AC2: tắt prefetch thì báo log_gap"
  FAIL=1
fi

echo "--- AC3: mọi tầng spool đầy thì báo prefetch_exhausted ---"
out3=$(go test -race -v -run ^TestPF3_AllSpoolTiersFull_ReportsPrefetchExhausted$ ./internal/seed 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestPF3_AllSpoolTiersFull_ReportsPrefetchExhausted"; then
  echo "  [PASS] AC3: mọi tầng spool đầy thì báo prefetch_exhausted"
else
  echo "$out3"
  echo "  [FAIL] AC3: mọi tầng spool đầy thì báo prefetch_exhausted"
  FAIL=1
fi

echo "--- AC4: spool tràn sang bucket (MinIO trong compose) ---"
out4=$(go test -race -v -run ^TestPF4_SpoolOverflowToBucket_MinIO$ ./internal/seed 2>&1 || true)
if echo "$out4" | grep -q "PASS: TestPF4_SpoolOverflowToBucket_MinIO"; then
  echo "  [PASS] AC4: spool tràn sang bucket (MinIO trong compose)"
else
  echo "$out4"
  echo "  [FAIL] AC4: spool tràn sang bucket (MinIO trong compose)"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

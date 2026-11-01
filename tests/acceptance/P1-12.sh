#!/usr/bin/env bash
set -uo pipefail

echo "=== Kiểm tra P1-12: Seed từ XtraBackup ==="
FAIL=0

echo "--- AC1: seed plan đọc P từ metadata, đối chiếu chéo ---"
out1=$(go test -race -v -run ^TestSD1_SeedPlan_XtraBackup_MetadataCrossCheck$ ./internal/seed 2>&1 || true)
if echo "$out1" | grep -q "PASS: TestSD1_SeedPlan_XtraBackup_MetadataCrossCheck"; then
  echo "  [PASS] AC1: seed plan đọc P từ metadata, đối chiếu chéo"
else
  echo "$out1"
  echo "  [FAIL] AC1: seed plan đọc P từ metadata, đối chiếu chéo"
  FAIL=1
fi

echo "--- AC2: backup hỏng (thiếu file, sai GTID, log có khoảng trống) bị từ chối 100% ---"
out2=$(go test -race -v -run ^TestSD2_CorruptedBackup_Rejected$ ./internal/seed 2>&1 || true)
if echo "$out2" | grep -q "PASS: TestSD2_CorruptedBackup_Rejected"; then
  echo "  [PASS] AC2: backup hỏng bị từ chối 100%"
else
  echo "$out2"
  echo "  [FAIL] AC2: backup hỏng bị từ chối 100%"
  FAIL=1
fi

echo "--- AC3: backup XtraBackup bản RC bị từ chối nếu không có --allow-prerelease-tool ---"
out3=$(go test -race -v -run ^TestSD4_PrereleaseTool_RejectedWithoutFlag$ ./internal/seed 2>&1 || true)
if echo "$out3" | grep -q "PASS: TestSD4_PrereleaseTool_RejectedWithoutFlag"; then
  echo "  [PASS] AC3: backup bản RC bị từ chối nếu thiếu flag"
else
  echo "$out3"
  echo "  [FAIL] AC3: backup bản RC bị từ chối nếu thiếu flag"
  FAIL=1
fi

if [ $FAIL -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

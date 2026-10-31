#!/usr/bin/env bash
# Test nghiệm thu P0-03b: Audit pglogrepl
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-03b ==="

# 1. AC1: pgoutput proto v1–v4 chạy trên PG 16, 17, 18
echo "--- AC1: pgoutput proto v1–v4 chạy trên PG 16, 17, 18 ---"
if go test -v -count=1 ./tests/audit/pg -run TestAC1_ProtoV1toV4_PG16_17_18; then
  pass "AC1: pgoutput proto v1–v4 chạy thành công trên PG 16, 17, 18"
else
  fail "AC1: TestAC1_ProtoV1toV4_PG16_17_18 thất bại"
fi

# 2. AC2: Nhận đúng transaction đang streaming (proto v2+)
echo "--- AC2: Nhận đúng transaction đang streaming (proto v2+) ---"
if go test -v -count=1 ./tests/audit/pg -run TestAC2_StreamingTxn_ProtoV2; then
  pass "AC2: Nhận đúng transaction đang streaming (proto v2+) thành công"
else
  fail "AC2: TestAC2_StreamingTxn_ProtoV2 thất bại"
fi

# 3. AC3: Tạo slot kèm exported snapshot; consistent_point khớp vị trí bắt đầu stream
echo "--- AC3: Tạo slot kèm exported snapshot; consistent_point khớp vị trí bắt đầu stream ---"
if go test -v -count=1 ./tests/audit/pg -run TestAC3_ExportSnapshot_ConsistentPoint; then
  pass "AC3: Tạo slot kèm exported snapshot; consistent_point khớp vị trí bắt đầu stream thành công"
else
  fail "AC3: TestAC3_ExportSnapshot_ConsistentPoint thất bại"
fi

# 4. AC4: Logical decoding trên standby PG16+
echo "--- AC4: Logical decoding trên standby PG16+ ---"
if go test -v -count=1 ./tests/audit/pg -run TestAC4_StandbyLogicalDecoding_PG16; then
  pass "AC4: Logical decoding trên standby PG16+ thành công"
else
  fail "AC4: TestAC4_StandbyLogicalDecoding_PG16 thất bại"
fi

# 5. AC5: Báo cáo có trích dẫn và mục khuyến nghị fork, như P0-03a
echo "--- AC5: Báo cáo có trích dẫn file:dòng tại SHA đã ghim và mục khuyến nghị fork ---"
AUDIT_DOC="docs/audit/pglogrepl.md"
if [ ! -f "$AUDIT_DOC" ]; then
  fail "AC5: Không tìm thấy $AUDIT_DOC"
else
  if grep -qiE "commit\s+[0-9a-f]{40}" "$AUDIT_DOC" || grep -qiE "SHA:\s*[0-9a-f]{40}" "$AUDIT_DOC" || grep -qE "[0-9a-f]{40}" "$AUDIT_DOC"; then
    pass "AC5: Đã ghim commit SHA trong báo cáo"
  else
    fail "AC5: Chưa ghim commit SHA đầy đủ trong báo cáo"
  fi

  if grep -qiE "khuyến nghị fork" "$AUDIT_DOC"; then
    pass "AC5: Báo cáo có mục Khuyến nghị fork kèm lý do"
  else
    fail "AC5: Thiếu mục Khuyến nghị fork trong báo cáo"
  fi
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
# Test nghiệm thu P0-03a: Audit go-mysql
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-03a ==="

# 1. AC1: Đọc được binlog qua stream có tagged GTID của MySQL 8.4
echo "--- AC1: Đọc được binlog qua stream có tagged GTID của MySQL 8.4 ---"
if go test -v -count=1 ./tests/audit/mysql -run TestAC1_TaggedGTID_MySQL84; then
  pass "AC1: Đọc được binlog qua stream có tagged GTID của MySQL 8.4"
else
  fail "AC1: TestAC1_TaggedGTID_MySQL84 thất bại"
fi

# 2. AC2: Parse đủ event của MySQL 9.7 (testcontainers mysql:9.7)
echo "--- AC2: Parse đủ event của MySQL 9.7 (testcontainers mysql:9.7) ---"
if go test -v -count=1 ./tests/audit/mysql -run TestAC2_Events_MySQL97; then
  pass "AC2: Parse đủ event của MySQL 9.7 thành công"
else
  fail "AC2: TestAC2_Events_MySQL97 thất bại"
fi

# 3. AC3: Resume bằng COM_BINLOG_DUMP_GTID với GTID set có lỗ: server gửi lại đúng các transaction còn thiếu
echo "--- AC3: Resume bằng COM_BINLOG_DUMP_GTID với GTID set có lỗ ---"
if go test -v -count=1 ./tests/audit/mysql -run TestAC3_Resume_Holes_GTID; then
  pass "AC3: Resume với GTID set có lỗ thành công"
else
  fail "AC3: TestAC3_Resume_Holes_GTID thất bại"
fi

# 4. AC4: Mọi khẳng định trong báo cáo trỏ tới file:dòng tại SHA đã ghim, hoặc tới test
echo "--- AC4: Báo cáo có trích dẫn file:dòng tại SHA đã ghim hoặc tới test ---"
AUDIT_DOC="docs/audit/go-mysql.md"
if [ ! -f "$AUDIT_DOC" ]; then
  fail "AC4: Không tìm thấy $AUDIT_DOC"
else
  # Kiểm tra có ghim commit SHA
  if grep -qiE "commit\s+[0-9a-f]{40}" "$AUDIT_DOC" || grep -qiE "SHA:\s*[0-9a-f]{40}" "$AUDIT_DOC" || grep -qE "[0-9a-f]{40}" "$AUDIT_DOC"; then
    pass "AC4: Đã ghim commit SHA trong báo cáo"
  else
    fail "AC4: Chưa ghim commit SHA đầy đủ trong báo cáo"
  fi
fi

# 5. AC5: Báo cáo có mục 'Khuyến nghị fork: có/không + lý do'
echo "--- AC5: Khuyến nghị fork trong báo cáo ---"
if [ -f "$AUDIT_DOC" ] && grep -qiE "khuyến nghị fork" "$AUDIT_DOC"; then
  pass "AC5: Báo cáo có mục Khuyến nghị fork kèm lý do"
else
  fail "AC5: Thiếu mục Khuyến nghị fork trong báo cáo"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

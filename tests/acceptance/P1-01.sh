#!/usr/bin/env bash
# Test nghiệm thu P1-01: Cấu hình và khung CLI (S02 §10, §12)
set -euo pipefail

FAIL=0
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P1-01: Cấu hình và khung CLI ==="

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# Biên dịch binary strand vào thư mục tạm
echo "--- Biên dịch strand CLI ---"
go build -o "$TMPDIR/strand" ./cmd/strand

# 1. AC1: Key lạ trong YAML trả exit 2
echo "--- AC1: Decode YAML strict, key lạ trả exit code 2 ---"

# File config chứa key lạ unknown_field_abc
cat << 'EOF' > "$TMPDIR/invalid_unknown_key.yaml"
version: 1
name: test-pipeline
unknown_field_abc: "not_allowed"
source:
  kind: mysql
  dsn: "root:dev@tcp(127.0.0.1:3306)/app"
target:
  kind: mysql
  dsn: "root:dev@tcp(127.0.0.1:3307)/app"
EOF

# Chạy lệnh strand validate với config có key lạ
set +e
"$TMPDIR/strand" validate -c "$TMPDIR/invalid_unknown_key.yaml" >/dev/null 2>&1
EC1=$?
set -e

if [ "$EC1" -eq 2 ]; then
  pass "AC1: Cấu hình chứa key lạ trả đúng exit code 2"
else
  fail "AC1: Cấu hình chứa key lạ mong đợi exit code 2, nhưng nhận được $EC1"
fi

# 2. AC2: `strand config schema` in JSON Schema hợp lệ
echo "--- AC2: strand config schema in JSON Schema hợp lệ ---"
SCHEMA_OUT="$TMPDIR/schema.json"
set +e
"$TMPDIR/strand" config schema > "$SCHEMA_OUT" 2>&1
EC2=$?
set -e

if [ "$EC2" -ne 0 ]; then
  fail "AC2: Lệnh strand config schema thất bại với exit code $EC2"
else
  # Kiểm tra tính hợp lệ của JSON bằng python3
  if python3 -c 'import json, sys; d = json.load(open(sys.argv[1])); assert "properties" in d or "$schema" in d' "$SCHEMA_OUT" 2>/dev/null; then
    pass "AC2: strand config schema in JSON Schema hợp lệ"
  else
    fail "AC2: Output của strand config schema không phải là JSON Schema hợp lệ"
  fi
fi

# 3. AC3: Mỗi exit code trong S02 §10 có test
echo "--- AC3: Kiểm tra mỗi exit code trong S02 §10 có test (0, 1, 2, 3, 4, 5, 10) ---"
if go test -v -count=1 ./internal/exitcode -run TestExitCodes; then
  pass "AC3: Tất cả exit code trong S02 §10 (0, 1, 2, 3, 4, 5, 10) có test kiểm chứng đầy đủ"
else
  fail "AC3: Kiểm thử bảng exit code thất bại"
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

#!/usr/bin/env bash
# Kiểm tra cơ học cho một task. Agent chạy ở Bước 4 và 6; bạn chạy lại một lần khi nghiệm thu.
#   bash scripts/check.sh <TASK-ID>
# Ghi toàn bộ output thật vào .strand/runs/<ID>-<sha>.txt
set -uo pipefail
T="${1:?TASK-ID}"
SHA="$(git rev-parse HEAD)"; BASE="$(git merge-base main HEAD)"
mkdir -p .strand/runs; OUT=".strand/runs/$T-${SHA:0:12}.txt"
FAIL=0
exec > >(tee "$OUT") 2>&1
echo "task=$T sha=$SHA base=${BASE:0:12} time=$(date -Is)"
[ -n "$(git status --porcelain --untracked-files=no)" ] && { echo "LỖI: còn thay đổi chưa commit"; FAIL=1; }
ok() { echo "  OK   $1"; }; bad() { echo "  LỖI  $1"; FAIL=1; }

echo "== 1. File không được sửa"
P="$(git diff --name-only "$BASE..HEAD" -- spec GEMINI.md .agent scripts/check.sh)"
[ -z "$P" ] && ok "spec/, GEMINI.md, .agent/, check.sh nguyên vẹn" || bad "đã sửa: $P"

echo "== 2. Test nghiệm thu bị khóa sau commit test($T): acceptance"
TC="$(git log --format='%H %s' "$BASE..HEAD" | awk -v t="test($T): acceptance" 'index($0,t)==42{print $1}' | tail -1)"
if [ -z "$TC" ]; then bad "không có commit test($T): acceptance"
else
  L="$(git diff --name-only "$TC..HEAD" -- "tests/acceptance/${T}"*)"
  [ -z "$L" ] && ok "test nghiệm thu không đổi kể từ ${TC:0:12}" || bad "test nghiệm thu bị sửa sau khi khóa: $L"
fi

echo "== 3. Không skip, không xóa test"
D="$(git diff "$BASE..HEAD" -- '*_test.go')"
S="$(echo "$D" | grep -E '^\+.*(t\.Skip|testing\.Short\(\)|go:build.*ignore)' || true)"
R="$(echo "$D" | grep -E '^-func Test' || true)"
[ -z "$S" ] && ok "không thêm skip" || bad "thêm skip: $S"
[ -z "$R" ] && ok "không xóa test" || bad "xóa test: $R"

echo "== 4. Build và test"
if ls go.mod >/dev/null 2>&1; then
  go vet ./... && ok "go vet" || bad "go vet"
  go test -race -count=1 ./... && ok "go test" || bad "go test"
  go test -count=1 -v ./... 2>/dev/null | grep -E -- '--- SKIP' && bad "có test bị SKIP" || ok "không có test SKIP"
else
  echo "  (chưa có go.mod, bỏ qua build Go)"
fi
if [ -f "tests/acceptance/$T.sh" ]; then
  bash "tests/acceptance/$T.sh" && ok "acceptance $T.sh" || bad "acceptance $T.sh"
fi

echo "== KẾT QUẢ: $([ $FAIL = 0 ] && echo PASS || echo FAIL)   file: $OUT"
exit $FAIL

#!/usr/bin/env bash
# Chạy bộ e2e Gate 1 (01 §6) trên MySQL thật qua testcontainers.
#   STRAND_E2E_MYSQL_IMAGE   ảnh MySQL cho cả source và target (mặc định mysql:8.4)
#   STRAND_E2E_SOURCE_IMAGE  ghi đè ảnh source (ví dụ mysql:8.0 cho smoke test vai trò source)
#   STRAND_E2E_KILL9_ITERATIONS  mặc định 1000
#   STRAND_E2E_BIGTXN_ROWS       mặc định 50000000
#   SEED                     chạy lại đúng kịch bản
set -euo pipefail
cd "$(dirname "$0")/.."
go test -count=1 -timeout 6h -v ./tests/e2e/ "$@"

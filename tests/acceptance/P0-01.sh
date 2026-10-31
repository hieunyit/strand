#!/usr/bin/env bash
# Test nghiệm thu P0-01: Môi trường docker compose
set -euo pipefail

FAIL=0
COMPOSE_FILE="dev/compose.yaml"
PROJECT_NAME="strand-dev"

# Helper check
pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAIL=1; }

echo "=== Kiểm tra P0-01 ==="

# 1. AC1: make up xong, mọi service healthy trong <= 300s
echo "--- AC1: Kiểm tra make up và healthcheck 7 service ---"
if ! make up; then
  fail "AC1: 'make up' thất bại"
else
  SERVICES=("mysql-src" "mysql-dst" "pg-src" "pg-dst" "toxiproxy" "victoriametrics" "grafana")
  DEADLINE=$((SECONDS + 300))
  ALL_HEALTHY=0

  while [ $SECONDS -lt $DEADLINE ]; do
    UNHEALTHY=0
    for s in "${SERVICES[@]}"; do
      # Tìm container ID của service theo compose file hoặc project
      CID="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" ps -q "$s" 2>/dev/null || true)"
      if [ -z "$CID" ]; then
        UNHEALTHY=1
        break
      fi
      STATUS="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$CID" 2>/dev/null || echo "missing")"
      if [ "$STATUS" != "healthy" ]; then
        UNHEALTHY=1
        break
      fi
    done

    if [ "$UNHEALTHY" -eq 0 ]; then
      ALL_HEALTHY=1
      break
    fi
    sleep 2
  done

  if [ "$ALL_HEALTHY" -eq 1 ]; then
    pass "AC1: Cả 7 service healthy trong thời gian cho phép"
  else
    fail "AC1: Quá 300s vẫn còn service chưa healthy"
  fi
fi

# 2. AC2: mysql-src: gtid_mode=ON, binlog_row_image=FULL, binlog_row_metadata=FULL
echo "--- AC2: Kiểm tra cấu hình mysql-src ---"
M_SRC_VARS="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T mysql-src mysql -uroot -pdev -N -e "SELECT @@gtid_mode, @@binlog_row_image, @@binlog_row_metadata;" 2>/dev/null || echo "ERROR")"
if echo "$M_SRC_VARS" | grep -q -w "ON" && echo "$M_SRC_VARS" | grep -q -w "FULL" && [ "$(echo "$M_SRC_VARS" | awk '{print $1,$2,$3}')" = "ON FULL FULL" ]; then
  pass "AC2: mysql-src: gtid_mode=ON, binlog_row_image=FULL, binlog_row_metadata=FULL"
else
  fail "AC2: mysql-src cấu hình sai: '$M_SRC_VARS' (mong đợi: 'ON FULL FULL')"
fi

# 3. AC3: User strand có trên cả 4 DB; trên pg-src có REPLICATION
echo "--- AC3: Kiểm tra user strand và quyền REPLICATION ---"
U_M_SRC="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T mysql-src mysql -uroot -pdev -N -e "SELECT count(*) FROM mysql.user WHERE user='strand';" 2>/dev/null || echo "0")"
U_M_DST="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T mysql-dst mysql -uroot -pdev -N -e "SELECT count(*) FROM mysql.user WHERE user='strand';" 2>/dev/null || echo "0")"
U_PG_SRC="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T pg-src psql -U postgres -t -c "SELECT count(*) FROM pg_roles WHERE rolname='strand' AND rolreplication = true;" 2>/dev/null || echo "0")"
U_PG_DST="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T pg-dst psql -U postgres -t -c "SELECT count(*) FROM pg_roles WHERE rolname='strand';" 2>/dev/null || echo "0")"

U_M_SRC="$(echo "$U_M_SRC" | tr -d '[:space:]')"
U_M_DST="$(echo "$U_M_DST" | tr -d '[:space:]')"
U_PG_SRC="$(echo "$U_PG_SRC" | tr -d '[:space:]')"
U_PG_DST="$(echo "$U_PG_DST" | tr -d '[:space:]')"

if [ "$U_M_SRC" -ge 1 ] && [ "$U_M_DST" -ge 1 ] && [ "$U_PG_SRC" -ge 1 ] && [ "$U_PG_DST" -ge 1 ]; then
  pass "AC3: User strand tồn tại trên cả 4 DB, pg-src có quyền REPLICATION"
else
  fail "AC3: User strand không hợp lệ (m-src:$U_M_SRC, m-dst:$U_M_DST, pg-src(repl):$U_PG_SRC, pg-dst:$U_PG_DST)"
fi

# 4. AC4: mysql-dst: read_only=1, super_read_only=0
echo "--- AC4: Kiểm tra cấu hình mysql-dst ---"
M_DST_VARS="$(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T mysql-dst mysql -uroot -pdev -N -e "SELECT @@read_only, @@super_read_only;" 2>/dev/null || echo "ERROR")"
M_DST_TRIMMED="$(echo "$M_DST_VARS" | awk '{print $1,$2}')"
if [ "$M_DST_TRIMMED" = "1 0" ]; then
  pass "AC4: mysql-dst: read_only=1, super_read_only=0"
else
  fail "AC4: mysql-dst cấu hình sai: '$M_DST_VARS' (mong đợi: '1 0')"
fi

# 5. AC5: make down không để lại volume
echo "--- AC5: Kiểm tra make down không để lại volume ---"
if ! make down; then
  fail "AC5: 'make down' thất bại"
else
  REMAINING_VOLS="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT_NAME" 2>/dev/null || true)"
  if [ -z "$REMAINING_VOLS" ]; then
    pass "AC5: 'make down' không để lại volume nào của project"
  else
    fail "AC5: Còn sót volume sau make down: $REMAINING_VOLS"
  fi
fi

if [ "$FAIL" -eq 0 ]; then
  echo "=== KẾT QUẢ: TẤT CẢ AC ĐÃ PASS ==="
  exit 0
else
  echo "=== KẾT QUẢ: CÓ AC THẤT BẠI ==="
  exit 1
fi

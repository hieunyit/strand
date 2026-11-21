#!/usr/bin/env bash
# Kiểm tra công cụ native đóng sẵn trong image Web (Dockerfile).
#
#   scripts/image-tools-test.sh [IMAGE]            # mặc định strand:local
#   LIVE=1 scripts/image-tools-test.sh [IMAGE]     # thêm smoke test với server thật (Docker)
#
# Phần tĩnh (luôn chạy, với user strand trong image):
#   - mỗi công cụ Strand gọi được tìm thấy qua PATH (như ResolveTool khi không đặt STRAND_BACKUP_TOOLS_DIR),
#     là file thường, có quyền chạy, không ghi được bởi group/other, thuộc root, user strand không ghi được;
#   - `--version` chạy được; mysqldump là của Oracle (không phải MariaDB), mariadb-dump là của MariaDB;
#   - client MySQL đọc được file option mặc định (không vấp cấu hình của MariaDB trong /etc/mysql).
# Phần LIVE: dựng container server (tiền tố $PREFIX, xóa khi xong), chạy đúng argv Strand dựng
# (internal/backup/runner/methods.go, restore.go) từ trong image: dump + nạp lại + đếm dòng.
#   LIVE_TARGETS (mặc định "mysql84 mysql80 mysql57 mariadb pg mongo mydumper")
#   MYSQL57_IMAGE, MYSQL80_IMAGE, MYSQL84_IMAGE, MARIADB_IMAGE, PG_IMAGES (danh sách), MONGO_IMAGE để đổi image server.
# Mật khẩu sinh ngẫu nhiên, chỉ truyền qua biến môi trường (docker run -e NAME, không có giá trị trong argv).
set -euo pipefail

IMAGE="${1:-strand:local}"
PREFIX="${PREFIX:-strand-imgtest-}"
FAIL=0
ok()  { printf '  OK    %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; FAIL=1; }

TOOLS="mysqldump mysqldump-8.0 mysql mysqlbinlog mariadb-dump mariadb mydumper myloader pg_dump pg_restore pg_basebackup psql pg_dump-16 pg_restore-16 mongodump mongorestore"

echo "== image: $IMAGE"
docker image inspect "$IMAGE" --format '   size: {{.Size}} bytes' || { echo "image $IMAGE not found" >&2; exit 2; }

echo "== static checks (user strand)"
# shellcheck disable=SC2016
out="$(docker run --rm --user strand --entrypoint /bin/sh -e TOOLS="$TOOLS" "$IMAGE" -c '
set -u
for t in $TOOLS; do
  p="$(command -v "$t")" || { echo "FAIL $t: not found in PATH"; continue; }
  r="$(readlink -f "$p")"
  m="$(stat -L -c %a "$p")"; o="$(stat -L -c %U "$p")"
  [ -f "$r" ] && [ -x "$r" ] || { echo "FAIL $t: $r is not an executable file"; continue; }
  case "$m" in ?[2367]?|??[2367]) echo "FAIL $t: $r mode $m is group/world-writable"; continue;; esac
  [ "$o" = root ] || { echo "FAIL $t: $r owned by $o"; continue; }
  [ -w "$r" ] && { echo "FAIL $t: writable by strand"; continue; }
  v="$("$t" --version 2>&1 | head -n1)" || { echo "FAIL $t: --version failed: $v"; continue; }
  echo "OK $t -> $r ($m) :: $v"
done
mysqldump --version | grep -q "MySQL Community" && ! mysqldump --version | grep -q MariaDB \
  && echo "OK mysqldump is Oracle MySQL" || echo "FAIL mysqldump is not Oracle MySQL"
mariadb-dump --version | grep -q MariaDB && echo "OK mariadb-dump is MariaDB" || echo "FAIL mariadb-dump is not MariaDB"
for t in mysql mysqldump mysqlbinlog mariadb mariadb-dump; do
  e="$("$t" --print-defaults 2>&1)" && echo "OK $t option files: $e" || echo "FAIL $t --print-defaults: $e"
done
[ -z "${STRAND_BACKUP_TOOLS_DIR:-}" ] && echo "OK STRAND_BACKUP_TOOLS_DIR unset (PATH lookup)" || echo "OK STRAND_BACKUP_TOOLS_DIR=$STRAND_BACKUP_TOOLS_DIR"
strand version >/dev/null 2>&1 && echo "OK strand version" || echo "FAIL strand version"
')"
while IFS= read -r l; do
  case "$l" in OK\ *) ok "${l#OK }";; FAIL\ *) bad "${l#FAIL }";; *) echo "        $l";; esac
done <<<"$out"

if [ "${LIVE:-0}" != 1 ]; then
  [ "$FAIL" = 0 ] && echo "PASS (static)" || echo "FAILED"
  exit "$FAIL"
fi

# ---------------------------------------------------------------------------------------------- LIVE
LIVE_TARGETS="${LIVE_TARGETS:-mysql84 mysql80 mysql57 mariadb pg mongo mydumper}"
NET="${PREFIX}net"
PW="$(head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \n')"
export MYSQL_ROOT_PASSWORD="$PW" MARIADB_ROOT_PASSWORD="$PW" POSTGRES_PASSWORD="$PW" MYSQL_PWD="$PW" PGPASSWORD="$PW" \
  MONGO_INITDB_ROOT_USERNAME=root MONGO_INITDB_ROOT_PASSWORD="$PW" PW
CREATED=()
cleanup() {
  for c in "${CREATED[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create "$NET" >/dev/null
start() { # name image [docker run options...] [-- server args...]
  local n="$PREFIX$1" img="$2" opts=(); shift 2
  while [ $# -gt 0 ] && [ "$1" != -- ]; do opts+=("$1"); shift; done
  [ $# -gt 0 ] && shift
  CREATED+=("$n")
  docker run -d --name "$n" --network "$NET" "${opts[@]}" "$img" "$@" >/dev/null
}
wait_mysql() { # container client
  for _ in $(seq 1 120); do
    docker exec -e MYSQL_PWD "$1" "$2" -uroot -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "timeout waiting for $1" >&2; return 1
}
# tool: chạy lệnh trong image (user strand), mật khẩu chỉ qua env.
tool() { docker run --rm -i --network "$NET" --user strand --entrypoint /bin/bash -e MYSQL_PWD -e PGPASSWORD -e PW "$IMAGE" -c "$1"; }

seed_mysql() { # container client
  docker exec -i -e MYSQL_PWD "$1" "$2" -uroot -h127.0.0.1 <<'SQL'
CREATE DATABASE app;
CREATE TABLE app.t (id INT PRIMARY KEY, v VARCHAR(20), b BLOB);
INSERT INTO app.t VALUES (1,'a',0x00ff),(2,'b',NULL),(3,'c','x');
USE app;
CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v;
SQL
}
# Argv của MySQLDumpInvocation (single database) + nạp bằng mysqlClient của restore.go.
MYSQLDUMP_COMMON='--single-transaction --quick --routines --events --triggers --hex-blob --default-character-set=utf8mb4'
mysql_roundtrip() { # label host dumptool posflags client
  local label="$1" host="$2" dt="$3" pos="$4" cl="$5"
  if res="$(tool "set -euo pipefail
    $dt --host=$host --port=3306 --user=root $MYSQLDUMP_COMMON $pos -- app > /tmp/d.sql
    grep -Eo \"CHANGE (MASTER|REPLICATION SOURCE) TO (MASTER|SOURCE)_LOG_FILE='[^']+', (MASTER|SOURCE)_LOG_POS=[0-9]+\" /tmp/d.sql | head -n1
    $cl --host=$host --port=3306 --user=root -e 'CREATE DATABASE app_restored'
    # restore.go đặt NewGTIDPurgedFilter trước client: bỏ SET @@GLOBAL.GTID_PURGED (có thể nhiều dòng).
    sed '/^SET @@GLOBAL.GTID_PURGED/,/;\$/d' /tmp/d.sql |
      $cl --host=$host --port=3306 --user=root --binary-mode --default-character-set=utf8mb4 --database=app_restored
    $cl --host=$host --port=3306 --user=root -N -e 'SELECT COUNT(*) FROM app_restored.t; DROP DATABASE app_restored'" 2>&1)"; then
    ok "$label: $(echo "$res" | grep -v 'Warning\|WARNING' | tr '\n' ' ')"
  else
    bad "$label: $res"
  fi
}

echo "== live smoke ($LIVE_TARGETS)"
for target in $LIVE_TARGETS; do
  case "$target" in
  mysql84)
    start mysql84 "${MYSQL84_IMAGE:-mysql:8.4}" -e MYSQL_ROOT_PASSWORD -- --gtid-mode=ON --enforce-gtid-consistency=ON
    wait_mysql "${PREFIX}mysql84" mysql && seed_mysql "${PREFIX}mysql84" mysql
    mysql_roundtrip "mysqldump 8.4 -> MySQL 8.4 (--source-data=2)" "${PREFIX}mysql84" mysqldump --source-data=2 mysql
    if res="$(tool "set -euo pipefail; f=\$(mysql --host=${PREFIX}mysql84 --user=root -N -e 'SHOW BINARY LOG STATUS' | cut -f1)
      mysqlbinlog --read-from-remote-server --host=${PREFIX}mysql84 --user=root \"\$f\" | grep -c 'Query\\|GTID' " 2>&1)"; then
      ok "mysqlbinlog 8.4 reads MySQL 8.4 binlog remotely ($res events)"; else bad "mysqlbinlog 8.4: $res"; fi
    ;;
  mysql80)
    # mysqldump >= 8.2 gửi SHOW BINARY LOG STATUS (chỉ có từ 8.2): với server 8.0 Strand chọn mysqldump-8.0
    # (runner.mysqlDumpTool). Kiểm cả hai: 8.0 chạy được, 8.4 hỏng đúng như lý do chọn.
    start mysql80 "${MYSQL80_IMAGE:-mysql:8.0}" -e MYSQL_ROOT_PASSWORD -- --gtid-mode=ON --enforce-gtid-consistency=ON
    wait_mysql "${PREFIX}mysql80" mysql && seed_mysql "${PREFIX}mysql80" mysql
    mysql_roundtrip "mysqldump-8.0 -> ${MYSQL80_IMAGE:-mysql:8.0} (--source-data=2)" "${PREFIX}mysql80" mysqldump-8.0 --source-data=2 mysql
    if res="$(tool "mysqldump --host=${PREFIX}mysql80 --user=root --single-transaction --source-data=2 -- app 2>&1 >/dev/null" 2>&1)"; then
      bad "mysqldump 8.4 unexpectedly dumped a MySQL 8.0 server with --source-data: Strand's mysqldump-8.0 selection may be unnecessary"
    elif echo "$res" | grep -q 'BINARY LOG STATUS'; then
      ok "mysqldump 8.4 -> MySQL 8.0 fails as expected (SHOW BINARY LOG STATUS): Strand uses mysqldump-8.0"
    else bad "mysqldump 8.4 -> MySQL 8.0: unexpected error: $res"; fi
    if res="$(tool "set -euo pipefail; f=\$(mysql --host=${PREFIX}mysql80 --user=root -N -e 'SHOW MASTER STATUS' | cut -f1)
      mysqlbinlog --read-from-remote-server --host=${PREFIX}mysql80 --user=root \"\$f\" | grep -c 'Query\\|GTID' " 2>&1)"; then
      ok "mysqlbinlog 8.4 reads MySQL 8.0 binlog remotely ($res events)"; else bad "mysqlbinlog 8.4 vs 8.0: $res"; fi
    ;;
  mysql57)
    start mysql57 "${MYSQL57_IMAGE:-mysql:5.7}" -e MYSQL_ROOT_PASSWORD -- --server-id=1 --log-bin=mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON
    wait_mysql "${PREFIX}mysql57" mysql && seed_mysql "${PREFIX}mysql57" mysql
    mysql_roundtrip "mysqldump 8.4 -> MySQL 5.7 (--source-data=2)" "${PREFIX}mysql57" mysqldump --source-data=2 mysql
    if res="$(tool "set -euo pipefail; f=\$(mysql --host=${PREFIX}mysql57 --user=root -N -e 'SHOW MASTER STATUS' | cut -f1)
      mysqlbinlog --read-from-remote-server --host=${PREFIX}mysql57 --user=root \"\$f\" | grep -c 'Query\\|GTID' " 2>&1)"; then
      ok "mysqlbinlog 8.4 reads MySQL 5.7 binlog remotely ($res events)"; else bad "mysqlbinlog 8.4 vs 5.7: $res"; fi
    ;;
  mariadb)
    start mariadb "${MARIADB_IMAGE:-mariadb:11.4}" -e MARIADB_ROOT_PASSWORD -- --log-bin=mariadb-bin --server-id=1
    wait_mysql "${PREFIX}mariadb" mariadb && seed_mysql "${PREFIX}mariadb" mariadb
    # restore.go nạp mọi dump MySQL/MariaDB bằng `mysql` (Oracle 8.4 trong image).
    mysql_roundtrip "mariadb-dump -> ${MARIADB_IMAGE:-mariadb:11.4} (--master-data=2 --gtid), load with mysql 8.4" \
      "${PREFIX}mariadb" mariadb-dump "--master-data=2 --gtid" mysql
    ;;
  pg)
    for img in ${PG_IMAGES:-postgres:15 postgres:16 postgres:17 postgres:18}; do
    # Chọn công cụ như runner.SelectPGDump/SelectPGRestore: server <= 16 dùng pg_dump-16/pg_restore-16.
    major="$(docker run --rm --entrypoint postgres "$img" --version | grep -o '[0-9][0-9]*' | head -n1)"
    if [ "$major" -le 16 ]; then dump=pg_dump-16; rest=pg_restore-16; else dump=pg_dump; rest=pg_restore; fi
    n="pg$(echo "$img" | tr -c 'a-z0-9\n' '-' | sed 's/^postgres-//')"
    h="$PREFIX$n"
    start "$n" "$img" -e POSTGRES_PASSWORD
    for _ in $(seq 1 60); do docker exec "$h" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1 && break; sleep 1; done
    sleep 2
    docker exec "$h" sh -c 'echo "host replication all all md5" >> "$PGDATA/pg_hba.conf"'
    docker exec "$h" psql -U postgres -qc 'SELECT pg_reload_conf()' -c 'CREATE TABLE t (id int primary key, v text)' \
      -c "INSERT INTO t SELECT g, 'v'||g FROM generate_series(1,100) g" >/dev/null
    # Argv của PGDumpInvocation, rồi pg_restore như restorePGDump (--exit-on-error).
    if res="$(tool "set -euo pipefail
      $dump --host=$h --format=custom --no-password --port=5432 --username=postgres --compress=0 --dbname=postgres > /tmp/d.dump
      pg_restore -l /tmp/d.dump | grep -c 'TABLE DATA'" 2>&1)"; then
      ok "$dump -> $img: custom archive, $res table(s)"; else bad "$dump -> $img: $res"; fi
    if res="$(tool "set -euo pipefail
      $dump --host=$h --format=custom --no-password --port=5432 --username=postgres --compress=0 --dbname=postgres > /tmp/d.dump
      psql -h $h -U postgres -qc 'CREATE DATABASE restored'
      trap \"psql -h $h -U postgres -qc 'DROP DATABASE restored'\" EXIT
      $rest --host=$h --dbname=restored --no-owner --no-privileges --exit-on-error --no-password --port=5432 --username=postgres /tmp/d.dump
      psql -h $h -U postgres -d restored -Atc 'SELECT count(*) FROM t'" 2>&1)"; then
      ok "$rest --exit-on-error (restorePGDump argv) -> $img: $res rows"
    else bad "$rest -> $img: $res"; fi
    if [ "$major" -le 16 ]; then
      if res="$(tool "pg_dump --host=$h --format=custom --no-password --username=postgres --dbname=postgres > /tmp/d.dump
        psql -h $h -U postgres -qc 'CREATE DATABASE restored2'
        pg_restore --host=$h --dbname=restored2 --exit-on-error --no-password --username=postgres /tmp/d.dump; rc=\$?
        psql -h $h -U postgres -qc 'DROP DATABASE restored2'; exit \$rc" 2>&1)"; then
        bad "pg_restore 19 -> $img succeeded: pg_restore-16 selection may be unnecessary"
      elif echo "$res" | grep -q transaction_timeout; then
        ok "pg_restore 19 -> $img fails as expected (SET transaction_timeout): Strand uses pg_restore-16"
      else bad "pg_restore 19 -> $img: unexpected error: $res"; fi
    fi
    if res="$(tool "set -euo pipefail
      pg_basebackup --host=$h --pgdata=- --format=tar --wal-method=fetch --checkpoint=fast --verbose --no-password --label=strand --port=5432 --username=postgres 2>/tmp/log | tar -tf - | grep -c . ; grep -o 'write-ahead log end point: [0-9A-F/]*' /tmp/log" 2>&1)"; then
      ok "pg_basebackup -> $img: $(echo "$res" | tr '\n' ' ')"; else bad "pg_basebackup -> $img: $res"; fi
    done
    ;;
  mongo)
    start mongo "${MONGO_IMAGE:-mongo:7.0}" -e MONGO_INITDB_ROOT_USERNAME -e MONGO_INITDB_ROOT_PASSWORD
    for _ in $(seq 1 60); do docker exec "${PREFIX}mongo" mongosh --quiet -u root -p "$PW" --eval 'db.runCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done
    docker exec -e PW "${PREFIX}mongo" sh -c 'mongosh --quiet -u root -p "$PW" --eval "db.getSiblingDB(\"app\").t.insertMany([{a:1},{a:2},{a:3}])"' >/dev/null
    if res="$(tool "set -euo pipefail; d=\$(mktemp -d)
      printf 'uri: \"mongodb://root:%s@${PREFIX}mongo:27017/?directConnection=true&authSource=admin\"\n' \"\$PW\" > \$d/mongo.yaml; chmod 600 \$d/mongo.yaml
      mongodump --config=\$d/mongo.yaml --archive --db=app > /tmp/a.archive 2>/tmp/e || { tail -n2 /tmp/e; exit 1; }
      mongorestore --config=\$d/mongo.yaml --archive=/tmp/a.archive --nsFrom='app.*' --nsTo='restored.*' 2>&1 | tail -n1" 2>&1)"; then
      ok "mongodump/mongorestore -> ${MONGO_IMAGE:-mongo:7.0}: $res"; else bad "mongodump: $res"; fi
    ;;
  mydumper)
    [ -n "$(docker ps -q -f name="^${PREFIX}mysql84\$")" ] || { start mysql84 "${MYSQL84_IMAGE:-mysql:8.4}" -e MYSQL_ROOT_PASSWORD -- --gtid-mode=ON --enforce-gtid-consistency=ON; wait_mysql "${PREFIX}mysql84" mysql; seed_mysql "${PREFIX}mysql84" mysql; }
    if res="$(tool "set -euo pipefail; d=\$(mktemp -d); printf '[client]\npassword=%s\n' \"\$PW\" > \$d/client.cnf; chmod 600 \$d/client.cnf
      mydumper --defaults-extra-file=\$d/client.cnf --outputdir=\$d/out --threads=4 --triggers --events --routines --host=${PREFIX}mysql84 --port=3306 --user=root --database=app 2>&1 | tail -n2
      echo \"\$(ls \$d/out | grep -c .) files\"; sed -n '/^\[source\]/,/^\$/p' \$d/out/metadata | grep -Ei 'SOURCE_LOG|gtid' | tr '\n' ' '" 2>&1)"; then
      ok "mydumper -> MySQL 8.4: $(echo "$res" | tr '\n' ' ')"; else bad "mydumper: $res"; fi
    ;;
  *) bad "unknown live target $target" ;;
  esac
done

[ "$FAIL" = 0 ] && echo "PASS" || echo "FAILED"
exit "$FAIL"

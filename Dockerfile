# syntax=docker/dockerfile:1
# Strand CDC: image cho `strand web` (Web UI + pipeline chế độ nền chạy trong cùng container).
# Build: docker build -t strand:local .   (hoặc: docker compose up -d --build, xem compose.yaml)
#
# Image kèm sẵn công cụ native mà backup/restore quản lý gọi (docs/CENTRAL-BACKUPS.md "Native tools"):
#   MySQL (Oracle) 8.4 LTS : mysqldump, mysql, mysqlbinlog        -> /opt/mysql/bin, symlink /usr/local/bin
#   MySQL (Oracle) 8.0     : mysqldump-8.0 (chỉ cho server 8.0/8.1) -> /opt/mysql-8.0/bin/mysqldump
#   MariaDB LTS            : mariadb-dump, mariadb                 -> /usr/bin (repo MariaDB)
#   PostgreSQL (PGDG)      : pg_dump, pg_restore, pg_basebackup, psql -> /usr/lib/postgresql/<major>/bin, symlink /usr/local/bin
#   PostgreSQL 16 (PGDG)   : pg_dump-16, pg_restore-16 (chỉ cho server <= 16)
#   MongoDB Database Tools : mongodump, mongorestore               -> /usr/bin
#   mydumper               : mydumper, myloader                    -> /usr/bin
# Mọi phiên bản ghim bằng ARG; repo apt xác thực bằng keyring signed-by (fingerprint ghim), file tải về
# kiểm SHA-256 ghim (tarball MySQL kiểm thêm chữ ký GPG của Oracle). Nâng phiên bản: đổi ARG + checksum,
# rồi chạy `make image-tools-test`.

# --- Phiên bản công cụ (ghim) ---
ARG MYSQL_VERSION=8.4.13
ARG MYSQL_SHA256_AMD64=5dfbb764f731df40ef9e4f5d69aed32bdac8ba1af20bd96b7d40a942d83d4ed2
ARG MYSQL_SHA256_ARM64=6531bb50f64c986a2abdab6b647cbd553e8740b3e5d3867f5415e33568879fc9
# MySQL 8.0 chỉ để lấy mysqldump cho server MySQL 8.0/8.1: mysqldump >= 8.2 gửi SHOW BINARY LOG STATUS (chỉ có
# từ 8.2) nên --source-data hỏng với server 8.0; Strand tự chọn mysqldump-8.0 theo phiên bản server.
ARG MYSQL80_VERSION=8.0.46
ARG MYSQL80_SHA256_AMD64=e2777d490fcaadc5a4be06db89b6935c78909bd9c3382af859ab284af0c950b0
ARG MYSQL80_SHA256_ARM64=49cda0de886b021df1c2e3e382164ca3c61e93e7e5c4ee90849e13862e30dd7f
# MySQL Release Engineering <mysql-build@oss.oracle.com>
ARG MYSQL_GPG_FPR=BCA43417C3B485DD128EC6D4B7B3B788A8D3785C
# MariaDB: repo lưu trữ theo đúng phiên bản (archive.mariadb.org/mariadb-<ver>), không trôi.
ARG MARIADB_VERSION=12.3.4
ARG MARIADB_GPG_FPR=177F4010FE56CA3336300305F1656F24C74CD1D8
# PostgreSQL: major mới nhất (pg_dump phải >= major của server). Để trống PG_CLIENT_VERSION = bản vá mới nhất của major.
ARG PG_MAJOR=19
ARG PG_CLIENT_VERSION=19.0-2.pgdg12+1
# PostgreSQL 16 client cho server <= 16: pg_restore 17+ luôn gửi SET transaction_timeout (server <= 16 từ chối,
# restore dùng --exit-on-error) và pg_restore 16 không đọc được archive của pg_dump 17+ (định dạng 1.16).
# Strand tự dùng pg_dump-16 khi dump server <= 16 và pg_restore-16 khi restore vào server <= 16 (13-16).
ARG PG_LEGACY_MAJOR=16
ARG PG_LEGACY_CLIENT_VERSION=16.16-1.pgdg12+1
ARG PGDG_GPG_FPR=B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
ARG MONGO_TOOLS_VERSION=100.20.1
ARG MONGO_TOOLS_SHA256_AMD64=3659cc8f0a39b551e079dd86624bd1b9422a32333bcc9f1efd8c2f5eaa83dc91
ARG MONGO_TOOLS_SHA256_ARM64=15c027489e7dfbc1259921aa584a3d69c2fe96897fd3e0b88d065265e4a7b577
ARG MYDUMPER_VERSION=1.0.19-1
ARG MYDUMPER_SHA256_AMD64=b75bf3efc1443922f9fb67df40eeaf75c4a7a0223799f20b161e379631e42036
ARG MYDUMPER_SHA256_ARM64=50ec97e372e0bbeb08ab2b1ba4835c4b0eeae687e5cc6a95137af4175415aa30

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/strand ./cmd/strand

# Tải và xác thực công cụ ở stage riêng: gnupg/xz/tarball không vào image cuối.
FROM debian:bookworm-slim AS tools
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
ARG TARGETARCH
ARG MYSQL_VERSION MYSQL_SHA256_AMD64 MYSQL_SHA256_ARM64 MYSQL_GPG_FPR
ARG MYSQL80_VERSION MYSQL80_SHA256_AMD64 MYSQL80_SHA256_ARM64
ARG MARIADB_GPG_FPR PGDG_GPG_FPR
ARG MONGO_TOOLS_VERSION MONGO_TOOLS_SHA256_AMD64 MONGO_TOOLS_SHA256_ARM64
ARG MYDUMPER_VERSION MYDUMPER_SHA256_AMD64 MYDUMPER_SHA256_ARM64
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg xz-utils \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
RUN <<'EOF'
set -euo pipefail
export GNUPGHOME="$(mktemp -d)"
mkdir -p /out/keyrings /out/debs
# primary_fpr FILE: fingerprint khóa chính duy nhất của FILE (từ chối file chứa nhiều khóa).
primary_fpr() {
  local n; n="$(gpg --batch --show-keys --with-colons "$1" | grep -c '^pub:')"
  [ "$n" = 1 ] || { echo "$1: expected exactly one public key, got $n" >&2; return 1; }
  gpg --batch --show-keys --with-colons "$1" | awk -F: '$1=="fpr"{print $10; exit}'
}
# keyring URL FPR NAME: tải khóa apt, kiểm fingerprint ghim, ghi keyring nhị phân cho signed-by.
keyring() {
  curl -fsSL --proto '=https' --tlsv1.2 "$1" -o "$3.asc"
  local got; got="$(primary_fpr "$3.asc")"
  [ "$got" = "$2" ] || { echo "$3: key fingerprint $got != pinned $2" >&2; return 1; }
  gpg --batch --dearmor < "$3.asc" > "/out/keyrings/$3.gpg"
}
keyring https://www.postgresql.org/media/keys/ACCC4CF8.asc "$PGDG_GPG_FPR" strand-pgdg
keyring https://mariadb.org/mariadb_release_signing_key.pgp "$MARIADB_GPG_FPR" strand-mariadb

# fetch URL SHA256 OUT: tải file và kiểm SHA-256 ghim.
fetch() {
  curl -fsSL --proto '=https' --tlsv1.2 --retry 3 "$1" -o "$3"
  echo "$2  $3" | sha256sum -c -
}
case "$TARGETARCH" in
  amd64) my_tar="mysql-${MYSQL_VERSION}-linux-glibc2.28-x86_64-minimal.tar.xz"; my_sha="$MYSQL_SHA256_AMD64"
         my80_tar="mysql-${MYSQL80_VERSION}-linux-glibc2.17-x86_64-minimal.tar.xz"; my80_sha="$MYSQL80_SHA256_AMD64"
         mongo_deb="mongodb-database-tools-debian12-x86_64-${MONGO_TOOLS_VERSION}.deb"; mongo_sha="$MONGO_TOOLS_SHA256_AMD64"
         myd_sha="$MYDUMPER_SHA256_AMD64" ;;
  # Oracle không phát hành bản "minimal" và gói .deb cho arm64: dùng tarball generic đầy đủ (chỉ lấy 3 binary).
  # MongoDB không có .deb debian12/arm64: dùng bản ubuntu2204/arm64 (binary Go, chỉ cần libc + krb5).
  arm64) my_tar="mysql-${MYSQL_VERSION}-linux-glibc2.28-aarch64.tar.xz"; my_sha="$MYSQL_SHA256_ARM64"
         my80_tar="mysql-${MYSQL80_VERSION}-linux-glibc2.28-aarch64.tar.xz"; my80_sha="$MYSQL80_SHA256_ARM64"
         mongo_deb="mongodb-database-tools-ubuntu2204-arm64-${MONGO_TOOLS_VERSION}.deb"; mongo_sha="$MONGO_TOOLS_SHA256_ARM64"
         myd_sha="$MYDUMPER_SHA256_ARM64" ;;
  *) echo "unsupported TARGETARCH=$TARGETARCH (amd64, arm64)" >&2; exit 1 ;;
esac

# MySQL: SHA-256 ghim + chữ ký GPG của Oracle (VALIDSIG phải là khóa ghim; khóa hết hạn vẫn xác minh được chữ ký).
curl -fsSL --proto '=https' https://repo.mysql.com/RPM-GPG-KEY-mysql-2023 -o mysql-key.asc
[ "$(primary_fpr mysql-key.asc)" = "$MYSQL_GPG_FPR" ] || { echo "MySQL key fingerprint mismatch" >&2; exit 1; }
gpg --batch --import mysql-key.asc
# mysql_dist VERSION TARBALL SHA256 DEST BIN...: lấy BIN + libssl/libcrypto riêng của Oracle (RPATH $ORIGIN/../lib/private).
mysql_dist() {
  local ver="$1" tarball="$2" sha="$3" dest="$4"; shift 4
  local url="https://cdn.mysql.com/Downloads/MySQL-${ver%.*}/${tarball}" top="${tarball%.tar.xz}" members=() b
  fetch "$url" "$sha" mysql.tar.xz
  curl -fsSL --proto '=https' "$url.asc" -o mysql.tar.xz.asc
  gpg --batch --status-fd 1 --verify mysql.tar.xz.asc mysql.tar.xz 2>/dev/null | grep -E "^\[GNUPG:\] VALIDSIG .* ${MYSQL_GPG_FPR}\$" >/dev/null \
    || { echo "$tarball: signature invalid" >&2; return 1; }
  for b in "$@"; do members+=("$top/bin/$b"); done
  mkdir -p "$dest"
  tar -xJf mysql.tar.xz -C "$dest" --strip-components=1 --no-same-owner "${members[@]}" \
    "$top/lib/private/libssl.so.3" "$top/lib/private/libcrypto.so.3" "$top/LICENSE"
  rm -f mysql.tar.xz mysql.tar.xz.asc
}
mysql_dist "$MYSQL_VERSION" "$my_tar" "$my_sha" /out/mysql mysql mysqldump mysqlbinlog
mysql_dist "$MYSQL80_VERSION" "$my80_tar" "$my80_sha" /out/mysql-8.0 mysqldump

fetch "https://fastdl.mongodb.org/tools/db/${mongo_deb}" "$mongo_sha" /out/debs/mongodb-database-tools.deb
fetch "https://github.com/mydumper/mydumper/releases/download/v${MYDUMPER_VERSION}/mydumper_${MYDUMPER_VERSION}.bookworm_${TARGETARCH}.deb" \
  "$myd_sha" /out/debs/mydumper.deb
chmod -R go-w /out
EOF

FROM debian:bookworm-slim
ARG MYSQL_VERSION MYSQL80_VERSION MARIADB_VERSION PG_MAJOR PG_CLIENT_VERSION PG_LEGACY_MAJOR PG_LEGACY_CLIENT_VERSION MONGO_TOOLS_VERSION MYDUMPER_VERSION
# tini: PID 1 thu hoạch zombie (web sinh pipeline nền bằng `strand sync --background`, tiến trình mồ côi về PID 1).
# ca-certificates: kết nối TLS tới nguồn/đích. curl: HEALTHCHECK.
# MariaDB và MySQL không cài chung được bằng gói (mariadb-client xung đột với mysql-community-client):
# MariaDB cài bằng gói từ repo MariaDB (không cài mariadb-client-compat, nên không có tên mysql* của MariaDB);
# MySQL lấy binary từ tarball chính thức vào /opt/mysql, chỉ symlink mysql/mysqldump/mysqlbinlog.
# PostgreSQL: symlink thẳng tới binary của major đã ghim (bỏ qua pg_wrapper chọn phiên bản theo cluster).
# Giữ gọn: dpkg path-exclude các tool MongoDB khác mongodump/mongorestore (~120 MB) và các tiện ích MariaDB
# Strand không gọi (~55 MB); giữ mariadb, mariadb-dump, mariadb-binlog, mariadb-admin.
RUN --mount=type=bind,from=tools,source=/out,target=/mnt/tools <<'EOF'
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates
install -m 0644 /mnt/tools/keyrings/strand-pgdg.gpg /mnt/tools/keyrings/strand-mariadb.gpg /usr/share/keyrings/
cat > /etc/apt/sources.list.d/pgdg.sources <<SRC
Types: deb
URIs: https://apt.postgresql.org/pub/repos/apt
Suites: bookworm-pgdg
Components: main
Signed-By: /usr/share/keyrings/strand-pgdg.gpg
SRC
cat > /etc/apt/sources.list.d/mariadb.sources <<SRC
Types: deb
URIs: https://archive.mariadb.org/mariadb-${MARIADB_VERSION}/repo/debian
Suites: bookworm
Components: main
Signed-By: /usr/share/keyrings/strand-mariadb.gpg
SRC
cat > /etc/dpkg/dpkg.cfg.d/strand-slim <<CFG
path-exclude=/usr/bin/bsondump
path-exclude=/usr/bin/mongoexport
path-exclude=/usr/bin/mongofiles
path-exclude=/usr/bin/mongoimport
path-exclude=/usr/bin/mongostat
path-exclude=/usr/bin/mongotop
CFG
for t in check analyze optimize repair conv import migrate-config-file plugin show slap tzinfo-to-sql waitpid; do
  echo "path-exclude=/usr/bin/mariadb-$t"
done >> /etc/dpkg/dpkg.cfg.d/strand-slim
printf 'path-exclude=/usr/bin/%s\n' mariadbcheck perror replace resolve_stack_dump >> /etc/dpkg/dpkg.cfg.d/strand-slim
apt-get update
apt-get install -y --no-install-recommends \
  tini curl libncurses6 \
  "postgresql-client-${PG_MAJOR}${PG_CLIENT_VERSION:+=${PG_CLIENT_VERSION}}" \
  "postgresql-client-${PG_LEGACY_MAJOR}${PG_LEGACY_CLIENT_VERSION:+=${PG_LEGACY_CLIENT_VERSION}}" \
  "mariadb-client=1:${MARIADB_VERSION}+maria~deb12" \
  /mnt/tools/debs/mongodb-database-tools.deb /mnt/tools/debs/mydumper.deb
apt-get clean
rm -rf /var/lib/apt/lists/* /var/cache/apt/*
# Gói MongoDB đóng file với uid 1000 (admin): trả về root để không ai ngoài root sửa được binary.
chown root:root /usr/bin/mongodump /usr/bin/mongorestore
cp -a /mnt/tools/mysql /opt/mysql
cp -a /mnt/tools/mysql-8.0 /opt/mysql-8.0
chown -R root:root /opt/mysql /opt/mysql-8.0
ln -s /opt/mysql-8.0/bin/mysqldump /usr/local/bin/mysqldump-8.0
for t in mysql mysqldump mysqlbinlog; do
  test ! -e "/usr/bin/$t"          # không được có client MariaDB mang tên MySQL
  ln -s "/opt/mysql/bin/$t" "/usr/local/bin/$t"
done
for t in pg_dump pg_restore pg_basebackup psql; do
  ln -s "/usr/lib/postgresql/${PG_MAJOR}/bin/$t" "/usr/local/bin/$t"
done
for t in pg_dump pg_restore; do
  ln -s "/usr/lib/postgresql/${PG_LEGACY_MAJOR}/bin/$t" "/usr/local/bin/$t-${PG_LEGACY_MAJOR}"
done
# Mỗi công cụ phải chạy được (thiếu thư viện thì build hỏng ngay, kể cả arm64).
for t in mysql mysqldump mysqldump-8.0 mysqlbinlog mariadb mariadb-dump pg_dump pg_restore pg_basebackup psql "pg_dump-${PG_LEGACY_MAJOR}" "pg_restore-${PG_LEGACY_MAJOR}" mongodump mongorestore mydumper myloader; do
  p="$(command -v "$t")"
  if ldd "$(readlink -f "$p")" 2>&1 | grep 'not found'; then echo "$t: missing shared library" >&2; exit 1; fi
  "$t" --version >/dev/null
done
useradd --system --uid 10001 --user-group --create-home --home-dir /home/strand --shell /usr/sbin/nologin strand
mkdir -p /etc/strand /var/lib/strand-web
chown strand:strand /etc/strand /var/lib/strand-web
chmod 0750 /var/lib/strand-web
EOF
LABEL io.strand.tools.mysql="${MYSQL_VERSION}" \
      io.strand.tools.mysql80-mysqldump="${MYSQL80_VERSION}" \
      io.strand.tools.mariadb="${MARIADB_VERSION}" \
      io.strand.tools.postgresql="${PG_CLIENT_VERSION:-${PG_MAJOR}}" \
      io.strand.tools.postgresql-legacy="${PG_LEGACY_CLIENT_VERSION:-${PG_LEGACY_MAJOR}}" \
      io.strand.tools.mongodb-database-tools="${MONGO_TOOLS_VERSION}" \
      io.strand.tools.mydumper="${MYDUMPER_VERSION}"

COPY --from=build /out/strand /usr/local/bin/strand
COPY deploy/compose/entrypoint.sh /usr/local/bin/strand-entrypoint
COPY deploy/compose/healthcheck.sh /usr/local/bin/strand-healthcheck
RUN chmod 0755 /usr/local/bin/strand-entrypoint /usr/local/bin/strand-healthcheck

USER strand
WORKDIR /home/strand
ENV STRAND_WEB_STATE_DIR=/var/lib/strand-web \
    STRAND_CONFIG_DIR=/etc/strand
VOLUME ["/etc/strand", "/var/lib/strand-web"]
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 CMD ["/usr/local/bin/strand-healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/strand-entrypoint"]
CMD ["serve"]

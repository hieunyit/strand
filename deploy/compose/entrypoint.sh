#!/bin/sh
# Entrypoint của image Strand: dựng web.yaml từ biến môi trường, tạo admin lần đầu, rồi chạy `strand web`.
# Mọi biến được liệt kê và giải thích trong .env.example.
set -eu

# Lệnh khác `serve` (ví dụ `docker compose run --rm web strand version`) thì chạy thẳng.
if [ "${1:-serve}" != "serve" ]; then
  exec "$@"
fi

: "${STRAND_WEB_DATABASE_DSN:?STRAND_WEB_DATABASE_DSN is required (postgres://user:pass@host:5432/db), see .env.example}"
# Mật khẩu mẫu của .env.example còn nguyên: từ chối (DB metadata giữ hash admin, .env mã hóa, audit).
case "$STRAND_WEB_DATABASE_DSN" in
  *change-me*) echo "POSTGRES_PASSWORD is still the placeholder from .env.example: set a real one (openssl rand -hex 24) in .env" >&2; exit 2 ;;
esac
: "${STRAND_WEB_SECRET_KEY:?STRAND_WEB_SECRET_KEY is required (pipeline secrets are encrypted in the database): set it in .env, e.g. openssl rand -hex 32}"
LISTEN="${STRAND_WEB_LISTEN:-0.0.0.0:8080}"
STATE_DIR="${STRAND_WEB_STATE_DIR:-/var/lib/strand-web}"
CONFIG_DIR="${STRAND_CONFIG_DIR:-/etc/strand}"
GENERATED=0
if [ -z "${STRAND_WEB_CONFIG:-}" ]; then
  # web.yaml sinh mới mỗi lần khởi động từ env (không chứa bí mật: DSN và khóa đi qua env).
  STRAND_WEB_CONFIG="$HOME/web.yaml"
  GENERATED=1
fi

truthy() { case "$(printf '%s' "${1:-}" | tr 'A-Z' 'a-z')" in 1|true|yes|on) return 0 ;; *) return 1 ;; esac; }

if [ "$GENERATED" = 1 ]; then
  rm -f "$STRAND_WEB_CONFIG"
  set -- --config "$STRAND_WEB_CONFIG" --listen "$LISTEN" --state-dir "$STATE_DIR" --scan "$CONFIG_DIR/*.yaml"
  if [ -n "${STRAND_WEB_TLS_CERT:-}" ] || [ -n "${STRAND_WEB_TLS_KEY:-}" ]; then
    set -- "$@" --tls-cert "${STRAND_WEB_TLS_CERT:-}" --tls-key "${STRAND_WEB_TLS_KEY:-}"
  elif truthy "${STRAND_WEB_ALLOW_PLAIN_HTTP:-true}"; then
    set -- "$@" --allow-plain-http
  fi
  truthy "${STRAND_WEB_SECURE_COOKIES:-false}" && set -- "$@" --secure-cookies
  truthy "${STRAND_WEB_TRUST_FORWARDED_FOR:-false}" && set -- "$@" --trust-forwarded-for
else
  set -- --config "$STRAND_WEB_CONFIG"
fi

# Mật khẩu admin chỉ dùng khi database CHƯA có admin (--if-no-admin): đổi biến sau này không đổi mật khẩu
# (dùng `docker compose exec web strand web passwd`). Không đặt thì sinh ngẫu nhiên và in MỘT lần ra log.
ADMIN_USER="${STRAND_WEB_ADMIN_USER:-admin}"
if [ -n "${STRAND_WEB_ADMIN_PASSWORD_FILE:-}" ]; then
  [ -r "$STRAND_WEB_ADMIN_PASSWORD_FILE" ] || { echo "STRAND_WEB_ADMIN_PASSWORD_FILE is not readable" >&2; exit 2; }
  STRAND_WEB_ADMIN_PASSWORD="$(cat "$STRAND_WEB_ADMIN_PASSWORD_FILE")"
fi
if [ -n "${STRAND_WEB_ADMIN_PASSWORD:-}" ]; then
  printf '%s\n' "$STRAND_WEB_ADMIN_PASSWORD" | strand web init "$@" --admin-user "$ADMIN_USER" --password-stdin --if-no-admin --quiet
else
  strand web init "$@" --admin-user "$ADMIN_USER" --generate-password --if-no-admin --quiet
fi
unset STRAND_WEB_ADMIN_PASSWORD

if [ "$GENERATED" = 1 ]; then
  {
    echo "config_dir: \"$CONFIG_DIR\""
    # Container dừng (docker stop) thì dừng êm pipeline nền; lần chạy sau tự chạy lại theo trạng thái mong muốn.
    echo "shutdown_stop_pipelines: true"
  } >> "$STRAND_WEB_CONFIG"
fi

exec strand web --config "$STRAND_WEB_CONFIG"

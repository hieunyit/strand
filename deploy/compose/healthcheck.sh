#!/bin/sh
# HEALTHCHECK của image: GET /healthz (200 khi web và database đều ổn, 503 khi mất DB).
set -eu
PORT="${STRAND_WEB_LISTEN:-0.0.0.0:8080}"
PORT="${PORT##*:}"
SCHEME=http
[ -n "${STRAND_WEB_TLS_CERT:-}" ] && SCHEME=https
exec curl -fsSk --max-time 4 "$SCHEME://127.0.0.1:$PORT/healthz" -o /dev/null

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
TMP_DIR="$(mktemp -d /tmp/strand-browser-run-XXXXXX)"

cleanup() {
  echo "Cleaning up..."
  if [ -n "${FIXTURE_PID:-}" ] && kill -0 "$FIXTURE_PID" 2>/dev/null; then
    kill "$FIXTURE_PID" 2>/dev/null || true
  fi
  docker stop strand-chrome-fixture 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "=== 1. Starting Isolated Headless Chromium (Docker --net=host on 9222) ==="
docker stop strand-chrome-fixture 2>/dev/null || true
docker run -d --name strand-chrome-fixture --rm --net=host \
  zenika/alpine-chrome:latest \
  --no-sandbox \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9222

# Wait for CDP to respond
for i in {1..30}; do
  if curl -s http://127.0.0.1:9222/json/version >/dev/null 2>&1; then
    echo "Chromium CDP ready on port 9222"
    break
  fi
  sleep 0.5
done

echo "=== 2. Building and Starting Web Server Fixture ==="
/usr/local/go/bin/go build -o "${TMP_DIR}/fixture-bin" "${SCRIPT_DIR}/server_fixture.go"
INFO_JSON="${TMP_DIR}/fixture-info.json"
"${TMP_DIR}/fixture-bin" -info-json "$INFO_JSON" > "${TMP_DIR}/fixture.log" 2>&1 &
FIXTURE_PID=$!

for i in {1..30}; do
  if [ -f "$INFO_JSON" ] && [ -s "$INFO_JSON" ]; then
    echo "Fixture info ready:"
    cat "$INFO_JSON"
    break
  fi
  sleep 0.5
done

echo "=== 3. Executing Playwright Browser Test Suite ==="
RESULTS_JSON="${TMP_DIR}/suite-results.json"
node "${SCRIPT_DIR}/suite.mjs" "$INFO_JSON" "$RESULTS_JSON"

echo "=== 4. Test Suite Output ==="
cat "$RESULTS_JSON"

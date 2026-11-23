#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/strand-browser-run-XXXXXX")"

cleanup() {
  echo "Cleaning up..."
  if [ -n "${FIXTURE_PID:-}" ] && kill -0 "$FIXTURE_PID" 2>/dev/null; then
    kill "$FIXTURE_PID" 2>/dev/null || true
    wait "$FIXTURE_PID" 2>/dev/null || true
  fi
  if [ -n "${CHROME_ID:-}" ]; then
    docker stop "$CHROME_ID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "=== 1. Starting Owned Headless Chromium (Docker --net=host, allocated CDP port) ==="
CHROME_CREATED="$(docker run -d --rm --net=host \
  --label "strand.diagnostic.run=$(basename "$TMP_DIR")" \
  zenika/alpine-chrome:latest \
  --no-sandbox \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=0)"
if [[ ! "$CHROME_CREATED" =~ ^[a-f0-9]{64}$ ]]; then
  echo "docker returned an invalid container ID" >&2
  exit 1
fi
CHROME_ID="$CHROME_CREATED"
unset STRAND_BROWSER_CDP_URL

# Read this owned browser's OS-allocated port, never a fixed shared endpoint.
for i in {1..60}; do
  CDP_URL="$(docker logs "$CHROME_ID" 2>&1 | sed -n 's/.*DevTools listening on ws:\/\/\(127\.0\.0\.1:[0-9]*\)\/.*/http:\/\/\1/p' | head -n 1)"
  if [ -n "$CDP_URL" ] && curl -fsS "$CDP_URL/json/version" >/dev/null; then
    export STRAND_BROWSER_CDP_URL="$CDP_URL"
    break
  fi
  sleep 0.5
done
: "${STRAND_BROWSER_CDP_URL:?Owned Chromium CDP endpoint did not become ready}"

echo "=== 2. Building and Starting Web Server Fixture ==="
go build -o "${TMP_DIR}/fixture-bin" "${SCRIPT_DIR}/server_fixture.go"
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

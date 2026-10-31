#!/usr/bin/env bash
# tla/run.sh: Chạy TLC model checker cho 4 module TLA+ theo S02 §12
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Tìm tla2tools.jar
JAR="${TLA2TOOLS_JAR:-}"
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
  if [ -f "/usr/local/lib/tla2tools.jar" ]; then
    JAR="/usr/local/lib/tla2tools.jar"
  elif [ -f "$HOME/.local/lib/tla2tools.jar" ]; then
    JAR="$HOME/.local/lib/tla2tools.jar"
  else
    echo "Tải tla2tools.jar về $HOME/.local/lib/..."
    mkdir -p "$HOME/.local/lib"
    curl -sL -o "$HOME/.local/lib/tla2tools.jar" "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"
    JAR="$HOME/.local/lib/tla2tools.jar"
  fi
fi

run_tlc() {
  local tla_file="$1"
  local cfg_file="$2"
  local metadir
  metadir="$(mktemp -d /tmp/tlc_meta_XXXXXX)"
  local res=0
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -metadir "$metadir" -deadlock -config "$cfg_file" "$tla_file" || res=$?
  rm -rf "$metadir"
  return "$res"
}

cleanup_traces() {
  rm -f "$SCRIPT_DIR"/*_TTrace_* "$SCRIPT_DIR"/*.bin
  rm -rf "$SCRIPT_DIR"/states "$ROOT_DIR"/states
}

trap cleanup_traces EXIT

MODE="${1:-all}"

run_check() {
  echo "=== [TLC CHECK] Chạy 4 module chuẩn (AC2) ==="
  local MODULES=("Scheduler" "Checkpoint" "RangeState" "Handover")
  local FAILED=0

  for mod in "${MODULES[@]}"; do
    echo "--- Kiểm tra module: $mod ---"
    local output ret
    set +e
    output=$(run_tlc "$SCRIPT_DIR/$mod.tla" "$SCRIPT_DIR/$mod.cfg" 2>&1)
    ret=$?
    set -e
    if [ "$ret" -eq 0 ] && echo "$output" | grep -q "Model checking completed. No error has been found."; then
      echo "  [PASS] $mod: Không tìm thấy vi phạm bất biến"
    else
      echo "  [FAIL] $mod: TLC phát hiện lỗi hoặc crash"
      echo "$output"
      FAILED=1
    fi
  done

  if [ "$FAILED" -ne 0 ]; then
    echo "LỖI: Có module chuẩn bị vi phạm bất biến!"
    return 1
  fi
  echo "=== TẤT CẢ 4 MODULE CHUẨN ĐÃ PASS ==="
  return 0
}

run_mutants() {
  echo "=== [TLC MUTANTS] Chạy 4 module biến thể cài lỗi (AC3) ==="
  local MUTANTS=("Scheduler_err" "Checkpoint_err" "RangeState_err" "Handover_err")
  local FAILED=0

  for mod in "${MUTANTS[@]}"; do
    echo "--- Kiểm tra mutant: $mod ---"
    local output ret
    set +e
    output=$(run_tlc "$SCRIPT_DIR/$mod.tla" "$SCRIPT_DIR/$mod.cfg" 2>&1)
    ret=$?
    set -e

    if [ "$ret" -ne 0 ] && echo "$output" | grep -q -E "(Error: Invariant .* is violated|Error: The behavior up to this point is)"; then
      local inv
      inv=$(echo "$output" | grep -o -E "Invariant [A-Za-z0-9_]+ is violated" | head -1 || echo "violation found")
      echo "  [PASS] $mod: TLC đã tìm thấy phản ví dụ ($inv)"
    else
      echo "  [FAIL] $mod: TLC KHÔNG phát hiện được lỗi trên module cài lỗi!"
      echo "$output"
      FAILED=1
    fi
  done

  if [ "$FAILED" -ne 0 ]; then
    echo "LỖI: Có biến thể cài lỗi không bị TLC phát hiện!"
    return 1
  fi
  echo "=== TẤT CẢ 4 BIẾN THỂ CÀI LỖI ĐÃ BỊ TLC PHÁT HIỆN ==="
  return 0
}

case "$MODE" in
  check)
    run_check
    ;;
  mutants)
    run_mutants
    ;;
  all)
    run_check
    run_mutants
    ;;
  *)
    echo "Usage: $0 [check|mutants|all]"
    exit 1
    ;;
esac

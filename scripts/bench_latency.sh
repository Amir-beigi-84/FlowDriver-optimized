#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-local}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/flowdriver-bench}"
TARGET_DIR="${TARGET_DIR:-/tmp/flow-target}"
TARGET_PORT="${TARGET_PORT:-18082}"
LISTEN_ADDR="${LISTEN_ADDR:-127.0.0.1:1080}"
METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC:-5}"
CONFIG="${CONFIG:-config.json}"
GC_FILE="${GC_FILE:-client-me.json}"
CURL_FORMAT="${CURL_FORMAT:-$ROOT_DIR/scripts/curl-format.txt}"
OUT_FILE="${OUT_FILE:-$WORK_DIR/flowdriver-bench.out}"
RESULT_FILE="${RESULT_FILE:-$WORK_DIR/curl-result.txt}"
SERVER_LOG="$WORK_DIR/server.log"
CLIENT_LOG="$WORK_DIR/client.log"

SERVER_PID=""
CLIENT_PID=""
TARGET_PID=""

usage() {
  cat <<USAGE
Usage: $0 local|google

Environment:
  LISTEN_ADDR              SOCKS listen address, default 127.0.0.1:1080
  TARGET_URL               URL to curl through FlowDriver
  CONFIG                   Google config path, default config.json
  GC_FILE                  Google OAuth client JSON path, default client-me.json
  METRICS_INTERVAL_SEC     Metrics log interval injected into temp config, default 5
  WORK_DIR                 Temp output dir, default /tmp/flowdriver-bench

Examples:
  $0 local
  CONFIG=config-me.json GC_FILE=client-me.json TARGET_URL=http://example.com/ $0 google
USAGE
}

cleanup() {
  if [[ -n "$CLIENT_PID" ]]; then kill "$CLIENT_PID" 2>/dev/null || true; fi
  if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2>/dev/null || true; fi
  if [[ -n "$TARGET_PID" ]]; then kill "$TARGET_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

require_running() {
  local name="$1"
  local pid="$2"
  local log_file="$3"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "ERROR: $name exited before benchmark" >&2
    if [[ -f "$log_file" ]]; then
      echo "=== $name log ===" >&2
      cat "$log_file" >&2
    fi
    exit 1
  fi
}

if [[ "$MODE" != "local" && "$MODE" != "google" ]]; then
  usage
  exit 2
fi

mkdir -p "$WORK_DIR" "$WORK_DIR/bin"

echo "Building FlowDriver binaries into $WORK_DIR/bin"
go build -o "$WORK_DIR/bin/client" "$ROOT_DIR/cmd/client"
go build -o "$WORK_DIR/bin/server" "$ROOT_DIR/cmd/server"

make_config() {
  local src="$1"
  local dst="$2"
  python3 - "$src" "$dst" "$LISTEN_ADDR" "$METRICS_INTERVAL_SEC" <<'PY'
import json
import sys

src, dst, listen_addr, metrics_interval = sys.argv[1:]
with open(src, "r", encoding="utf-8") as f:
    cfg = json.load(f)
cfg["listen_addr"] = listen_addr
cfg["metrics_log_interval_sec"] = int(metrics_interval)
with open(dst, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2)
    f.write("\n")
PY
}

if [[ "$MODE" == "local" ]]; then
  mkdir -p "$TARGET_DIR"
  printf 'flowdriver bench target\n' > "$TARGET_DIR/index.html"
  python3 -m http.server "$TARGET_PORT" --bind 127.0.0.1 --directory "$TARGET_DIR" > "$WORK_DIR/target.log" 2>&1 &
  TARGET_PID=$!

  CONFIG_PATH="$WORK_DIR/local-config.json"
  cat > "$CONFIG_PATH" <<JSON
{
  "listen_addr": "$LISTEN_ADDR",
  "storage_type": "local",
  "local_dir": "$WORK_DIR/storage",
  "refresh_rate_ms": 200,
  "flush_rate_ms": 300,
  "metrics_log_interval_sec": $METRICS_INTERVAL_SEC
}
JSON
  mkdir -p "$WORK_DIR/storage"
  TARGET_URL="${TARGET_URL:-http://127.0.0.1:$TARGET_PORT/}"
else
  if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: CONFIG not found: $CONFIG" >&2
    exit 1
  fi
  if [[ ! -f "$GC_FILE" ]]; then
    echo "ERROR: GC_FILE not found: $GC_FILE" >&2
    exit 1
  fi
  CONFIG_PATH="$WORK_DIR/google-config.json"
  make_config "$CONFIG" "$CONFIG_PATH"
  TARGET_URL="${TARGET_URL:-http://example.com/}"
fi

echo "Starting server"
if [[ "$MODE" == "google" ]]; then
  "$WORK_DIR/bin/server" -c "$CONFIG_PATH" -gc "$GC_FILE" > "$SERVER_LOG" 2>&1 &
else
  "$WORK_DIR/bin/server" -c "$CONFIG_PATH" > "$SERVER_LOG" 2>&1 &
fi
SERVER_PID=$!

echo "Starting client"
if [[ "$MODE" == "google" ]]; then
  "$WORK_DIR/bin/client" -c "$CONFIG_PATH" -gc "$GC_FILE" > "$CLIENT_LOG" 2>&1 &
else
  "$WORK_DIR/bin/client" -c "$CONFIG_PATH" > "$CLIENT_LOG" 2>&1 &
fi
CLIENT_PID=$!

sleep "${STARTUP_SLEEP_SEC:-3}"

if [[ -n "$TARGET_PID" ]]; then
  require_running "target" "$TARGET_PID" "$WORK_DIR/target.log"
fi
require_running "server" "$SERVER_PID" "$SERVER_LOG"
require_running "client" "$CLIENT_PID" "$CLIENT_LOG"

echo "Benchmark curl: $TARGET_URL"
set +e
curl --noproxy "" -w "@$CURL_FORMAT" -o "$OUT_FILE" -s --socks5-hostname "$LISTEN_ADDR" "$TARGET_URL" > "$RESULT_FILE"
CURL_STATUS=$?
set -e

sleep "$METRICS_INTERVAL_SEC"

echo "=== curl result ==="
cat "$RESULT_FILE"
echo "curl_exit_status: $CURL_STATUS"

echo "=== latest metrics ==="
grep '\[METRICS\]' "$CLIENT_LOG" "$SERVER_LOG" | tail -n 16 || true

echo "=== output ==="
echo "body_file: $OUT_FILE"
echo "client_log: $CLIENT_LOG"
echo "server_log: $SERVER_LOG"

exit "$CURL_STATUS"

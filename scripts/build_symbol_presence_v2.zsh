#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/deploy/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/deploy/.env.example"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$EXAMPLE_ENV_FILE" "$ENV_FILE"
fi

read_env() {
  local key="$1"
  local default_value="$2"
  local value
  value="$(grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2-)"
  if [[ -z "$value" ]]; then
    echo "$default_value"
  else
    echo "$value"
  fi
}

CH_PORT="$(read_env CLICKHOUSE_HTTP_PORT 18123)"
CH_DB="$(read_env CLICKHOUSE_DB ios_headers)"
CH_USER="$(read_env CLICKHOUSE_USER default)"
CH_PASS="$(read_env CLICKHOUSE_PASSWORD '')"

PY_SCRIPT="$ROOT_DIR/scripts/build_symbol_presence_v2.py"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "missing script: $PY_SCRIPT"
  exit 1
fi

"$PYTHON_BIN" "$PY_SCRIPT" \
  --clickhouse-url "http://127.0.0.1:${CH_PORT}" \
  --clickhouse-db "$CH_DB" \
  --clickhouse-user "$CH_USER" \
  --clickhouse-password "$CH_PASS" \
  "$@"

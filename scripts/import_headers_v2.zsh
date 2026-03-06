#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/.env.example"

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

CH_HOST="$(read_env CLICKHOUSE_HOST 127.0.0.1)"
CH_PORT="$(read_env CLICKHOUSE_NATIVE_PORT 19000)"
CH_DB="$(read_env CLICKHOUSE_DB ios_headers)"
CH_USER="$(read_env CLICKHOUSE_USER default)"
CH_PASS="$(read_env CLICKHOUSE_PASSWORD '')"
MINIO_PORT="$(read_env MINIO_API_PORT 19001)"
MINIO_USER="$(read_env MINIO_ROOT_USER minioadmin)"
MINIO_PASS="$(read_env MINIO_ROOT_PASSWORD minioadmin)"
MINIO_BUCKET="$(read_env MINIO_BUCKET ios-headers)"
PROGRESS_EVERY="$(read_env PROGRESS_EVERY 1000)"

PY_SCRIPT="$ROOT_DIR/scripts/import_headers_v2.py"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "missing script: $PY_SCRIPT"
  exit 1
fi

"$PYTHON_BIN" "$PY_SCRIPT" \
  --clickhouse-host "$CH_HOST" \
  --clickhouse-port "$CH_PORT" \
  --clickhouse-db "$CH_DB" \
  --clickhouse-user "$CH_USER" \
  --clickhouse-password "$CH_PASS" \
  --minio-endpoint "127.0.0.1:${MINIO_PORT}" \
  --minio-access-key "$MINIO_USER" \
  --minio-secret-key "$MINIO_PASS" \
  --minio-bucket "$MINIO_BUCKET" \
  --progress-every "$PROGRESS_EVERY" \
  "$@"

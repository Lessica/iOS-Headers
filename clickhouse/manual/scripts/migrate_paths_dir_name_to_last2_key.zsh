#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/.env.example"
SQL_FILE="$ROOT_DIR/clickhouse/manual/004_migrate_paths_dir_name_to_last2_key.sql"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$EXAMPLE_ENV_FILE" "$ENV_FILE"
fi

read_env() {
  local key="$1"
  local default_value="$2"
  local value
  value="$(grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- || true)"
  if [[ -z "$value" ]]; then
    echo "$default_value"
  else
    echo "$value"
  fi
}

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

compose_clickhouse_ready() {
  command -v docker >/dev/null 2>&1 || return 1
  docker info >/dev/null 2>&1 || return 1
  compose ps -q clickhouse >/dev/null 2>&1 || return 1

  local container_id
  container_id="$(compose ps -q clickhouse | head -n 1)"
  [[ -n "$container_id" ]] || return 1
}

run_with_local_client() {
  command -v clickhouse-client >/dev/null 2>&1 || {
    echo "clickhouse-client is required when docker compose clickhouse is unavailable"
    return 1
  }

  local ch_host ch_port ch_db ch_user ch_pass
  ch_host="$(read_env CLICKHOUSE_HOST 127.0.0.1)"
  ch_port="$(read_env CLICKHOUSE_NATIVE_PORT 19000)"
  ch_db="$(read_env CLICKHOUSE_DB ios_headers)"
  ch_user="$(read_env CLICKHOUSE_USER default)"
  ch_pass="$(read_env CLICKHOUSE_PASSWORD '')"

  if [[ -n "$ch_pass" ]]; then
    clickhouse-client \
      --host "$ch_host" \
      --port "$ch_port" \
      --database "$ch_db" \
      --user "$ch_user" \
      --password "$ch_pass" \
      < "$SQL_FILE"
  else
    clickhouse-client \
      --host "$ch_host" \
      --port "$ch_port" \
      --database "$ch_db" \
      --user "$ch_user" \
      < "$SQL_FILE"
  fi
}

if [[ ! -f "$SQL_FILE" ]]; then
  echo "missing sql file: $SQL_FILE"
  exit 1
fi

if compose_clickhouse_ready; then
  compose exec -T clickhouse clickhouse-client < "$SQL_FILE"
  echo "paths dir_name migration completed via docker compose"
  exit 0
fi

echo "docker compose clickhouse is unavailable, falling back to local clickhouse-client"
run_with_local_client

echo "paths dir_name migration completed"

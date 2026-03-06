#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/.env.example"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$EXAMPLE_ENV_FILE" "$ENV_FILE"
fi

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

require_tools() {
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required"
    exit 1
  }
  docker info >/dev/null 2>&1 || {
    echo "docker daemon is not running"
    exit 1
  }
}

wait_http_ok() {
  local name="$1"
  local url="$2"
  local max_try="${3:-60}"
  local i=1
  while (( i <= max_try )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is healthy"
      return 0
    fi
    sleep 1
    ((i++))
  done
  echo "$name health check failed: $url"
  return 1
}

get_env_value() {
  local key="$1"
  local default_value="$2"
  local value
  value="$(grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d'=' -f2- || true)"
  if [[ -z "$value" ]]; then
    value="$default_value"
  fi
  echo "$value"
}

ensure_minio_bucket() {
  local project_name network_name minio_root_user minio_root_password minio_bucket
  project_name="$(get_env_value "COMPOSE_PROJECT_NAME" "ios_headers")"
  network_name="${project_name}_net"
  minio_root_user="$(get_env_value "MINIO_ROOT_USER" "minioadmin")"
  minio_root_password="$(get_env_value "MINIO_ROOT_PASSWORD" "minioadmin")"
  minio_bucket="$(get_env_value "MINIO_BUCKET" "ios-headers")"

  docker run --rm --network "$network_name" --entrypoint /bin/sh minio/mc:latest -c \
    "mc alias set local http://minio:9000 '$minio_root_user' '$minio_root_password' && \
    (mc mb -p local/'$minio_bucket' || true) && \
    mc anonymous set download local/'$minio_bucket' && \
    echo 'minio bucket initialized'"
}

up() {
  require_tools
  compose up -d

  local minio_port
  minio_port="$(get_env_value "MINIO_API_PORT" "19001")"

  local i=1
  while (( i <= 60 )); do
    if compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
      echo "clickhouse is healthy"
      break
    fi
    sleep 1
    ((i++))
  done
  if (( i > 60 )); then
    echo "clickhouse health check failed"
    return 1
  fi

  wait_http_ok "minio" "http://127.0.0.1:${minio_port}/minio/health/live"

  compose exec -T redis redis-cli ping | grep -q PONG
  echo "redis is healthy"
  ensure_minio_bucket

  echo "stack is up"
  compose ps
}

down() {
  require_tools
  compose down
}

restart() {
  down
  up
}

status() {
  require_tools
  compose ps
}

logs() {
  require_tools
  local service="${1:-}"
  if [[ -n "$service" ]]; then
    compose logs -f "$service"
  else
    compose logs -f
  fi
}

check() {
  require_tools
  local minio_port
  minio_port="$(get_env_value "MINIO_API_PORT" "19001")"

  compose exec -T clickhouse clickhouse-client --query "SELECT 1" | grep -q '^1$'
  echo "clickhouse ping ok"

  curl -fsS "http://127.0.0.1:${minio_port}/minio/health/live" >/dev/null
  echo "minio health ok"

  compose exec -T redis redis-cli ping | grep -q PONG
  echo "redis ping ok"

  compose exec -T clickhouse clickhouse-client --query "SHOW DATABASES" | grep -q '^ios_headers$'
  echo "ios_headers database exists"
}

init_db() {
  require_tools
  local sql_file
  for sql_file in "$ROOT_DIR"/clickhouse/init/*.sql; do
    compose exec -T clickhouse clickhouse-client < "$sql_file"
  done
  echo "schema initialized"
}

init_minio() {
  require_tools
  ensure_minio_bucket
}

rebuild_web() {
  require_tools
  compose up -d --build web nginx
  echo "web stack rebuilt"
  compose ps web nginx
}

clear_cache() {
  require_tools
  compose exec -T redis redis-cli FLUSHDB >/dev/null
  echo "redis cache cleared"
}

usage() {
  echo "usage: $0 {up|down|restart|status|logs [service]|check|init-db|init-minio|rebuild-web|clear-cache}"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  restart) restart ;;
  status) status ;;
  logs) shift; logs "${1:-}" ;;
  check) check ;;
  init-db) init_db ;;
  init-minio) init_minio ;;
  rebuild-web) rebuild_web ;;
  clear-cache) clear_cache ;;
  *) usage; exit 1 ;;
esac

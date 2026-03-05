#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deploy/docker-compose.yml"
ENV_FILE="$ROOT_DIR/deploy/.env"
EXAMPLE_ENV_FILE="$ROOT_DIR/deploy/.env.example"

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

up() {
  require_tools
  compose up -d

  local ch_port minio_port
  ch_port="$(grep -E '^CLICKHOUSE_HTTP_PORT=' "$ENV_FILE" | cut -d'=' -f2)"
  minio_port="$(grep -E '^MINIO_API_PORT=' "$ENV_FILE" | cut -d'=' -f2)"
  wait_http_ok "clickhouse" "http://127.0.0.1:${ch_port}/ping"
  wait_http_ok "minio" "http://127.0.0.1:${minio_port}/minio/health/live"

  compose exec -T redis redis-cli ping | grep -q PONG
  echo "redis is healthy"

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
  local ch_port minio_port
  ch_port="$(grep -E '^CLICKHOUSE_HTTP_PORT=' "$ENV_FILE" | cut -d'=' -f2)"
  minio_port="$(grep -E '^MINIO_API_PORT=' "$ENV_FILE" | cut -d'=' -f2)"

  curl -fsS "http://127.0.0.1:${ch_port}/ping" | grep -q Ok.
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
  compose exec -T clickhouse clickhouse-client < "$ROOT_DIR/deploy/clickhouse/init/001_schema.sql"
  echo "schema initialized"
}

usage() {
  echo "usage: $0 {up|down|restart|status|logs [service]|check|init-db}"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  restart) restart ;;
  status) status ;;
  logs) shift; logs "${1:-}" ;;
  check) check ;;
  init-db) init_db ;;
  *) usage; exit 1 ;;
esac

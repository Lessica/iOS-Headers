#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

REMOTE_HOST="${REMOTE_HOST:-drm-north}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/ios-headers-frp}"
FRPS_IMAGE="${FRPS_IMAGE:-ghcr.io/fatedier/frps:v0.61.2}"
FRP_BIND_PORT="${FRP_SERVER_PORT:-7000}"
FRP_REMOTE_PORT="${FRP_REMOTE_PORT:-18080}"
FRP_DASHBOARD_PORT="${FRP_DASHBOARD_PORT:-7500}"
FRP_DASHBOARD_USER="${FRP_DASHBOARD_USER:-admin}"
FRP_DASHBOARD_PASSWORD="${FRP_DASHBOARD_PASSWORD:-change-this-password}"
FRP_ENABLE_DASHBOARD="${FRP_ENABLE_DASHBOARD:-true}"
OPEN_UFW_PORTS="${OPEN_UFW_PORTS:-true}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "$1 is required"
    exit 1
  }
}

require_env() {
  local key="$1"
  local value="${(P)key:-}"
  if [[ -z "$value" ]]; then
    echo "$key is required (set it in .env or environment)"
    exit 1
  fi
}

render_frps_toml() {
  require_env "FRP_TOKEN"
  if [[ "$FRP_ENABLE_DASHBOARD" == "true" ]]; then
    cat <<EOF
bindPort = ${FRP_BIND_PORT}

auth.method = "token"
auth.token = "${FRP_TOKEN}"

webServer.addr = "0.0.0.0"
webServer.port = ${FRP_DASHBOARD_PORT}
webServer.user = "${FRP_DASHBOARD_USER}"
webServer.password = "${FRP_DASHBOARD_PASSWORD}"
EOF
  else
    cat <<EOF
bindPort = ${FRP_BIND_PORT}

auth.method = "token"
auth.token = "${FRP_TOKEN}"
EOF
  fi
}

deploy() {
  require_cmd ssh
  require_env "FRP_TOKEN"

  local rendered
  rendered="$(render_frps_toml)"

  ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_APP_DIR'"
  ssh "$REMOTE_HOST" "cat > '$REMOTE_APP_DIR/frps.toml'" <<<"$rendered"

  ssh "$REMOTE_HOST" "docker pull '$FRPS_IMAGE'"
  ssh "$REMOTE_HOST" "docker rm -f frps >/dev/null 2>&1 || true"
  ssh "$REMOTE_HOST" "docker run -d --name frps --restart unless-stopped --network host -v '$REMOTE_APP_DIR/frps.toml:/etc/frp/frps.toml:ro' '$FRPS_IMAGE' -c /etc/frp/frps.toml"

  if [[ "$OPEN_UFW_PORTS" == "true" ]]; then
    ssh "$REMOTE_HOST" "if command -v ufw >/dev/null 2>&1; then if ufw status | grep -q 'Status: active'; then ufw allow ${FRP_BIND_PORT}/tcp; ufw allow ${FRP_REMOTE_PORT}/tcp; if [[ '${FRP_ENABLE_DASHBOARD}' == 'true' ]]; then ufw allow ${FRP_DASHBOARD_PORT}/tcp; fi; fi; fi"
  fi

  status
}

status() {
  ssh "$REMOTE_HOST" "docker ps --filter name=^/frps$ --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'"
}

logs() {
  ssh "$REMOTE_HOST" "docker logs --tail 200 -f frps"
}

restart() {
  ssh "$REMOTE_HOST" "docker restart frps"
  status
}

remove() {
  ssh "$REMOTE_HOST" "docker rm -f frps >/dev/null 2>&1 || true"
}

render() {
  render_frps_toml
}

usage() {
  echo "usage: $0 {deploy|status|logs|restart|remove|render}"
}

case "${1:-}" in
  deploy) deploy ;;
  status) status ;;
  logs) logs ;;
  restart) restart ;;
  remove) remove ;;
  render) render ;;
  *) usage; exit 1 ;;
esac

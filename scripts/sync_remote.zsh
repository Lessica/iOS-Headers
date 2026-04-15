#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_NAME="$(basename "$0")"

REMOTE_HOST="${REMOTE_HOST:-drm-north}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/ios-headers}"
REMOTE_DATA_DIR="${REMOTE_DATA_DIR:-/opt/ios-headers-data}"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"

DRY_RUN="false"
VERBOSE="false"

run_rsync() {
  local rc
  local err_file
  local had_errexit="false"
  err_file="$(mktemp)"

  if [[ "$-" == *e* ]]; then
    had_errexit="true"
  fi

  set +e
  rsync "$@" 2>"$err_file"
  rc=$?
  if [[ "$had_errexit" == "true" ]]; then
    set -e
  fi

  if [[ "$DRY_RUN" == "true" && $rc -eq 13 && ! -t 1 ]]; then
    if grep -q 'code 13' "$err_file"; then
      rm -f "$err_file"
      echo "[warn] rsync output was truncated by a downstream pipe (e.g. head); dry-run is still active, no data was transferred."
      return 0
    fi
  fi

  if [[ -s "$err_file" ]]; then
    cat "$err_file" >&2
  fi
  rm -f "$err_file"

  return $rc
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "$1 is required"
    exit 1
  }
}

is_modern_rsync() {
  local vline
  vline="$(rsync --version 2>/dev/null | head -n 1 || true)"
  [[ "$vline" == rsync* ]]
}

supports_from0() {
  rsync --help 2>&1 | grep -q -- '--from0'
}

supports_delete_missing_args() {
  rsync --help 2>&1 | grep -q -- '--delete-missing-args'
}

common_rsync_args() {
  local -a args
  args=( -az --human-readable --usermap='*:root' --groupmap='*:root' )

  if [[ "$VERBOSE" == "true" ]]; then
    args+=( -v )
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    args+=( --dry-run --itemize-changes )
  fi
  printf '%s\n' "${args[@]}"
}

ensure_remote_dirs() {
  ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_APP_DIR' '$REMOTE_DATA_DIR/clickhouse' '$REMOTE_DATA_DIR/minio' '$REMOTE_DATA_DIR/redis'"
}

normalize_remote_path() {
  local p="$1"
  p="${p%/}"
  [[ -n "$p" ]] || p="/"
  printf '%s\n' "$p"
}

validate_remote_layout() {
  local app data
  app="$(normalize_remote_path "$REMOTE_APP_DIR")"
  data="$(normalize_remote_path "$REMOTE_DATA_DIR")"

  if [[ "$data" == "$app" || "$data" == "$app"/* ]]; then
    echo "Unsafe layout: REMOTE_DATA_DIR is inside REMOTE_APP_DIR"
    echo "  REMOTE_APP_DIR=$app"
    echo "  REMOTE_DATA_DIR=$data"
    echo "Please set REMOTE_DATA_DIR to a path outside app dir, e.g. /opt/ios-headers-data"
    exit 1
  fi
}

generate_code_manifest_from0() {
  local manifest_raw="$1"
  local manifest_filtered="$2"

  {
    git -C "$ROOT_DIR" ls-files -z
    git -C "$ROOT_DIR" ls-files --others --exclude-standard -z
  } > "$manifest_raw"

  : > "$manifest_filtered"
  while IFS= read -r -d '' path; do
    case "$path" in
      .DS_Store|*/.DS_Store|*.pyc|__pycache__/*|*/__pycache__/*)
        continue
        ;;
      *)
        printf '%s\0' "$path" >> "$manifest_filtered"
        ;;
    esac
  done < "$manifest_raw"
}

generate_code_manifest_lines() {
  local manifest_txt="$1"

  {
    git -C "$ROOT_DIR" ls-files
    git -C "$ROOT_DIR" ls-files --others --exclude-standard
  } | sort -u |
    grep -Ev '(^|/)\.DS_Store$|(^|/)__pycache__(/|$)|\.pyc$' > "$manifest_txt"
}

sync_code() {
  require_cmd git
  require_cmd ssh
  require_cmd rsync

  if ! is_modern_rsync; then
    echo "Current rsync is not modern (likely openrsync)."
    echo "Please ensure Homebrew rsync is first in PATH, e.g. /opt/homebrew/opt/rsync/bin."
    exit 1
  fi

  validate_remote_layout

  ensure_remote_dirs

  echo "[info] mode=code dry_run=$DRY_RUN host=$REMOTE_HOST app_dir=$REMOTE_APP_DIR"

  local -a args
  args=( ${(f)"$(common_rsync_args)"} )

  if supports_from0; then
    local manifest_raw manifest_filtered
    manifest_raw="$(mktemp)"
    manifest_filtered="$(mktemp)"

    generate_code_manifest_from0 "$manifest_raw" "$manifest_filtered"

    local -a extra
    extra=( --from0 --files-from="$manifest_filtered" )
    if supports_delete_missing_args; then
      extra+=( --delete-missing-args )
    fi

    run_rsync "${args[@]}" "${extra[@]}" "$ROOT_DIR/" "$REMOTE_HOST:$REMOTE_APP_DIR/"
    rm -f "$manifest_raw" "$manifest_filtered"
  else
    local manifest_txt
    manifest_txt="$(mktemp)"

    generate_code_manifest_lines "$manifest_txt"

    local -a extra
    extra=( --files-from="$manifest_txt" )
    if supports_delete_missing_args; then
      extra+=( --delete-missing-args )
    fi

    run_rsync "${args[@]}" "${extra[@]}" "$ROOT_DIR/" "$REMOTE_HOST:$REMOTE_APP_DIR/"
    rm -f "$manifest_txt"
  fi
}

sync_data_dir() {
  local name="$1"
  local src="$ROOT_DIR/data/$name/"
  local dst="$REMOTE_HOST:$REMOTE_DATA_DIR/$name/"

  [[ -d "$ROOT_DIR/data/$name" ]] || {
    echo "Skip missing local data dir: $ROOT_DIR/data/$name"
    return 0
  }

  local -a args
  args=( ${(f)"$(common_rsync_args)"} )
  echo "[info] mode=data dry_run=$DRY_RUN src=$src dst=$dst"
  local rc
  set +e
  run_rsync "${args[@]}" --delete "$src" "$dst"
  rc=$?
  set -e

  if [[ $rc -eq 24 ]]; then
    echo "[warn] rsync code 24 on $name: some files vanished during transfer (usually runtime-changing files)."
    echo "[warn] Continue syncing remaining data directories."
    return 0
  fi

  return $rc
}

sync_data() {
  require_cmd ssh
  require_cmd rsync

  validate_remote_layout

  ensure_remote_dirs

  local target="${1:-all}"
  case "$target" in
    all)
      sync_data_dir clickhouse
      sync_data_dir minio
      sync_data_dir redis
      ;;
    clickhouse|minio|redis)
      sync_data_dir "$target"
      ;;
    *)
      echo "Invalid data target: $target"
      echo "Use: all|clickhouse|minio|redis"
      exit 1
      ;;
  esac
}

sync_env() {
  require_cmd ssh
  require_cmd rsync

  validate_remote_layout

  [[ -f "$ENV_FILE" ]] || {
    echo "Env file not found: $ENV_FILE"
    exit 1
  }

  ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_APP_DIR'"

  local env_tmp
  env_tmp="$(mktemp)"
  cp "$ENV_FILE" "$env_tmp"

  if grep -q '^FRP_SERVER_ADDR=' "$env_tmp"; then
    sed -i'' -e "s#^FRP_SERVER_ADDR=.*#FRP_SERVER_ADDR=#" "$env_tmp"
  else
    printf '\nFRP_SERVER_ADDR=\n' >> "$env_tmp"
  fi

  if grep -q '^STACK_DATA_DIR=' "$env_tmp"; then
    sed -i'' -e "s#^STACK_DATA_DIR=.*#STACK_DATA_DIR=$REMOTE_DATA_DIR#" "$env_tmp"
  else
    printf '\nSTACK_DATA_DIR=%s\n' "$REMOTE_DATA_DIR" >> "$env_tmp"
  fi

  local -a args
  args=( ${(f)"$(common_rsync_args)"} )
  echo "[info] mode=env dry_run=$DRY_RUN src=$ENV_FILE dst=$REMOTE_HOST:$REMOTE_APP_DIR/.env stack_data_dir=$REMOTE_DATA_DIR"
  run_rsync "${args[@]}" "$env_tmp" "$REMOTE_HOST:$REMOTE_APP_DIR/.env"

  rm -f "$env_tmp"
}

usage() {
  cat <<EOF
usage: $SCRIPT_NAME [global-options] {code|data [all|clickhouse|minio|redis]|env}

global options:
  --host <ssh-host>            default: $REMOTE_HOST
  --remote-app-dir <path>      default: $REMOTE_APP_DIR
  --remote-data-dir <path>     default: $REMOTE_DATA_DIR
  --env-file <path>            default: $ENV_FILE
  --dry-run                    show changes only
  --verbose                    verbose rsync output

examples:
  $SCRIPT_NAME --host drm-north code
  $SCRIPT_NAME --host drm-north data all
  $SCRIPT_NAME --host drm-north data clickhouse
  $SCRIPT_NAME --host drm-north env
  $SCRIPT_NAME --host drm-north --dry-run code
EOF
}

main() {
  local -a pos
  while (( $# > 0 )); do
    case "$1" in
      --host)
        REMOTE_HOST="$2"
        shift 2
        ;;
      --remote-app-dir)
        REMOTE_APP_DIR="$2"
        shift 2
        ;;
      --remote-data-dir)
        REMOTE_DATA_DIR="$2"
        shift 2
        ;;
      --env-file)
        ENV_FILE="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN="true"
        shift
        ;;
      --verbose)
        VERBOSE="true"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        pos+=( "$1" )
        shift
        ;;
    esac
  done

  if (( ${#pos[@]} == 0 )); then
    usage
    exit 1
  fi

  local cmd="${pos[1]}"
  case "$cmd" in
    code)
      sync_code
      ;;
    data)
      sync_data "${pos[2]:-all}"
      ;;
    env)
      sync_env
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"

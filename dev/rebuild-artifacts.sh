#!/usr/bin/env bash
# Rebuild the independently cached artifacts used by local Jazz tooling.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'EOF'
Usage: dev/rebuild-artifacts.sh [tools] [server] [napi] [wasm] [wasm-fast]

With no arguments, rebuild every layer. Specify one or more layers to skip
expensive layers such as wasm.
EOF
}

if (($# == 0)); then
  layers=(tools server napi wasm)
else
  layers=("$@")
fi

for layer in "${layers[@]}"; do
  case "$layer" in
    tools|server|napi|wasm|wasm-fast) ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown artifact layer: $layer" >&2
      usage >&2
      exit 2
      ;;
  esac
done

overall=0
run_layer() {
  local layer="$1"
  shift
  local status

  printf '\n==> %s\n' "$layer"
  printf '    '
  printf '%q ' "$@"
  printf '\n'

  # Do not use this command as an if-condition: capture its status directly so
  # command-not-found (127) and every build failure are reported truthfully.
  set +e
  "$@"
  status=$?
  set -e

  printf '<== %s: exit %d\n' "$layer" "$status"
  if ((status != 0)); then
    overall=1
  fi
}

for layer in "${layers[@]}"; do
  case "$layer" in
    tools)
      run_layer tools pnpm --filter jazz-tools build
      ;;
    server)
      run_layer server cargo build -p jazz-cli --bin jazz-tools
      ;;
    napi)
      run_layer napi pnpm --filter jazz-napi build:debug
      ;;
    wasm)
      run_layer wasm pnpm --filter jazz-wasm build
      ;;
    wasm-fast)
      run_layer wasm-fast pnpm --filter jazz-wasm build:fast
      ;;
  esac
done

exit "$overall"

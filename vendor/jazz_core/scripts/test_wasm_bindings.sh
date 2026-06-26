#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
alpha_node_example="$repo_root/examples/jazz-tools"
browser_example="$repo_root/examples/browser-wasm"

usage() {
  cat <<'EOF'
usage: scripts/test_wasm_bindings.sh [--install]

Runs the TypeScript WASM binding gates sequentially:
  1. examples/jazz-tools npm test
  2. examples/browser-wasm npm test

Prerequisites:
  - wasm-pack on PATH
  - npm install in each TS WASM example package, or pass --install to run npm ci
  - Playwright Chromium installed for the browser smoke test:
      cd examples/browser-wasm && npx playwright install chromium

Targeted iteration:
  - cd examples/jazz-tools && npm run test:alpha-public-flow
  - cd examples/jazz-tools && npm run test:transaction-compat
  - cd examples/browser-wasm && npm run smoke:built
EOF
}

install_deps=0

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
elif [[ "${1:-}" == "--install" ]]; then
  install_deps=1
  shift
fi

if [[ $# -gt 0 ]]; then
  echo "unknown option: $1" >&2
  usage >&2
  exit 2
fi

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    echo "see scripts/test_wasm_bindings.sh --help for prerequisites" >&2
    exit 1
  fi
}

require_node_modules() {
  local package_dir="$1"

  if [[ ! -d "$package_dir/node_modules" ]]; then
    echo "missing npm install in ${package_dir#$repo_root/}" >&2
    echo "run: cd ${package_dir#$repo_root/} && npm install, or rerun this helper with --install" >&2
    exit 1
  fi
}

install_package() {
  local package_dir="$1"

  echo
  echo "==> npm ci in ${package_dir#$repo_root/}"
  (cd "$package_dir" && npm ci)
}

run_gate() {
  local label="$1"
  local package_dir="$2"

  echo
  echo "==> $label"
  (cd "$package_dir" && npm test)
}

require_command wasm-pack
require_command npm

if [[ "$install_deps" -eq 1 ]]; then
  install_package "$alpha_node_example"
  install_package "$browser_example"
else
  require_node_modules "$alpha_node_example"
  require_node_modules "$browser_example"
fi

run_gate "Alpha Node direct facade gate" "$alpha_node_example"
run_gate "Browser direct WasmDb gate (bundle, worker smoke)" "$browser_example"

echo
echo "WASM binding gates passed."

#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "::error::Missing required environment variable: ${name}"
    exit 1
  fi
}

build_scope_args() {
  SCOPE_ARGS=()
  if [ -n "${VERCEL_TEAM_ID:-}" ]; then
    SCOPE_ARGS+=(--scope "${VERCEL_TEAM_ID}")
  fi
}

validate_secrets() {
  require_env MAESTRO_KEY
  require_env MAESTRO_PROJECT_ID
  require_env VERCEL_SANDBOXES_TOKEN
  require_env VERCEL_SANDBOXES_PROJECT_ID
}

ensure_wasm_pack_binary() {
  local wasm_pack_pkg_json
  local wasm_pack_bin

  # Resolve wasm-pack from the jazz-wasm package context so we always use the
  # workspace node_modules version (never a globally installed binary).
  wasm_pack_pkg_json="$(
    pnpm --filter jazz-wasm exec node -e "process.stdout.write(require.resolve('wasm-pack/package.json'))"
  )"
  wasm_pack_bin="$(dirname "${wasm_pack_pkg_json}")/binary/wasm-pack"

  if [ ! -f "${wasm_pack_bin}" ]; then
    echo "::error::wasm-pack binary not found at ${wasm_pack_bin}"
    exit 1
  fi

  chmod +x "${wasm_pack_bin}"
  pnpm --filter jazz-wasm exec wasm-pack --version
}

start_sandbox_server() {
  require_env VERCEL_SANDBOXES_TOKEN
  require_env VERCEL_SANDBOXES_PROJECT_ID
  require_env JAZZ_E2E_APP_ID
  require_env JAZZ_E2E_ADMIN_SECRET
  require_env JAZZ_E2E_SERVER_PORT
  require_env GITHUB_ENV

  rm -f "${ROOT_DIR}/vercel-sandbox.log" "${ROOT_DIR}/server.log"

  local sandbox_create_output
  local sandbox_id
  local sandbox_arch
  local sandbox_rust_target
  local sandbox_binary
  local sandbox_host
  local server_public_url=""
  local sandbox_ready=0
  local candidate
  local existing
  local seen
  local -a url_candidates=()
  local -a deduped_url_candidates=()

  SANDBOX_ARGS=(
    --token "${VERCEL_SANDBOXES_TOKEN}"
    --project "${VERCEL_SANDBOXES_PROJECT_ID}"
    --runtime node22
    --timeout 60m
    --publish-port "${JAZZ_E2E_SERVER_PORT}"
  )
  build_scope_args
  if [ "${#SCOPE_ARGS[@]}" -gt 0 ]; then
    SANDBOX_ARGS+=("${SCOPE_ARGS[@]}")
  fi

  sandbox_create_output="$(sandbox create "${SANDBOX_ARGS[@]}")"
  printf '%s\n' "${sandbox_create_output}" | tee "${ROOT_DIR}/vercel-sandbox.log"

  sandbox_id="$(printf '%s\n' "${sandbox_create_output}" | grep -Eo 'sb_[a-zA-Z0-9]+' | head -n 1 || true)"
  if [ -z "${sandbox_id}" ]; then
    echo "::error::Could not parse sandbox ID from sandbox create output."
    exit 1
  fi
  echo "SANDBOX_ID=${sandbox_id}" >> "${GITHUB_ENV}"

  sandbox_arch="$(
    sandbox exec \
      --token "${VERCEL_SANDBOXES_TOKEN}" \
      --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
      "${SCOPE_ARGS[@]}" \
      "${sandbox_id}" \
      sh -lc 'uname -m' | tr -d '\r' | tail -n 1
  )"

  case "${sandbox_arch}" in
    x86_64 | amd64)
      sandbox_rust_target="x86_64-unknown-linux-gnu"
      ;;
    aarch64 | arm64)
      sandbox_rust_target="aarch64-unknown-linux-gnu"
      ;;
    *)
      echo "::error::Unsupported sandbox architecture: ${sandbox_arch}"
      exit 1
      ;;
  esac

  sandbox_binary="${ROOT_DIR}/target/${sandbox_rust_target}/release/jazz-tools"
  echo "Detected sandbox architecture: ${sandbox_arch} (${sandbox_rust_target})"

  # Build only the target needed by the actual sandbox architecture.
  sccache --show-stats || true
  cargo build --release -p jazz-tools --bin jazz-tools --features cli --target "${sandbox_rust_target}"
  sccache --show-stats || true

  if [ ! -f "${sandbox_binary}" ]; then
    echo "::error::Expected jazz-tools binary not found at ${sandbox_binary}."
    exit 1
  fi

  sandbox copy \
    --token "${VERCEL_SANDBOXES_TOKEN}" \
    --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
    "${SCOPE_ARGS[@]}" \
    "${sandbox_binary}" \
    "${sandbox_id}:/tmp/jazz-tools"

  sandbox exec \
    --token "${VERCEL_SANDBOXES_TOKEN}" \
    --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
    "${SCOPE_ARGS[@]}" \
    "${sandbox_id}" \
    sh -lc "chmod +x /tmp/jazz-tools && nohup /tmp/jazz-tools server '${JAZZ_E2E_APP_ID}' --admin-secret '${JAZZ_E2E_ADMIN_SECRET}' --port '${JAZZ_E2E_SERVER_PORT}' >/tmp/server.log 2>&1 &"

  while IFS= read -r candidate; do
    if [ -n "${candidate}" ]; then
      url_candidates+=("${candidate%/}")
    fi
  done < <(printf '%s\n' "${sandbox_create_output}" | grep -Eo 'https://[^ )]+' || true)

  sandbox_host="${sandbox_id//_/-}"
  url_candidates+=("https://${sandbox_host}.vercel.app")
  url_candidates+=("https://${sandbox_host}.vercel.run")

  for candidate in "${url_candidates[@]}"; do
    seen=0
    for existing in "${deduped_url_candidates[@]}"; do
      if [ "${existing}" = "${candidate}" ]; then
        seen=1
        break
      fi
    done
    if [ "${seen}" -eq 0 ]; then
      deduped_url_candidates+=("${candidate}")
    fi
  done

  if [ "${#deduped_url_candidates[@]}" -eq 0 ]; then
    echo "::error::Could not derive sandbox URL candidates from sandbox create output."
    exit 1
  fi

  for _attempt in $(seq 1 120); do
    for candidate in "${deduped_url_candidates[@]}"; do
      if curl --fail --silent --show-error --max-time 5 "${candidate}/health" > /dev/null; then
        server_public_url="${candidate}"
        sandbox_ready=1
        break 2
      fi
    done
    sleep 1
  done

  if [ "${sandbox_ready}" -ne 1 ]; then
    echo "::error::Timed out waiting for sandbox URL health check."
    echo "Tried candidates: ${deduped_url_candidates[*]}"
    sandbox exec \
      --token "${VERCEL_SANDBOXES_TOKEN}" \
      --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
      "${SCOPE_ARGS[@]}" \
      "${sandbox_id}" \
      sh -lc "tail -n 200 /tmp/server.log || true" | tee -a "${ROOT_DIR}/vercel-sandbox.log"
    exit 1
  fi

  echo "SERVER_PUBLIC_URL=${server_public_url}" >> "${GITHUB_ENV}"
  echo "Verified sandbox health endpoint: ${server_public_url}"
}

build_expo_apk() {
  require_env EXPO_PUBLIC_JAZZ_SERVER_URL

  local app_dir="${ROOT_DIR}/examples/todo-client-localfirst-expo"
  local android_dir="${app_dir}/android"

  if ! printf '%s' "${EXPO_PUBLIC_JAZZ_SERVER_URL}" | grep -Eq '^https://[^[:space:]]+$'; then
    echo "::error::EXPO_PUBLIC_JAZZ_SERVER_URL has unexpected format: ${EXPO_PUBLIC_JAZZ_SERVER_URL}"
    exit 1
  fi

  if [ ! -f "${android_dir}/gradlew" ]; then
    echo "::warning::Missing ${android_dir}/gradlew before assembleRelease; regenerating Android project."
    pnpm --filter todo-client-localfirst-expo verify:expo:android
  fi

  if [ ! -f "${android_dir}/gradlew" ]; then
    echo "::error::Android project was not generated at ${android_dir}."
    exit 1
  fi

  chmod +x "${android_dir}/gradlew"
  (
    cd "${android_dir}"
    ./gradlew :app:assembleRelease --no-daemon
  )
}

fetch_sandbox_server_log() {
  if [ -z "${SANDBOX_ID:-}" ]; then
    echo "::warning::SANDBOX_ID is not set; skipping sandbox server log download."
    exit 0
  fi

  require_env VERCEL_SANDBOXES_TOKEN
  require_env VERCEL_SANDBOXES_PROJECT_ID

  build_scope_args
  sandbox copy \
    --token "${VERCEL_SANDBOXES_TOKEN}" \
    --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
    "${SCOPE_ARGS[@]}" \
    "${SANDBOX_ID}:/tmp/server.log" \
    "${ROOT_DIR}/server.log" || true
}

verify_server_evidence() {
  local sync_count
  local events_count

  if [ ! -f "${ROOT_DIR}/server.log" ]; then
    echo "::error::server.log not found before verification."
    exit 1
  fi

  sync_count="$(grep -c 'sync request' "${ROOT_DIR}/server.log" || true)"
  events_count="$(grep -c 'events stream connecting' "${ROOT_DIR}/server.log" || true)"

  echo "sync request count: ${sync_count}"
  echo "events stream connecting count: ${events_count}"

  if [ "${sync_count}" -eq 0 ]; then
    echo "::error::No 'sync request' lines found in server.log."
    exit 1
  fi

  if [ "${events_count}" -eq 0 ]; then
    echo "::error::No 'events stream connecting' lines found in server.log."
    exit 1
  fi
}

stop_sandbox() {
  if [ -z "${SANDBOX_ID:-}" ]; then
    echo "::warning::SANDBOX_ID not set; skipping sandbox stop."
    exit 0
  fi

  require_env VERCEL_SANDBOXES_TOKEN
  require_env VERCEL_SANDBOXES_PROJECT_ID

  build_scope_args
  sandbox stop \
    --token "${VERCEL_SANDBOXES_TOKEN}" \
    --project "${VERCEL_SANDBOXES_PROJECT_ID}" \
    "${SCOPE_ARGS[@]}" \
    "${SANDBOX_ID}" || true
}

usage() {
  cat <<'EOF'
Usage: scripts/expo-android-maestro-e2e.sh <command>

Commands:
  validate-secrets
  ensure-wasm-pack-binary
  start-sandbox-server
  build-expo-apk
  fetch-sandbox-server-log
  verify-server-evidence
  stop-sandbox
EOF
}

main() {
  local command="${1:-}"
  case "${command}" in
    validate-secrets) validate_secrets ;;
    ensure-wasm-pack-binary) ensure_wasm_pack_binary ;;
    start-sandbox-server) start_sandbox_server ;;
    build-expo-apk) build_expo_apk ;;
    fetch-sandbox-server-log) fetch_sandbox_server_log ;;
    verify-server-evidence) verify_server_evidence ;;
    stop-sandbox) stop_sandbox ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"

#!/usr/bin/env bash

# The test-ts runner has 16 CPUs. Turbo gets at most two test tasks, while the
# browser lane runs the Jazz Tools and inspector suites in parallel. Jazz Tools
# caps Vitest at four file workers so Chromium retains scheduling headroom; all
# suites reuse the artifact build completed before this script starts.
set -u

# This runner is deliberately not a standalone shortcut.  Its parent performs
# the producer-manifest admission and supplies exact snapshot paths; accepting
# a direct invocation would reintroduce mutable NAPI/WASM pointer selection.
if [[ "${JAZZ_SKIP_JAZZ_TOOLS_BUILD:-0}" != "1" ]]; then
  if [[ "${JAZZ_CORRECTNESS_ARTIFACT_RUN:-}" != "1" ]]; then
    echo "run-ts-tests must be launched through pnpm test:typescript-consumers" >&2
    exit 1
  fi
  for required_artifact_env in \
    JAZZ_CORRECTNESS_WASM_PACKAGE \
    JAZZ_CORRECTNESS_NAPI_BINDING \
    JAZZ_CORRECTNESS_NAPI_FINGERPRINT \
    JAZZ_CORRECTNESS_CLI; do
    if [[ -z "${!required_artifact_env:-}" ]]; then
      echo "run-ts-tests is missing immutable correctness artifact ${required_artifact_env}" >&2
      exit 1
    fi
  done
fi

# The runner accepts small command overrides solely so its process-management
# contract tests can use deterministic short-lived children.  The shared local
# CI partition sets this guard, matching CI's unmodified environment: an
# inherited override would otherwise let a local "CI-equivalent" receipt skip
# the suites that GitHub will run.
if [[ "${JAZZ_REQUIRE_CI_TEST_COMMANDS:-0}" == "1" ]]; then
  for override in JAZZ_NODE_TEST_COMMAND JAZZ_BROWSER_TEST_COMMAND JAZZ_SKIP_JAZZ_TOOLS_BUILD; do
    if [[ -v "${override}" ]]; then
      echo "${override} is a test-harness override and is forbidden by the CI-equivalent partition" >&2
      exit 1
    fi
  done
fi

# Every workspace package's `test` target belongs to this Node/Turbo partition.
# Browser-only receipts keep their topology out of that target and run through
# `test:browser` below, where their Vitest projects own the Jazz server commands.
node_tests_command=${JAZZ_NODE_TEST_COMMAND:-"pnpm test --filter=!moon-lander-react --filter=!@jazz/rust --filter=!auth-simple-chat --filter=!auth-workos-chat --filter=!auth-betterauth-chat --filter=!chat-react --filter=!world-tour --filter=!jazz-rn --concurrency=2"}
browser_tests_command=${JAZZ_BROWSER_TEST_COMMAND:-"pnpm --parallel --filter jazz-tools --filter inspector --filter band-chat-nextjs-betterauth --filter record-player-next-betterauth test:browser"}
node_tests_pid=""
browser_tests_pid=""
log_dir=${RUNNER_TEMP:-/tmp}
node_tests_log="${log_dir}/jazz-node-tests-$$.log"
browser_tests_log="${log_dir}/jazz-browser-tests-$$.log"

# The producer receipt is checked before either suite starts. It binds the
# immutable NAPI/WASM snapshot and CLI to this exact checkout, so a cache hit
# from another revision cannot become a TypeScript false-green. Jazz Tools is
# built by run-ts-consumers.mjs after that preflight; this runner only consumes
# the prepared surface and seals it against child rebuilds.
if [[ "${JAZZ_SKIP_JAZZ_TOOLS_BUILD:-0}" != "1" ]]; then
  if ! node dev/artifacts/correctness-artifact-producer.mjs; then
    echo "prepared native correctness artifact manifest is missing or stale; run pnpm build:correctness-artifacts" >&2
    exit 1
  fi
  if ! node -e "require('./crates/jazz-napi')"; then
    echo "prepared release jazz-napi artifact did not load; run pnpm build:correctness-artifacts before test-ts" >&2
    exit 1
  fi
  if ! node dev/gates/verify-jazz-tools-exports.mjs; then
    echo "prepared jazz-tools public export surface is incomplete; refusing to launch suites" >&2
    exit 1
  fi
  # Test children share this prepared public surface. A child that tries to
  # rebuild it fails before clean-dist can remove files another suite imports.
  export JAZZ_TEST_SEALED_TOOLS_DIST=1
fi

terminate_children() {
  trap - INT TERM
  for child_pid in "${node_tests_pid}" "${browser_tests_pid}"; do
    if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
      # Each suite starts in its own session, so this reaches pnpm and every
      # descendant it spawned instead of leaving Vitest/Turbo processes behind.
      kill -TERM -- "-${child_pid}" 2>/dev/null || true
    fi
  done
  [[ -z "${node_tests_pid}" ]] || wait "${node_tests_pid}" 2>/dev/null || true
  [[ -z "${browser_tests_pid}" ]] || wait "${browser_tests_pid}" 2>/dev/null || true
}

interrupt() {
  local signal_status=$1
  terminate_children
  exit "${signal_status}"
}

trap 'interrupt 130' INT
trap 'interrupt 143' TERM

setsid bash -c "${node_tests_command}" >"${node_tests_log}" 2>&1 &
node_tests_pid=$!
setsid bash -c "${browser_tests_command}" >"${browser_tests_log}" 2>&1 &
browser_tests_pid=$!

wait "${node_tests_pid}"
node_tests_status=$?
wait "${browser_tests_pid}"
browser_tests_status=$?
trap - INT TERM

# GitHub's live log pipe can be nonblocking. Two concurrent Turbo/Vitest trees
# writing directly to it can fail with EAGAIN even though the tests themselves
# are healthy. Give each suite a blocking regular file, then replay both logs
# after both process trees have terminated.
cat "${node_tests_log}"
cat "${browser_tests_log}"
rm -f "${node_tests_log}" "${browser_tests_log}"

echo "Node test suite exit status: ${node_tests_status}"
echo "Browser test suite exit status: ${browser_tests_status}"

if [[ "${node_tests_status}" -ne 0 || "${browser_tests_status}" -ne 0 ]]; then
  exit 1
fi

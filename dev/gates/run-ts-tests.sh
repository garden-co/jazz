#!/usr/bin/env bash

# The test-ts runner has 16 CPUs. Vite+ runs at most three package test tasks.
# Jazz Tools retains its eight-worker Node cap; its separate Chromium lane
# uses four file workers. All suites reuse the admitted artifact build.
#
# Usage: run-ts-tests.sh [--shard NAME]
#        run-ts-tests.sh --describe-shard NAME
#
# Without --shard it runs every suite (the `all` shard). CI's test-ts matrix
# runs the named shards instead, one per job; together they run exactly the
# suites of `all` (dev/gates/test/ts-shards.test.mjs proves the union). Each
# shard keeps every guarantee below: the producer receipt, the sealed dists and
# the two-lane log handling. --describe-shard prints a shard's two lane
# commands and exits, for that contract.
set -u

ts_shards="all tools-node-1 tools-node-2 tools-other browser-1 browser-2 packages examples"
# Not a merge-group partition: build-jazz-packages.yml and nightly-tests.yml
# run jazz-tools' Durable Object suite alone, on the -Oz host that ships.
# Merge groups run the same suite inside tools-other.
release_ts_shards="durable-objects"
workspace_tests="pnpm exec vp run --parallel --concurrency-limit=3 --ignore-depends-on --no-cache --filter=!. --filter=!@jazz/rust --filter=!auth-simple-chat --filter=!auth-workos-chat --filter=!auth-betterauth-chat --filter=!chat-react --filter=!jazz-rn"
all_browser_tests_command="pnpm --parallel --filter jazz-tools --filter inspector --filter band-chat-nextjs-betterauth --filter record-player-next-betterauth --filter auth-workos-chat test:browser"

# Two lanes per shard, run concurrently as the node and browser suites always
# have been; ":" is an empty lane. jazz-tools' `test` is `test:without-node`
# then `test:node`, and its `test:browser` is `test:browser:vitest`, then
# `test:browser:storage-quota`, then `test:browser:transport`.
shard_lanes() {
  case "$1" in
    all)
      shard_node_command="${workspace_tests} test"
      shard_browser_command=${all_browser_tests_command} ;;
    tools-node-1)
      shard_node_command="pnpm --filter jazz-tools run test:node --shard=1/2"
      shard_browser_command=":" ;;
    tools-node-2)
      shard_node_command="pnpm --filter jazz-tools run test:node --shard=2/2"
      shard_browser_command=":" ;;
    tools-other)
      shard_node_command="pnpm --filter jazz-tools run test:without-node"
      shard_browser_command=":" ;;
    # The other packages' browser suites share browser-1 with the first half
    # of jazz-tools' files; its storage-quota and transport receipts follow
    # the second half.
    browser-1)
      shard_node_command="pnpm --parallel --filter inspector --filter band-chat-nextjs-betterauth --filter record-player-next-betterauth --filter auth-workos-chat test:browser"
      shard_browser_command="pnpm --filter jazz-tools run test:browser:vitest --shard=1/2" ;;
    browser-2)
      shard_node_command=":"
      shard_browser_command="pnpm --filter jazz-tools run test:browser:vitest --shard=2/2 && pnpm --filter jazz-tools run test:browser:storage-quota && pnpm --filter jazz-tools run test:browser:transport" ;;
    packages)
      shard_node_command="${workspace_tests} --filter=!jazz-tools test"
      shard_browser_command=":" ;;
    # The examples partition's own commands (typecheck, bindings, browser
    # corpus) follow this runner; the shard only admits the prepared surface.
    examples)
      shard_node_command=":"
      shard_browser_command=":" ;;
    durable-objects)
      shard_node_command="pnpm --filter jazz-tools run test:durable-objects"
      shard_browser_command=":" ;;
    *)
      echo "run-ts-tests: unknown shard '$1' (expected one of: ${ts_shards} ${release_ts_shards})" >&2
      return 1 ;;
  esac
}

shard=all
if [[ $# -eq 2 && "$1" == "--describe-shard" ]]; then
  shard_lanes "$2" || exit 2
  printf '%s\n%s\n' "${shard_node_command}" "${shard_browser_command}"
  exit 0
elif [[ $# -eq 2 && "$1" == "--shard" ]]; then
  shard=$2
elif [[ $# -ne 0 ]]; then
  echo "usage: run-ts-tests.sh [--shard NAME] | --describe-shard NAME" >&2
  exit 2
fi
shard_lanes "${shard}" || exit 2

# macOS exposes its temporary directory through the `/var` symlink. Tests that
# deliberately reject symlinked migration paths should receive the canonical
# location, just as they do on Linux CI.
if [[ -n "${TMPDIR:-}" && -d "${TMPDIR}" ]]; then
  TMPDIR="$(cd "${TMPDIR}" && pwd -P)"
  export TMPDIR
fi

# The runner accepts small command overrides solely so its process-management
# contract tests can use deterministic short-lived children. The shared local
# CI partition sets this guard, matching CI's unmodified environment: an
# inherited override would otherwise let a local "CI-equivalent" receipt skip
# the suites that GitHub will run. Check this before the producer admission so
# an unsafe inherited control always gets its own actionable diagnostic.
if [[ "${JAZZ_REQUIRE_CI_TEST_COMMANDS:-0}" == "1" ]]; then
  for override in JAZZ_NODE_TEST_COMMAND JAZZ_BROWSER_TEST_COMMAND JAZZ_SKIP_JAZZ_TOOLS_BUILD JAZZ_TEST_LOG_INTERVAL_SECONDS; do
    if printenv "${override}" >/dev/null 2>&1; then
      echo "${override} is a test-harness override and is forbidden by the CI-equivalent partition" >&2
      exit 1
    fi
  done
fi

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
    JAZZ_CORRECTNESS_NAPI_FINGERPRINT; do
    if [[ -z "${!required_artifact_env:-}" ]]; then
      echo "run-ts-tests is missing sealed correctness artifact ${required_artifact_env}" >&2
      exit 1
    fi
  done
fi

# Every workspace package's `test` target belongs to this Node partition.
# Browser-only receipts keep their topology out of that target and run through
# `test:browser` below, where their Vitest projects own the Jazz server commands.
node_tests_command=${JAZZ_NODE_TEST_COMMAND:-${shard_node_command}}
browser_tests_command=${JAZZ_BROWSER_TEST_COMMAND:-${shard_browser_command}}
echo "TypeScript test shard: ${shard}"
node_tests_pid=""
browser_tests_pid=""
log_monitor_pid=""
log_interval=${JAZZ_TEST_LOG_INTERVAL_SECONDS:-30}
if [[ ! "${log_interval}" =~ ^[1-9][0-9]*$ ]]; then
  echo "JAZZ_TEST_LOG_INTERVAL_SECONDS must be a positive integer" >&2
  exit 1
fi
log_dir=${RUNNER_TEMP:-/tmp}
node_tests_log="${log_dir}/jazz-node-tests-$$.log"
browser_tests_log="${log_dir}/jazz-browser-tests-$$.log"

# The producer receipt is checked before either suite starts. It binds the
# sealed NAPI/WASM snapshot to this exact checkout, so a cache hit
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
  if [[ ! -f packages/inspector/dist-embedded/embedded.html ]]; then
    echo "prepared embedded inspector is missing; refusing to launch suites" >&2
    exit 1
  fi
  # Test children share this prepared public surface. A child that tries to
  # rebuild it fails before clean-dist can remove files another suite imports.
  export JAZZ_TEST_SEALED_TOOLS_DIST=1
  export JAZZ_TEST_SEALED_INSPECTOR_DIST=1
fi

stop_log_monitor() {
  if [[ -n "${log_monitor_pid}" ]]; then
    kill -TERM -- "-${log_monitor_pid}" 2>/dev/null || true
    wait "${log_monitor_pid}" 2>/dev/null || true
    log_monitor_pid=""
  fi
}

print_log_tail() {
  local suite_log=$1
  echo "--- ${suite_log} (last 16 KiB, at most 100 lines) ---"
  if [[ -f "${suite_log}" ]]; then
    tail -c 16384 "${suite_log}" | tail -n 100 || true
    echo
  fi
}

print_log_tails() {
  print_log_tail "${node_tests_log}"
  print_log_tail "${browser_tests_log}"
}

monitor_logs() {
  # Cancellation may kill an outer Node/container process without delivering
  # a trappable signal here. Publish progress beforehand, and stop within one
  # tick if this runner disappears without invoking its cleanup trap.
  local elapsed=0 index size
  local sizes=(0 0)
  local logs=("${node_tests_log}" "${browser_tests_log}")
  while kill -0 "$$" 2>/dev/null; do
    sleep 1
    kill -0 "$$" 2>/dev/null || return
    elapsed=$((elapsed + 1))
    if (( elapsed >= log_interval )); then
      for index in 0 1; do
        size=$(wc -c < "${logs[$index]}")
        if [[ "${size}" != "${sizes[$index]}" ]]; then
          echo "TypeScript suites still running; bounded live log tail:"
          print_log_tail "${logs[$index]}"
          sizes[$index]=${size}
        fi
      done
      elapsed=0
    fi
  done
}

terminate_children() {
  trap - INT TERM
  stop_log_monitor
  for child_pid in "${node_tests_pid}" "${browser_tests_pid}"; do
    if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
      # Each suite starts in its own session, so this reaches pnpm and every
      # descendant it spawned instead of leaving test processes behind.
      kill -TERM -- "-${child_pid}" 2>/dev/null || true
    fi
  done
  [[ -z "${node_tests_pid}" ]] || wait "${node_tests_pid}" 2>/dev/null || true
  [[ -z "${browser_tests_pid}" ]] || wait "${browser_tests_pid}" 2>/dev/null || true
}

interrupt() {
  local signal_status=$1
  # Replay before waiting for children: a stuck shutdown must not hide the
  # last test output. Bound bytes as well as lines (a test may print one huge
  # line), and retain the complete regular files at the printed paths.
  echo "TypeScript suites interrupted (exit ${signal_status}); retained log tails:"
  stop_log_monitor
  print_log_tails
  terminate_children
  exit "${signal_status}"
}

trap 'interrupt 130' INT
trap 'interrupt 143' TERM

# Job control gives each background suite its own process group on both the
# macOS system Bash and GNU Bash. `terminate_children` can therefore still
# terminate each complete test process tree without relying on Linux `setsid`.
echo "Node test live log: ${node_tests_log}"
echo "Browser test live log: ${browser_tests_log}"

set -m
test_env="$(dirname "${BASH_SOURCE[0]}")/with-test-env.sh"
bash "$test_env" bash -c "${node_tests_command}" >"${node_tests_log}" 2>&1 &
node_tests_pid=$!
bash "$test_env" bash -c "${browser_tests_command}" >"${browser_tests_log}" 2>&1 &
browser_tests_pid=$!
(trap - INT TERM; monitor_logs) &
log_monitor_pid=$!
echo "TypeScript live log monitor PID: ${log_monitor_pid}"
set +m

wait "${node_tests_pid}"
node_tests_status=$?
wait "${browser_tests_pid}"
browser_tests_status=$?
stop_log_monitor
trap - INT TERM

# GitHub's live log pipe can be nonblocking. Two concurrent test process trees
# writing directly to it can fail with EAGAIN even though the tests themselves
# are healthy. Give each suite a blocking regular file, then replay both logs
# after both process trees have terminated. Only the bounded monitor writes
# progress while tests run; test children never inherit the live log pipe.
cat "${node_tests_log}"
cat "${browser_tests_log}"
rm -f "${node_tests_log}" "${browser_tests_log}"

echo "Node test suite exit status: ${node_tests_status}"
echo "Browser test suite exit status: ${browser_tests_status}"

if [[ "${node_tests_status}" -ne 0 || "${browser_tests_status}" -ne 0 ]]; then
  exit 1
fi

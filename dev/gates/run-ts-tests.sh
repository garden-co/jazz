#!/usr/bin/env bash

# The test-ts runner has 16 CPUs. Turbo gets at most two test tasks, while the
# jazz-tools browser suite caps Vitest at four file workers so Chromium retains
# scheduling headroom; both reuse the artifact build completed before this
# script starts.
set -u

node_tests_command=${JAZZ_NODE_TEST_COMMAND:-"pnpm test --filter=!moon-lander-react --filter=!@jazz/rust --filter=!auth-simple-chat --filter=!auth-workos-chat --filter=!auth-betterauth-chat --filter=!chat-react --filter=!world-tour --filter=!jazz-rn --concurrency=2"}
browser_tests_command=${JAZZ_BROWSER_TEST_COMMAND:-"pnpm --filter jazz-tools test:browser"}
node_tests_pid=""
browser_tests_pid=""
log_dir=${RUNNER_TEMP:-/tmp}
node_tests_log="${log_dir}/jazz-node-tests-$$.log"
browser_tests_log="${log_dir}/jazz-browser-tests-$$.log"

# Every example resolves the public `jazz-tools/*` exports from `dist`. Build
# them once before either suite starts: otherwise an example's fallback build
# can clean that shared directory while a sibling browser suite imports it.
if [[ "${JAZZ_SKIP_JAZZ_TOOLS_BUILD:-0}" != "1" ]]; then
  pnpm --filter jazz-tools build || {
    status=$?
    echo "jazz-tools prebuild failed; refusing to launch suites against stale exports" >&2
    exit "${status}"
  }
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

#!/usr/bin/env bash
# Persistent lane worktrees.
#
# Lanes used to be created per task and never removed. With each carrying its
# own Cargo `target/`, ~28 of them filled a 1.8 TB disk mid-run — three times.
# A fixed pool bounds that, and reusing a lane keeps `target/` and
# `node_modules` warm, which also avoids the `ts-wire-codec` false-failures
# that fresh worktrees produce when they have no `node_modules`.
#
#   lane.sh acquire <branch> [base]   reset a free lane onto a new branch, print its path
#   lane.sh list                      show every lane, its branch, and whether it is busy
#   lane.sh reset <path> <branch> [base]
#
# A lane is "busy" if a codex process is running with it as cwd. Acquire never
# takes a busy lane, so concurrency is capped at LANE_COUNT — which is the
# point: 8 lanes x 6 build jobs already oversubscribes a 32-core box.
set -euo pipefail

LANE_ROOT="${LANE_ROOT:-/work}"
LANE_COUNT="${LANE_COUNT:-8}"
MAIN="${MAIN:-/work/jazz_core}"
DEFAULT_BASE="${DEFAULT_BASE:-origin/codex/jazz-core-engine-swap}"

lane_path() { echo "$LANE_ROOT/lane-$1"; }

lane_busy() {
  local path="$1" p cwd
  for p in $(pgrep -f "codex exec" 2>/dev/null || true); do
    cwd=$(readlink "/proc/$p/cwd" 2>/dev/null || true)
    [ "$cwd" = "$path" ] && return 0
  done
  return 1
}

# Reset a lane to a fresh branch. Deliberately keeps target/ and node_modules:
# they are the whole reason a pool beats fresh worktrees, and neither can carry
# source state between lanes.
#
# The workspace crates ARE cleaned. Reusing their artifacts across branches
# produced `error[E0460]: found possibly newer version of crate removed storage backend`,
# which fails an entire test binary at once and reads like 32 test failures
# rather than a build-cache problem. Third-party dependencies — the bulk of the
# build and the whole point of keeping target/ — are untouched.
lane_reset() {
  local path="$1" branch="$2" base="${3:-$DEFAULT_BASE}"
  git -C "$MAIN" fetch -q origin
  git -C "$path" reset -q --hard
  git -C "$path" clean -qfdx -e target -e node_modules
  git -C "$path" checkout -q -B "$branch" "$base"
  if [ -d "$path/target" ]; then
    (cd "$path" && cargo clean -q -p jazz -p groove \
        -p jazz-sim -p jazz-napi -p jazz-wasm 2>/dev/null || true)
  fi
  lane_install_node_modules "$path"
  echo "$path"
}

# A lane created by `git worktree add` has no node_modules, so `pnpm exec
# oxfmt` exits 254 and the format gate silently does nothing — a lane reported
# "oxfmt is not installed" rather than a formatting result, which is the worst
# kind of gate failure because it looks like a tooling note instead of a skipped
# check. With a warm pnpm store this costs ~3s, so it runs on every reset rather
# than only when node_modules is missing: a branch can change the lockfile.
lane_install_node_modules() {
  local path="$1"
  (cd "$path" && pnpm install --frozen-lockfile --prefer-offline) >/dev/null 2>&1 || {
    echo "lane.sh: WARNING: pnpm install failed in $path; oxfmt/vitest gates will not run" >&2
    return 0
  }
  if [ ! -x "$path/node_modules/.bin/oxfmt" ]; then
    echo "lane.sh: WARNING: oxfmt still missing in $path after install" >&2
  fi
}

cmd_ensure() {
  local i path
  for i in $(seq 1 "$LANE_COUNT"); do
    path=$(lane_path "$i")
    if [ ! -d "$path" ]; then
      git -C "$MAIN" worktree add -q --detach "$path" "$DEFAULT_BASE"
      echo "created $path"
    fi
  done
}

# A lane holding work that exists nowhere else must not be reset. Finished
# lanes have repeatedly sat unpushed — one carried a fix for ten browser test
# failures for a day — and reset would destroy them silently. Checks committed
# work not on any remote, and uncommitted changes.
lane_has_unsaved_work() {
  local path="$1" branch
  [ -d "$path" ] || return 1
  [ -n "$(git -C "$path" status --porcelain 2>/dev/null)" ] && return 0
  branch=$(git -C "$path" rev-parse --abbrev-ref HEAD 2>/dev/null)
  [ "$branch" = "HEAD" ] && return 1
  # HEAD, not --branches: --branches spans every local branch in the repo, so
  # unpushed work in any unrelated worktree would flag every lane.
  [ -n "$(git -C "$path" log --oneline HEAD --not --remotes -1 2>/dev/null)" ]
}

cmd_acquire() {
  local branch="${1:?usage: lane.sh acquire <branch> [base]}" base="${2:-$DEFAULT_BASE}"
  cmd_ensure >/dev/null
  local i path skipped=0
  for i in $(seq 1 "$LANE_COUNT"); do
    path=$(lane_path "$i")
    lane_busy "$path" && continue
    if [ "${LANE_FORCE:-0}" != "1" ] && lane_has_unsaved_work "$path"; then
      echo "skipping $path: unpushed or uncommitted work" >&2
      skipped=$((skipped + 1))
      continue
    fi
    lane_reset "$path" "$branch" "$base"
    return 0
  done
  echo "no free lane (${skipped} held unsaved work; push them, or LANE_FORCE=1 to override)" >&2
  return 1
}

cmd_list() {
  local i path branch state
  for i in $(seq 1 "$LANE_COUNT"); do
    path=$(lane_path "$i")
    if [ ! -d "$path" ]; then
      printf "lane-%-3s %-44s %s\n" "$i" "(not created)" "-"
      continue
    fi
    branch=$(git -C "$path" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    state=$(lane_busy "$path" && echo BUSY || echo free)
    printf "lane-%-3s %-44s %s\n" "$i" "$branch" "$state"
  done
}

case "${1:-}" in
  ensure)  shift; cmd_ensure "$@" ;;
  acquire) shift; cmd_acquire "$@" ;;
  reset)   shift; lane_reset "$@" ;;
  list)    shift; cmd_list "$@" ;;
  *) sed -n '2,20p' "$0" >&2; exit 2 ;;
esac

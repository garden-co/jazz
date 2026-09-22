#!/usr/bin/env bash
# Refuse stack/rebase operations against a checkout that has source state not
# represented by its current commit.  `git diff --quiet` alone misses staged
# changes, which is how a staged index can accidentally survive a restack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPOSITORY="${1:-$ROOT}"

git -C "$REPOSITORY" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
  echo "stack-preflight: not a Git worktree: $REPOSITORY" >&2
  exit 2
}

failed=0
if ! git -C "$REPOSITORY" diff --cached --quiet; then
  echo "stack-preflight: staged changes are present; commit, unstage, or preserve them before restacking." >&2
  failed=1
fi
if ! git -C "$REPOSITORY" diff --quiet; then
  echo "stack-preflight: unstaged tracked changes are present; commit, discard, or preserve them before restacking." >&2
  failed=1
fi
if [[ -n "$(git -C "$REPOSITORY" ls-files --others --exclude-standard)" ]]; then
  echo "stack-preflight: untracked non-ignored files are present; add, remove, or preserve them before restacking." >&2
  failed=1
fi

if (( failed )); then
  echo "stack-preflight: refusing stack/rebase operation in $REPOSITORY" >&2
  exit 1
fi

echo "stack-preflight: clean worktree and index: $REPOSITORY"

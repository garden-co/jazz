#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TEMP="$(mktemp -d "${TMPDIR:-/tmp}/jazz-stack-preflight-test.XXXXXX")"
trap 'rm -rf "$TEMP" "$TEMP.out"' EXIT

git -C "$TEMP" init -q
git -C "$TEMP" config user.email test@example.invalid
git -C "$TEMP" config user.name 'Jazz test'
printf 'tracked\n' >"$TEMP/file"
printf 'ignored\n' >"$TEMP/.gitignore"
git -C "$TEMP" add file .gitignore
git -C "$TEMP" commit -qm initial

run() {
  "$ROOT/dev/gates/require-clean-worktree.sh" "$TEMP"
}

run >/dev/null

printf 'staged\n' >>"$TEMP/file"
git -C "$TEMP" add file
if run >"$TEMP.out" 2>&1; then
  echo 'expected staged changes to fail preflight' >&2
  exit 1
fi
grep -F 'staged changes are present' "$TEMP.out" >/dev/null
git -C "$TEMP" restore --staged file
git -C "$TEMP" restore file

printf 'unstaged\n' >>"$TEMP/file"
if run >"$TEMP.out" 2>&1; then
  echo 'expected unstaged tracked changes to fail preflight' >&2
  exit 1
fi
grep -F 'unstaged tracked changes are present' "$TEMP.out" >/dev/null
git -C "$TEMP" restore file

printf 'untracked\n' >"$TEMP/untracked"
if run >"$TEMP.out" 2>&1; then
  echo 'expected untracked files to fail preflight' >&2
  exit 1
fi
grep -F 'untracked non-ignored files are present' "$TEMP.out" >/dev/null
rm "$TEMP/untracked"

printf 'cache\n' >"$TEMP/ignored"
run >/dev/null

echo 'stack clean-worktree preflight checks passed'

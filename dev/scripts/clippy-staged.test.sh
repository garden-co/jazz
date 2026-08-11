#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
real_cargo=$(command -v cargo)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/clippy-staged-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cat >"$tmp/git" <<'EOF'
#!/bin/sh
# Simulate a restricted child process: the hook must not need this executable.
exit 126
EOF
cat >"$tmp/cargo" <<'EOF'
#!/bin/sh
if [ "$1" = clippy ]; then
  printf '%s\n' "$*" >> "${CLIPPY_TEST_LOG:?}"
  exit "${CLIPPY_TEST_EXIT:-0}"
fi
if [ "$1" = metadata ]; then
  exec "${REAL_CARGO:?}" "$@"
fi
echo "unexpected cargo invocation: $*" >&2
exit 125
EOF
chmod +x "$tmp/git" "$tmp/cargo"

log=$tmp/cargo.log
output=$(PATH="$tmp:$PATH" REAL_CARGO="$real_cargo" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_LOG="$log" \
  sh "$root/dev/scripts/clippy-staged.sh" Cargo.toml)

grep -F -- '--workspace' "$log" >/dev/null

: >"$log"
output=$(PATH="$tmp:$PATH" REAL_CARGO="$real_cargo" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_LOG="$log" \
  sh "$root/dev/scripts/clippy-staged.sh" \
    "dev/benchmarks/storage/native/file with spaces.rs")
grep -F -- '--manifest-path dev/benchmarks/storage/native/Cargo.toml' "$log" >/dev/null
: >"$log"
PATH="$tmp:$PATH" REAL_CARGO="$real_cargo" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_LOG="$log" \
  sh "$root/dev/scripts/clippy-staged.sh" examples/todo-server-rs/src/main.rs >/dev/null
grep -F -- '--manifest-path examples/todo-server-rs/Cargo.toml' "$log" >/dev/null
if [ "$(grep -c -- '--package groove' "$log")" -ne 0 ]; then
  echo "root Cargo.toml must not be mixed with package mode" >&2
  exit 1
fi

: >"$log"
output=$(PATH="$tmp:$PATH" REAL_CARGO="$real_cargo" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_LOG="$log" \
  sh "$root/dev/scripts/clippy-staged.sh" crates/groove/removed.rs crates/groove/renamed.rs)
[ "$(grep -c -- '--package groove' "$log")" -eq 1 ]

if PATH="$tmp:$PATH" REAL_CARGO="$real_cargo" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_LOG="$log" CLIPPY_TEST_EXIT=37 \
  sh "$root/dev/scripts/clippy-staged.sh" crates/groove/removed.rs >/dev/null 2>&1; then
  echo "expected cargo failure to propagate" >&2
  exit 1
elif [ "$?" -ne 37 ]; then
  echo "cargo failure was not propagated" >&2
  exit 1
fi

echo "clippy-staged hook tests passed"

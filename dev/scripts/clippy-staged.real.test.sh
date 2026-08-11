#!/bin/sh
# Real Cargo probe for the nonmember fallback. Kept separate from the fast
# mocked mapper test because it intentionally compiles the full workspace.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output=$(sh "$root/dev/scripts/clippy-staged.sh" \
  "dev/benchmarks/storage/native/file with spaces.rs" \
  examples/todo-server-rs/src/main.rs)
printf '%s\n' "$output" | grep -F -- 'cargo clippy --workspace --all-targets -- -D warnings' >/dev/null
echo "real nonmember workspace fallback passed"

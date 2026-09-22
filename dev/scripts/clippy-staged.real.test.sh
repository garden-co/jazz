#!/bin/sh
# Real Cargo probe for the nonmember fallback. Kept separate from the fast
# mocked mapper test because it intentionally compiles the full workspace.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
set +e
output=$(sh "$root/dev/scripts/clippy-staged.sh" \
  "dev/benchmarks/storage/native/file with spaces.rs" \
  examples/todo-server-rs/src/main.rs 2>&1)
status=$?
set -e
if [ "$status" -ne 0 ]; then
  printf '%s\n' "$output" >&2
  exit "$status"
fi
printf '%s\n' "$output" | grep -F -- 'cargo clippy --workspace -- -D warnings' >/dev/null
echo "real nonmember workspace fallback passed"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TEMP="$(mktemp -d "${TMPDIR:-/tmp}/jazz-dev-t-test.XXXXXX")"
trap 'rm -rf "$TEMP"' EXIT
mkdir "$TEMP/bin"

cat >"$TEMP/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_CARGO_LOG"
if [[ " $* " == *" -- --list "* ]]; then
  if [[ " $* " == *" --test incremental_delivery_canary "* ]]; then
    printf '%s\n' 'maintained_relation::smoke: test'
  else
    printf '%s\n' \
      'db::tests::round_trips: test' \
      'db::tests::other: test' \
      'node::tests::harness::query_rows_at_lowers_filters_against_historical_current_rows: test'
  fi
  exit "${MOCK_INVENTORY_STATUS:-0}"
fi
printf '%s\n' 'running 1 test' 'test db::tests::round_trips ... ok' 'test result: ok. 1 passed; 0 failed;'
exit "${MOCK_RUN_STATUS:-0}"
EOF
chmod +x "$TEMP/bin/cargo"

run() {
  PATH="$TEMP/bin:$PATH" \
    MOCK_CARGO_LOG="$TEMP/cargo.log" \
    MOCK_INVENTORY_STATUS="${MOCK_INVENTORY_STATUS:-}" \
    MOCK_RUN_STATUS="${MOCK_RUN_STATUS:-}" \
    "$ROOT/dev/t" "$@"
}

run db::tests::round
grep -F -- '-p jazz --no-default-features --features test --lib -- --list' "$TEMP/cargo.log" >/dev/null
grep -F -- '-p jazz --no-default-features --features test --lib db::tests::round_trips -- --exact' "$TEMP/cargo.log" >/dev/null

: >"$TEMP/cargo.log"
run --exact db::tests::round_trips
grep -F -- 'db::tests::round_trips -- --exact' "$TEMP/cargo.log" >/dev/null

# Rust files under src/node/tests are wired through node::tests::harness.  A
# human should be able to supply the distinctive test-name suffix without
# knowing that internal module wrapper, and dev/t must still invoke Cargo with
# the canonical name and --exact.
: >"$TEMP/cargo.log"
run query_rows_at_lowers_filters_against_historical_current_rows
grep -F -- 'node::tests::harness::query_rows_at_lowers_filters_against_historical_current_rows -- --exact' "$TEMP/cargo.log" >/dev/null

if run --exact db::tests::round; then
  echo 'expected exact non-match to fail' >&2
  exit 1
fi

: >"$TEMP/cargo.log"
if run query::evaluate::wrong_path; then
  echo 'expected plausible wrong module path to fail before test run' >&2
  exit 1
fi
[[ "$(wc -l <"$TEMP/cargo.log")" -eq 1 ]]

: >"$TEMP/cargo.log"
if run db::tests; then
  echo 'expected ambiguous test filter to fail before test run' >&2
  exit 1
fi
[[ "$(wc -l <"$TEMP/cargo.log")" -eq 1 ]]

: >"$TEMP/cargo.log"
run --test incremental_delivery_canary maintained_relation -- --nocapture
grep -F -- '--test incremental_delivery_canary -- --list' "$TEMP/cargo.log" >/dev/null
grep -F -- '--test incremental_delivery_canary maintained_relation::smoke -- --nocapture --exact' "$TEMP/cargo.log" >/dev/null

if MOCK_INVENTORY_STATUS=17 run round_trips; then
  echo 'expected inventory failure to be preserved' >&2
  exit 1
else
  [[ "$?" -eq 17 ]]
fi

if MOCK_RUN_STATUS=23 run round_trips; then
  echo 'expected test failure to be preserved' >&2
  exit 1
else
  [[ "$?" -eq 23 ]]
fi

echo 'dev/t focused test wrapper checks passed'

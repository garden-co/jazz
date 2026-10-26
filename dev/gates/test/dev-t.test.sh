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
  if [[ " $* " == *" --test integration "* ]]; then
    printf '%s\n' 'include_live::renaming_a_sender_updates_exactly_their_messages: test'
  else
    printf '%s\n' \
      'db::tests::round_trips: test' \
      'db::tests::other: test' \
      'ids::tests::row_author_rejects_noncanonical_system_origin: test'
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

# db:: belongs to the jazz-db layer crate, so dev/t selects it without -p.
run db::tests::round
grep -F -- '-p jazz-db --no-default-features --features testing,transport-compression-zstd --lib -- --list' "$TEMP/cargo.log" >/dev/null
grep -F -- '-p jazz-db --no-default-features --features testing,transport-compression-zstd --lib db::tests::round_trips -- --exact' "$TEMP/cargo.log" >/dev/null

# A filter naming no extracted layer module stays on jazz.
: >"$TEMP/cargo.log"
run round_trips
grep -F -- '-p jazz --no-default-features --features testing,transport-compression-zstd --lib -- --list' "$TEMP/cargo.log" >/dev/null

: >"$TEMP/cargo.log"
run --exact db::tests::round_trips
grep -F -- 'db::tests::round_trips -- --exact' "$TEMP/cargo.log" >/dev/null

# -p selects a layer crate, and a distinctive suffix still resolves to the
# canonical name that dev/t passes to Cargo with --exact.
: >"$TEMP/cargo.log"
run -p jazz-types row_author_rejects_noncanonical_system_origin
grep -F -- '-p jazz-types --no-default-features --features testing --lib ids::tests::row_author_rejects_noncanonical_system_origin -- --exact' "$TEMP/cargo.log" >/dev/null

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
run --test integration include_live::renaming_a_sender -- --nocapture
grep -F -- '--test integration -- --list' "$TEMP/cargo.log" >/dev/null
grep -F -- '--test integration include_live::renaming_a_sender_updates_exactly_their_messages -- --nocapture --exact' "$TEMP/cargo.log" >/dev/null

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

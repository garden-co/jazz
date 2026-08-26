#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT/dev/scripts/publish-crates-alpha.sh"
TEMP="$(mktemp -d "${TMPDIR:-/tmp}/publish-crates-alpha-test.XXXXXX")"
trap 'rm -rf "$TEMP"' EXIT
mkdir "$TEMP/bin"

cat >"$TEMP/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${MOCK_CARGO_LOG:?}"

case "${1:-}" in
  metadata)
    cat "${MOCK_METADATA_FILE:?}"
    ;;
  check)
    ;;
  publish)
    if [[ "${MOCK_PUBLISH_RESULT:-success}" == "already-published" ]]; then
      echo "error: crate jazz-wasm-tracing@3.0.0-alpha.0 already exists on crates.io index" >&2
      exit 101
    fi
    ;;
  *)
    echo "unexpected cargo invocation: $*" >&2
    exit 125
    ;;
esac
EOF
chmod +x "$TEMP/bin/cargo"

cat >"$TEMP/metadata-valid.json" <<'EOF'
{
  "packages": [
    {
      "name": "jazz-wasm-tracing",
      "version": "3.0.0-alpha.0",
      "id": "path+file:///workspace/crates/wasm-tracing#jazz-wasm-tracing@3.0.0-alpha.0",
      "license": "MIT OR Apache-2.0",
      "license_file": null,
      "description": "Tracing subscriber for WebAssembly.",
      "source": null,
      "dependencies": [],
      "targets": [
        {
          "kind": ["lib"],
          "crate_types": ["lib"],
          "name": "wasm_tracing",
          "src_path": "/workspace/crates/wasm-tracing/src/lib.rs",
          "edition": "2021",
          "doc": true,
          "doctest": true,
          "test": true
        }
      ],
      "features": {},
      "manifest_path": "/workspace/crates/wasm-tracing/Cargo.toml",
      "metadata": null,
      "publish": null,
      "authors": [],
      "categories": [],
      "keywords": [],
      "readme": "README.md",
      "repository": "https://github.com/dsgallups/wasm-tracing",
      "homepage": null,
      "documentation": "https://docs.rs/wasm_tracing",
      "edition": "2021",
      "links": null,
      "default_run": null,
      "rust_version": null
    }
  ],
  "workspace_members": [
    "path+file:///workspace/crates/wasm-tracing#jazz-wasm-tracing@3.0.0-alpha.0"
  ],
  "workspace_default_members": [
    "path+file:///workspace/crates/wasm-tracing#jazz-wasm-tracing@3.0.0-alpha.0"
  ],
  "resolve": null,
  "target_directory": "/workspace/target",
  "version": 1,
  "workspace_root": "/workspace",
  "metadata": null
}
EOF

cat >"$TEMP/metadata-missing.json" <<'EOF'
{
  "packages": [
    {
      "name": "jazz-cli",
      "version": "0.1.0",
      "id": "path+file:///workspace/crates/cli#jazz-cli@0.1.0",
      "license": "MIT",
      "license_file": null,
      "description": "Jazz command-line tools.",
      "source": null,
      "dependencies": [],
      "targets": [],
      "features": {},
      "manifest_path": "/workspace/crates/cli/Cargo.toml",
      "metadata": null,
      "publish": [],
      "authors": [],
      "categories": [],
      "keywords": [],
      "readme": null,
      "repository": null,
      "homepage": null,
      "documentation": null,
      "edition": "2021",
      "links": null,
      "default_run": null,
      "rust_version": null
    }
  ],
  "workspace_members": ["path+file:///workspace/crates/cli#jazz-cli@0.1.0"],
  "workspace_default_members": ["path+file:///workspace/crates/cli#jazz-cli@0.1.0"],
  "resolve": null,
  "target_directory": "/workspace/target",
  "version": 1,
  "workspace_root": "/workspace",
  "metadata": null
}
EOF

sed 's/3\.0\.0-alpha\.0/3.0.0-alpha.1/g' \
  "$TEMP/metadata-valid.json" >"$TEMP/metadata-wrong-version.json"

fail() {
  echo "$1" >&2
  exit 1
}

run_case() {
  local name="$1"
  local metadata="$2"
  local mode="$3"
  local publish_result="${4:-success}"

  CASE_LOG="$TEMP/$name.cargo.log"
  CASE_OUTPUT="$TEMP/$name.output"
  : >"$CASE_LOG"

  set +e
  PATH="$TEMP/bin:$PATH" \
    MOCK_CARGO_LOG="$CASE_LOG" \
    MOCK_METADATA_FILE="$metadata" \
    CARGO_NET_OFFLINE=true \
    CARGO_REGISTRY_TOKEN="test-token-never-sent" \
    MOCK_PUBLISH_RESULT="$publish_result" \
    "$SCRIPT" "$mode" >"$CASE_OUTPUT" 2>&1
  CASE_STATUS=$?
  set -e

  if grep -F 'jazz-tools' "$CASE_LOG" "$CASE_OUTPUT" >/dev/null; then
    fail "$name selected stale jazz-tools"
  fi
}

assert_log() {
  local name="$1"
  shift
  local expected="$TEMP/$name.expected"
  printf '%s\n' "$@" >"$expected"
  if ! cmp -s "$expected" "$CASE_LOG"; then
    echo "$name used unexpected Cargo calls:" >&2
    diff -u "$expected" "$CASE_LOG" >&2 || true
    exit 1
  fi
}

run_case missing-package "$TEMP/metadata-missing.json" dry-run
[[ "$CASE_STATUS" -ne 0 ]] || fail 'missing package metadata must fail'
assert_log missing-package 'metadata --no-deps --format-version 1'

run_case wrong-version "$TEMP/metadata-wrong-version.json" dry-run
[[ "$CASE_STATUS" -ne 0 ]] || fail 'wrong package version must fail'
assert_log wrong-version 'metadata --no-deps --format-version 1'

run_case valid-dry-run "$TEMP/metadata-valid.json" dry-run
[[ "$CASE_STATUS" -eq 0 ]] || {
  cat "$CASE_OUTPUT" >&2
  fail 'valid dry-run must succeed'
}
assert_log valid-dry-run \
  'metadata --no-deps --format-version 1' \
  'publish --allow-dirty --dry-run -p jazz-wasm-tracing'

run_case valid-publish "$TEMP/metadata-valid.json" publish
[[ "$CASE_STATUS" -eq 0 ]] || {
  cat "$CASE_OUTPUT" >&2
  fail 'valid publish must succeed'
}
assert_log valid-publish \
  'metadata --no-deps --format-version 1' \
  'publish --allow-dirty -p jazz-wasm-tracing'

run_case already-published "$TEMP/metadata-valid.json" publish already-published
[[ "$CASE_STATUS" -eq 0 ]] || {
  cat "$CASE_OUTPUT" >&2
  fail 'already-published crate must be skipped successfully'
}
assert_log already-published \
  'metadata --no-deps --format-version 1' \
  'publish --allow-dirty -p jazz-wasm-tracing'
grep -F 'already published, skipping' "$CASE_OUTPUT" >/dev/null ||
  fail 'already-published result must be reported as skipped'

echo 'crate alpha publish allowlist checks passed'

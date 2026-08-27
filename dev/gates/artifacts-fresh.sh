#!/usr/bin/env bash
# Verify that the artifacts consumed by local tooling match this exact checkout.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for required in find node git; do
  command -v "$required" >/dev/null 2>&1 || {
    echo "artifacts-fresh: required command not found: $required" >&2
    exit 127
  }
done

failed=0

newest_artifact() {
  local path candidate newest=""
  for path in "$@"; do
    if [[ -f "$path" ]]; then
      if [[ -z "$newest" || "$path" -nt "$newest" ]]; then
        newest="$path"
      fi
    elif [[ -d "$path" ]]; then
      while IFS= read -r candidate; do
        if [[ -z "$newest" || "$candidate" -nt "$newest" ]]; then
          newest="$candidate"
        fi
      done < <(find "$path" -type f -print 2>/dev/null)
    fi
  done
  [[ -n "$newest" ]] || return 1
  printf '%s\n' "$newest"
}

check_layer() {
  local layer="$1"
  local fix="$2"
  shift 2
  local artifact newest_source

  for source_root in "${source_roots[@]}"; do
    [[ -e "$source_root" ]] || {
      echo "STALE $layer: cannot inspect source input $source_root. Fix: $fix" >&2
      failed=1
      return
    }
  done

  artifact="$(newest_artifact "$@")" || {
    echo "STALE $layer: artifact is missing. Fix: $fix" >&2
    failed=1
    return
  }
  [[ -n "$artifact" ]] || {
    echo "STALE $layer: artifact is empty. Fix: $fix" >&2
    failed=1
    return
  }

  newest_source="$(find "${source_roots[@]}" -type f -newer "$artifact" -print -quit 2>/dev/null || true)"
  if [[ -n "$newest_source" ]]; then
    echo "STALE $layer: $newest_source is newer than $artifact. Fix: $fix" >&2
    failed=1
  else
    echo "FRESH $layer: $artifact"
  fi
}

# Cargo packages have local path dependencies. Listing them explicitly keeps this
# check useful without declaring unrelated workspace changes stale.
source_roots=(packages/jazz-tools/src packages/jazz-tools/scripts packages/jazz-tools/package.json packages/jazz-tools/svelte.config.js packages/jazz-tools/tsconfig.json packages/jazz-tools/tsconfig.react-native-tests.json packages/jazz-tools/tsconfig.solid.json packages/jazz-tools/tsconfig.svelte.json packages/jazz-tools/tsconfig.tests.json packages/jazz-tools/vite.config.solid.ts packages/jazz-tools/vitest.config.browser.ts packages/jazz-tools/vitest.config.react.ts packages/jazz-tools/vitest.config.solid.ts packages/jazz-tools/vitest.config.svelte.ts packages/jazz-tools/vitest.config.ts pnpm-lock.yaml package.json)
check_layer \
  "packages/jazz-tools/dist" \
  "pnpm --filter jazz-tools build" \
  packages/jazz-tools/dist

source_roots=(crates/jazz-cli/src crates/jazz-cli/Cargo.toml crates/jazz/src crates/groove/src crates/jazz/Cargo.toml crates/groove/Cargo.toml Cargo.toml Cargo.lock)
check_layer \
  "target/debug/jazz-tools" \
  "cargo build -p jazz-cli --bin jazz-tools" \
  target/debug/jazz-tools

# napi-rs's generated loader/declarations live in an immutable staged
# generation; the tracked CJS/ESM wrappers are ABI inputs.
source_roots=(crates/jazz-napi/src crates/jazz-napi/build.rs crates/jazz-napi/Cargo.toml crates/jazz-napi/package.json crates/jazz-napi/index.cjs crates/jazz-napi/index.mjs crates/jazz-napi/index.d.ts crates/jazz-napi/scripts crates/jazz-otel/src crates/jazz-otel/Cargo.toml crates/jazz/src crates/groove/src crates/jazz/Cargo.toml crates/groove/Cargo.toml Cargo.toml Cargo.lock)
shopt -s nullglob
napi_artifacts=(crates/jazz-napi/.native-artifacts/*/*.node)
shopt -u nullglob
check_layer \
  "crates/jazz-napi/.native-artifacts/*/*.node" \
  "pnpm --filter jazz-napi build:debug" \
  "${napi_artifacts[@]}"

if ! node dev/artifacts/provenance.mjs verify napi debug; then
  echo "Fix: pnpm --filter jazz-napi build:debug" >&2
  failed=1
fi

check_layer \
  "crates/jazz-wasm/pkg" \
  "pnpm --filter jazz-wasm build" \
  crates/jazz-wasm/pkg

if ! node dev/artifacts/provenance.mjs verify wasm release; then
  echo "Fix: pnpm --filter jazz-wasm build" >&2
  failed=1
fi

exit "$failed"

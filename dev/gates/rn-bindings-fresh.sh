#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

case "$(uname -s)" in
  Darwin)
    library_extension="dylib"
    ;;
  Linux)
    library_extension="so"
    ;;
  *)
    echo "React Native binding freshness is supported on macOS and Linux hosts" >&2
    exit 1
    ;;
esac

cargo build -p jazz-rn

target_dir="${CARGO_TARGET_DIR:-target}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi

library="$target_dir/debug/libjazz_rn.$library_extension"
if [[ ! -f "$library" ]]; then
  echo "jazz-rn host library was not produced at $library" >&2
  exit 1
fi

ubrn="$repo_root/crates/jazz-rn/node_modules/.bin/ubrn"
if [[ ! -x "$ubrn" ]]; then
  echo "uniffi-bindgen-react-native is not installed; run pnpm install" >&2
  exit 1
fi

generated_dir="$(mktemp -d "$repo_root/crates/jazz-rn/.bindings-check.XXXXXX")"
trap 'rm -rf "$generated_dir"' EXIT
mkdir -p "$generated_dir/ts" "$generated_dir/cpp"

"$ubrn" generate jsi bindings "$library" \
  --library \
  --ts-dir "$generated_dir/ts" \
  --cpp-dir "$generated_dir/cpp"

bindings_are_fresh=true
for generated_file in jazz_rn.ts jazz_rn-ffi.ts; do
  committed="$repo_root/crates/jazz-rn/src/generated/$generated_file"
  regenerated="$generated_dir/ts/$generated_file"

  if ! cmp -s "$committed" "$regenerated"; then
    echo "Committed React Native binding is stale: $generated_file" >&2
    diff -u "$committed" "$regenerated" || true
    bindings_are_fresh=false
  fi
done

if [[ "$bindings_are_fresh" != true ]]; then
  echo "Regenerate with pnpm --filter jazz-rn ubrn:ios or ubrn:android and commit the generated bindings." >&2
  exit 1
fi

echo "React Native bindings are fresh."

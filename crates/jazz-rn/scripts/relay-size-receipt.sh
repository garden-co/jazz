#!/usr/bin/env bash
# Link proxy for the relay's marginal cost in an adopter's iOS app executable:
# link a C main that references every exported jazz_native_relay_* symbol, let
# -dead_strip discard everything unreachable from that public ABI, then report
# the fully stripped and gzipped bytes. This is an upper bound on what the relay
# adds to an app's arm64 slice, not an App Store Connect download/install size.
# ponytail: iOS device slice only; an Android/ELF receipt is a follow-up.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
archive=${1:-$root/target/aarch64-apple-ios/release-mobile/libjazz_native_relay.a}

if [[ "$(uname -s)" != "Darwin" ]] || ! command -v xcrun >/dev/null 2>&1; then
  echo "relay-size-receipt.sh needs macOS with Xcode command line tools (xcrun)" >&2
  exit 1
fi
if [[ ! -f "$archive" ]]; then
  echo "no such archive: $archive (build it with: pnpm --filter jazz-rn build:relay:ios)" >&2
  exit 1
fi

# Xcode's nm cannot parse the LLVM bitcode archive members that a newer rustc
# emits for compiler_builtins; it still lists every Mach-O member's symbols, so
# tolerate its exit status and let the empty-symbol check below catch real
# failures.
symbols=$({ xcrun nm -gU "$archive" 2>/dev/null || true; } | sed -nE 's/^[0-9a-f]+ T _(jazz_native_relay_[A-Za-z0-9_]+)$/\1/p' | sort -u)
if [[ -z "$symbols" ]]; then
  echo "no exported T _jazz_native_relay_* symbols in $archive" >&2
  exit 1
fi
count=$(printf '%s\n' "$symbols" | wc -l | tr -d ' ')

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

{
  echo '#include <stdint.h>'
  printf 'extern void %s(void);\n' $symbols
  echo 'int main(int argc, char **argv) { (void)argv; void *p[] = {'
  printf '(void*)%s,\n' $symbols
  echo "}; return (int)(intptr_t)p[argc % $count]; }"
} > "$tmp/main.c"

xcrun -sdk iphoneos clang -arch arm64 -miphoneos-version-min=15.0 -O2 \
  "$tmp/main.c" "$archive" \
  -framework Security -framework CoreFoundation -lc++ \
  -Wl,-dead_strip -o "$tmp/relay-size-probe"

echo "archive $archive"
echo "archive_bytes $(stat -f %z "$archive")"
echo "exported_symbols $count"

xcrun size -m "$tmp/relay-size-probe" \
  | sed -nE 's/^Segment (__TEXT|__DATA_CONST|__DATA|__LINKEDIT): ([0-9]+).*/segment.\1 \2/p'

xcrun otool -l "$tmp/relay-size-probe" | awk '
  /^  sectname /   { sect = $2 }
  /^   segname /   { seg = $2 }
  /^      size /   { if (seg == "__TEXT") print "section.__TEXT." sect, $2 }
' | while read -r name hex; do echo "$name $((hex))"; done

xcrun strip "$tmp/relay-size-probe"
echo "stripped_executable_bytes $(stat -f %z "$tmp/relay-size-probe")"
gzip -9 -c "$tmp/relay-size-probe" > "$tmp/relay-size-probe.gz"
echo "stripped_executable_gzip9_bytes $(stat -f %z "$tmp/relay-size-probe.gz")"

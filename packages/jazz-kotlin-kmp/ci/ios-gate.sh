#!/usr/bin/env bash
# The KMP spike's iOS gate, on macOS with Xcode (bash 3.2 compatible):
#   ios-gate.sh rust      libjazz_ffi.a for device and simulator, as JazzFFI.xcframework builds it
#   ios-gate.sh klibs     cinterop and klibs compile for both, jazz-ffi inside
#   ios-gate.sh link      binaries link for the device, unsigned
#   ios-gate.sh test      the common tests and LocalAppTest pass on the simulator
#   ios-gate.sh size      what jazz-ffi adds to a linked, stripped iOS binary
# Each fails unless it saw what it claims: a skipped task is not a pass.
set -euo pipefail
pkg=$(cd "$(dirname "$0")/.." && pwd)
repo=$(cd "$pkg/../.." && pwd)
cd "$pkg"
export IPHONEOS_DEPLOYMENT_TARGET=16.0
device=aarch64-apple-ios
simulator=aarch64-apple-ios-sim

fail() { echo "::error::$*"; exit 1; }
say() { echo "$*"; echo "$*" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"; }
kmp() { gradle --no-daemon --console=plain -Dorg.gradle.jvmargs=-Xmx4g -PcargoProfile=release "$@"; }
binary() {
  local found
  found=$(find "build/bin/iosArm64/$1" -maxdepth 1 -type f -name '*.kexe')
  [ -n "$found" ] || fail "no binary in build/bin/iosArm64/$1"
  echo "$found"
}

rust() {
  cd "$repo"
  for target in $device $simulator; do
    cargo rustc --locked -p jazz-ffi --release --lib --target "$target" --crate-type staticlib -- --print native-static-libs
    [ -s "target/$target/release/libjazz_ffi.a" ] || fail "no libjazz_ffi.a for $target"
    say "- built libjazz_ffi.a for $target"
  done
}

klibs() {
  kmp cinteropJazzFfiIosArm64 cinteropJazzFfiIosSimulatorArm64 compileKotlinIosArm64 compileKotlinIosSimulatorArm64
  for target in iosArm64 iosSimulatorArm64; do
    local main=build/classes/kotlin/$target/main included
    [ -e "$main/klib" ] || fail "no klib for $target (compile skipped?)"
    included=$(find "$main/cinterop" -name libjazz_ffi.a -type f)
    if [ -z "$included" ]; then # a packed cinterop klib
      for klib in $(find "$main/cinterop" -name '*.klib' -type f); do
        included="$included$(unzip -l "$klib" | grep 'libjazz_ffi\.a' || true)"
      done
    fi
    [ -n "$included" ] || fail "$target's cinterop klib has no libjazz_ffi.a"
    say "- PASS klibs: $target (cinterop with libjazz_ffi.a, and Kotlin)"
  done
}

link() {
  kmp linkDebugTestIosArm64 linkGateReleaseExecutableIosArm64
  for kind in debugTest gateReleaseExecutable; do
    local bin build
    bin=$(binary "$kind")
    [ "$(xcrun lipo -archs "$bin")" = arm64 ] || fail "$kind isn't arm64"
    build=$(xcrun vtool -show-build "$bin")
    echo "$build"
    grep -Eq 'platform IOS$' <<< "$build" || fail "$kind isn't for an iOS device"
    grep -Eq 'minos 16\.0' <<< "$build" || fail "$kind's minimum iOS isn't 16.0"
    xcrun nm "$bin" > build/symbols.txt
    grep -Eq ' [Tt] _jazz_open$' build/symbols.txt || fail "$kind doesn't contain jazz-ffi (_jazz_open)"
    if grep -E ' U _jazz_' build/symbols.txt; then fail "$kind leaves jazz-ffi unresolved"; fi
    if xcrun otool -L "$bin" | tail -n +2 | grep jazz_ffi; then fail "$kind links jazz-ffi dynamically"; fi
    if codesign -dv "$bin" > build/codesign.txt 2>&1; then
      grep -q 'Signature=adhoc' build/codesign.txt || fail "$kind is signed with an identity"
      say "- PASS device link: $kind (arm64 iOS 16.0, jazz-ffi linked in, only the linker's ad-hoc signature)"
    else
      say "- PASS device link: $kind (arm64 iOS 16.0, jazz-ffi linked in, unsigned)"
    fi
  done
}

simulator_tests() {
  rm -rf build/test-results/iosSimulatorArm64Test
  # A failing test still gets counted and named below; the status decides last.
  local status=0
  kmp iosSimulatorArm64Test --rerun || status=$?
  mkdir -p build
  python3 -I - "$pkg" "$status" > build/simulator-tests.txt <<'PY' || { cat build/simulator-tests.txt; exit 1; }
import pathlib, sys, xml.etree.ElementTree as ET
pkg = pathlib.Path(sys.argv[1])
sources = {"JazzTest": "commonTest", "LocalAppTest": "notesTest"}
expected = {
    name: (pkg / f"src/{src}/kotlin/tools/jazz/{name}.kt").read_text().count("@Test")
    for name, src in sources.items()
}
seen, output, ok = {}, "", True
for report in sorted((pkg / "build/test-results/iosSimulatorArm64Test").glob("*.xml")):
    root = ET.parse(report).getroot()
    name = root.get("name").split(".")[-1]
    cases = root.findall("testcase")
    bad = {k: sum(c.find(k) is not None for c in cases) for k in ("failure", "error", "skipped")}
    seen[name] = len(cases)
    print(f"- simulator {name}: {len(cases)} run, {bad['failure']} failed, {bad['error']} errors, {bad['skipped']} skipped")
    for case in cases:
        print(f"  - {case.get('name')} ({case.get('time')} s)")
    ok = ok and not any(bad.values())
    output += root.findtext("system-out") or ""
if seen != expected or not all(seen.values()):
    print(f"- FAIL: ran {seen}, the sources have {expected}")
    ok = False
# Only the real core says this; FakeNative doesn't check ids.
if "the id must be a UUID" not in output:
    print("- FAIL: no sign of the real jazz-ffi in LocalAppTest's output")
    ok = False
if sys.argv[2] != "0":
    print(f"- FAIL: iosSimulatorArm64Test exited {sys.argv[2]}")
    ok = False
if ok:
    print(f"- PASS simulator: {sum(seen.values())} tests, none skipped, LocalAppTest on the real jazz-ffi")
sys.exit(0 if ok else 1)
PY
  while IFS= read -r line; do say "$line"; done < build/simulator-tests.txt
}

size() {
  mkdir -p build/size
  cp "$(binary gateReleaseExecutable)" build/size/kotlin-jazz
  # In that binary: the bytes the linker kept from libjazz_ffi.a. (A Kotlin
  # baseline can't show this: linking the klib links jazz-ffi, used or not.)
  python3 -I - build/ios-gate-link-map.txt > build/size/map.txt <<'PY'
import re, sys
part, objects, zero_fill, total = None, set(), [], 0
for line in open(sys.argv[1], errors="replace"):
    if line.startswith("#"):
        for header in ("Object files", "Sections", "Symbols", "Dead Stripped Symbols"):
            if line.startswith(f"# {header}:"):
                part = header
        continue
    if part == "Object files":
        found = re.match(r"\[\s*(\d+)\] (.*)", line)
        if found and "libjazz_ffi.a" in found.group(2):
            objects.add(found.group(1))
    elif part == "Sections":
        f = line.split()
        if len(f) >= 4 and f[3] in ("__bss", "__common", "__thread_bss"):
            start = int(f[0], 16)
            zero_fill.append((start, start + int(f[1], 16)))
    elif part == "Symbols":
        found = re.match(r"(0x[0-9A-Fa-f]+)\s+(0x[0-9A-Fa-f]+)\s+\[\s*(\d+)\]", line)
        if found and found.group(3) in objects:
            at = int(found.group(1), 16)
            if not any(start <= at < end for start, end in zero_fill):
                total += int(found.group(2), 16)
if not total:
    sys.exit("the linker map names nothing from libjazz_ffi.a")
print(f"- in the Kotlin binary, {total} bytes come from libjazz_ffi.a (linker map, without zero-fill)")
PY
  say "$(cat build/size/map.txt)"
  # jazz-ffi alone: an empty C program, and one that references exactly the
  # C functions CAbiNative imports.
  local functions flags name
  functions=$(sed -n 's/^import tools\.jazz\.cabi\.\(jazz_[a-z_]*\)$/\1/p' src/nativeMain/kotlin/tools/jazz/CAbiNative.kt)
  [ -n "$functions" ] || fail "no C functions found in CAbiNative.kt"
  echo 'int main(void) { return 0; }' > build/size/empty.c
  {
    echo '#include "jazz_ffi.h"'
    echo '__attribute__((used)) void *const used_functions[] = {'
    for name in $functions; do echo "  (void *)&$name,"; done
    echo '};'
    echo 'int main(void) { return 0; }'
  } > build/size/jazz.c
  flags=$(sed -n 's/^linkerOpts\.ios = //p' src/nativeInterop/cinterop/jazz_ffi.def)
  local cc=(xcrun --sdk iphoneos clang -target arm64-apple-ios16.0 -Os -Wl,-dead_strip)
  "${cc[@]}" build/size/empty.c -o build/size/c-empty
  # $flags is a list of linker arguments, split on purpose.
  # shellcheck disable=SC2086
  "${cc[@]}" -I "$repo/crates/jazz-ffi/include" build/size/jazz.c "$repo/target/$device/release/libjazz_ffi.a" $flags -o build/size/c-jazz
  for name in c-empty c-jazz kotlin-jazz; do
    xcrun strip "build/size/$name"
    codesign --remove-signature "build/size/$name" || true
    say "- stripped $name: $(stat -f %z "build/size/$name") bytes, $(gzip -6 -c "build/size/$name" | wc -c | tr -d ' ') deflated"
  done
  bytes() { stat -f %z "build/size/$1"; }
  say "- jazz-ffi adds $(($(bytes c-jazz) - $(bytes c-empty))) bytes to a stripped iOS binary"
  say "- libjazz_ffi.a ($device): $(stat -f %z "$repo/target/$device/release/libjazz_ffi.a") bytes, $(gzip -6 -c "$repo/target/$device/release/libjazz_ffi.a" | wc -c | tr -d ' ') deflated"
}

case "${1:-}" in
  rust | klibs | link | size) "$1" ;;
  test) simulator_tests ;;
  *) echo "usage: $0 rust|klibs|link|test|size" >&2; exit 2 ;;
esac

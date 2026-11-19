#!/usr/bin/env bash
# Verifies an assembled jazz-swift mirror (assemble-swift-mirror.sh) against a
# local JazzFFI.xcframework.zip, the checks the Swift mirror workflow runs
# before publishing. The release asset doesn't exist yet: the checks use a
# copy of the mirror whose binary target is the unzipped local XCFramework.
# Usage: verify-swift-mirror.sh <mirror> <xcframework.zip> <work> <check>...
# Checks (each runs on its own; they can run in parallel jobs):
#   smoke          a consumer package builds and runs Smoke.swift on macOS
#   test-macos     the package's tests (packages/jazz-swift/Tests) on macOS
#   test-ios       the same tests on the newest available iPhone simulator
#   example-macos  examples/stage-plan/apps/swiftui builds for macOS
#   example-ios    its UI test on the iPhone simulator
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
mirror=$1 zip=$2 work=$3
shift 3
verify="$work/verify"
example="$root/examples/stage-plan/apps/swiftui"

prepare() {
  rm -rf "$verify" && mkdir -p "$work"
  cp -R "$mirror" "$verify"
  (cd "$verify" && unzip -q "$zip" \
    && perl -0pi -e 's/\.binaryTarget\(\s*name: "jazz_ffiFFI", url: "[^"]*", checksum: "[^"]*"\)/.binaryTarget(name: "jazz_ffiFFI", path: "JazzFFI.xcframework")/' Package.swift)
  grep -q 'path: "JazzFFI.xcframework"' "$verify/Package.swift" || { echo "::error::Package.swift's binary target wasn't swapped for the local XCFramework"; exit 1; }
  cp -R "$root/packages/jazz-swift/Tests" "$verify/Tests"
  perl -0pi -e 's/(\n        jazzFFI,\n    \])/\n        jazzFFI,\n        .testTarget(name: "JazzTests", dependencies: ["Jazz"], path: "Tests\/JazzTests"),\n    ]/' "$verify/Package.swift"
  grep -q testTarget "$verify/Package.swift" || { echo "::error::no test target added to Package.swift"; exit 1; }
}

# The newest available iPhone simulator.
simulator() {
  xcrun simctl list devices available -j | python3 -c '
import json, re, sys
def version(runtime):
    return [int(n) for n in re.findall(r"\d+", runtime.split("iOS")[-1])]
runtimes = sorted((r for r in json.load(sys.stdin)["devices"].items() if "iOS" in r[0]), key=lambda r: version(r[0]))
phones = [d for _, ds in runtimes for d in ds if d["name"].startswith("iPhone")]
if not phones:
    sys.exit("no available iPhone simulator")
print(phones[-1]["udid"])'
}

# examples/stage-plan/apps/swiftui depends on garden-co/jazz-swift; before
# publishing, that's the local copy of the mirror.
example() {
  command -v xcodegen >/dev/null || brew install xcodegen
  if ! grep -q "path: $verify" "$example/project.yml"; then
    perl -0pi -e "s|    url: https://github.com/garden-co/jazz-swift\n    exactVersion: [^\n]*|    path: $verify|" "$example/project.yml"
  fi
  # Fail rather than test the published package if the swap didn't match.
  grep -q "path: $verify" "$example/project.yml" || { echo "::error::project.yml still names the published package"; exit 1; }
  (cd "$example" && xcodegen generate)
}

prepare
for check in "$@"; do
  echo "::group::$check"
  case "$check" in
    smoke)
      consumer="$work/consumer"
      rm -rf "$consumer" && mkdir -p "$consumer/Sources/Smoke"
      cat > "$consumer/Package.swift" <<SWIFT
// swift-tools-version:6.0
import PackageDescription
let package = Package(
    name: "smoke",
    platforms: [.macOS(.v13), .iOS(.v16)],
    dependencies: [.package(path: "$verify")],
    targets: [.executableTarget(name: "Smoke", dependencies: [.product(name: "Jazz", package: "verify")])]
)
SWIFT
      cp "$root/packages/jazz-swift/mirror/Smoke.swift" "$consumer/Sources/Smoke/Smoke.swift"
      (cd "$consumer" && swift run Smoke) ;;
    test-macos)
      (cd "$verify" && swift test) ;;
    test-ios)
      udid=$(simulator); echo "iOS simulator: $udid"
      (cd "$verify" && xcodebuild test -scheme jazz-swift -destination "platform=iOS Simulator,id=$udid" -quiet) ;;
    example-macos)
      example
      (cd "$example" && xcodebuild build -scheme StagePlanUI -destination 'platform=macOS' -quiet CODE_SIGNING_ALLOWED=NO) ;;
    example-ios)
      example
      udid=$(simulator); echo "iOS simulator: $udid"
      (cd "$example" && xcodebuild test -scheme StagePlanUI -destination "platform=iOS Simulator,id=$udid" -quiet CODE_SIGNING_ALLOWED=NO) ;;
    *) echo "unknown check: $check" >&2; exit 64 ;;
  esac
  echo "::endgroup::"
done

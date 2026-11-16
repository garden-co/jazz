#!/usr/bin/env bash
# Assembles the garden-co/jazz-swift mirror tree for VERSION into OUT:
# Package.swift (from packages/jazz-swift/mirror/Package.swift.template, the
# XCFramework's URL and checksum filled in) and the package's sources.
# Usage: assemble-swift-mirror.sh <version> <xcframework-url> <checksum> <out>
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
version=$1 url=$2 checksum=$3 out=$4
pkg="$root/packages/jazz-swift"
rm -rf "$out" && mkdir -p "$out/Sources"
cp -R "$pkg/Sources/Jazz" "$pkg/Sources/JazzFFI" "$pkg/Sources/jazz_ffiFFI" "$out/Sources/"
sed -e "s|@VERSION@|$version|g" -e "s|@URL@|$url|g" -e "s|@CHECKSUM@|$checksum|g" \
  "$pkg/mirror/Package.swift.template" > "$out/Package.swift"
cp "$root/LICENSE" "$out/LICENSE"
cat > "$out/README.md" <<README
# Jazz for Swift $version

Published from [garden-co/jazz](https://github.com/garden-co/jazz) (packages/jazz-swift);
change it there. See https://jazz.tools/docs/setup/swift.

\`\`\`swift
.package(url: "https://github.com/garden-co/jazz-swift", exact: "$version")
\`\`\`
README
echo "assembled the jazz-swift mirror for $version in $out"

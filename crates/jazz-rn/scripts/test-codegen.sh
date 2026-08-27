#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

# package.json is the one canonical codegen contract for a published RN
# library. The Gradle plugin reads it when it generates the Android base class;
# keep this receipt on exactly that invocation rather than checking the
# generic, app-oriented generate-codegen-artifacts output.
android_package=$(node -e 'console.log(require(process.argv[1]).codegenConfig.android.javaPackageName)' "$root/package.json")
library_name=$(node -e 'console.log(require(process.argv[1]).codegenConfig.name)' "$root/package.json")
codegen_combine_cli=$(node -e '
  const path = require("path");
  const reactNative = require.resolve("react-native/package.json", { paths: [process.argv[1]] });
  console.log(require.resolve("@react-native/codegen/lib/cli/combine/combine-js-to-schema-cli.js", {
    paths: [path.dirname(reactNative)],
  }));
' "$root")

for platform in android ios; do
  if ! output=$(node "$root/node_modules/react-native/scripts/generate-codegen-artifacts.js" \
    --path "$root" \
    --outputPath "$output_dir/$platform" \
    --targetPlatform "$platform" 2>&1); then
    printf '%s\n' "$output" >&2
    echo "React Native Codegen exited unsuccessfully for $platform" >&2
    exit 1
  fi
  printf '%s\n' "$output"
  if [[ "$output" == *"Error:"* ]] || [[ "$output" == *"Unsupported"* ]]; then
    echo "React Native Codegen reported an error for $platform" >&2
    exit 1
  fi
  if ! rg -q 'NativeJazzRelay' "$output_dir/$platform"; then
    echo "React Native Codegen did not generate the JazzRelay module for $platform" >&2
    exit 1
  fi
  if [[ "$platform" == android ]] && ! rg -q 'getAbiVersion|execute' "$output_dir/$platform"; then
    echo "React Native Codegen did not generate the JazzRelay command methods for Android" >&2
    exit 1
  fi
done

# This is the same schema-to-spec invocation made by the React Native Gradle
# plugin for an Android library. It proves that the Java implementation imports
# the generated class that an application-linked AAR will actually compile.
mkdir -p "$output_dir/android-gradle"
node "$codegen_combine_cli" \
  --platform android \
  --exclude NativeSampleTurboModule \
  "$output_dir/android-gradle/schema.json" \
  "$root/src"
node "$root/node_modules/react-native/scripts/generate-specs-cli.js" \
  --platform android \
  --schemaPath "$output_dir/android-gradle/schema.json" \
  --outputDir "$output_dir/android-gradle" \
  --libraryName "$library_name" \
  --javaPackageName "$android_package"

generated_spec="$output_dir/android-gradle/java/${android_package//./\/}/NativeJazzRelaySpec.java"
if [[ ! -f "$generated_spec" ]] \
  || ! rg -q "package ${android_package//./\\.};" "$generated_spec" \
  || ! rg -q 'class NativeJazzRelaySpec' "$generated_spec"; then
  echo "React Native Gradle codegen did not generate the NativeJazzRelaySpec Java base class in the package declared by jazz-rn" >&2
  exit 1
fi

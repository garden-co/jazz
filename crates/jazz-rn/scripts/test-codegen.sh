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
  if [[ "$platform" == android ]] && ! rg -q 'getAbiVersion|installForegroundRuntime|execute' "$output_dir/$platform"; then
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

# The generated spec's package is a compile-time part of the Android module
# ABI, not merely codegen metadata. Keep both handwritten Android sources in
# that same package: the Java TurboModule extends the generated base directly,
# and the Kotlin autolinking package instantiates that Java implementation.
for source in \
  "$root/android/src/main/java/com/jazzrn/JazzRelayModule.java" \
  "$root/android/src/main/java/com/jazzrn/JazzRelayPackage.kt"; do
  source_package=$(sed -nE 's/^package ([A-Za-z0-9_.]+);?$/\1/p' "$source")
  if [[ "$source_package" != "$android_package" ]]; then
    echo "Android relay source $source is not in the package declared by jazz-rn codegen ($android_package)" >&2
    exit 1
  fi
done

# React-Codegen makes JazzRelaySpec.h visible while compiling the JazzRn pod,
# not to an application that imports <JazzRn/JazzRelay.h>. Keep that generated
# protocol behind the pod's private header so a normal public import cannot
# fail before the consumer reaches any Jazz code.
public_header="$root/ios/JazzRelay.h"
private_header="$root/ios/JazzRelayModule.h"
podspec="$root/JazzRn.podspec"
if rg -q '#import "JazzRelaySpec\.h"|NativeJazzRelaySpec|@interface JazzRelay[[:space:]]*:' "$public_header"; then
  echo "JazzRn public header leaks the pod-target-only JazzRelay generated spec" >&2
  exit 1
fi
if ! rg -q '#import "JazzRelaySpec\.h"' "$private_header" \
  || ! rg -q '@interface JazzRelay : NSObject <NativeJazzRelaySpec(?:, RCTTurboModuleWithJSIBindings)?>' "$private_header"; then
  echo "JazzRn private TurboModule header no longer owns the generated spec contract" >&2
  exit 1
fi
if ! rg -q 's\.public_header_files = "ios/JazzRelay\.h"' "$podspec" \
  || ! rg -q 's\.private_header_files = "ios/JazzRelayModule\.h"' "$podspec"; then
  echo "JazzRn podspec does not publish only the generated-header-free host surface" >&2
  exit 1
fi

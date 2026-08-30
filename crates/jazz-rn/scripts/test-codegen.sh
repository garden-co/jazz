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

# JSI bindings are installed by React Native itself when this TurboModule is
# resolved, including when it is lazily initialized. Keep the foreground
# factory on that standard lifecycle on both platforms: an ordinary generated
# method cannot safely rediscover the current JSI runtime after JavaScript has
# discarded the bindings-installed factory.
android_module="$root/android/src/main/java/com/jazzrn/JazzRelayModule.java"
android_package_source="$root/android/src/main/java/com/jazzrn/JazzRelayPackage.kt"
ios_module="$root/ios/JazzRelayModule.h"
ios_implementation="$root/ios/JazzRelay.mm"
if ! rg -q 'implements TurboModuleWithJSIBindings' "$android_module" \
  || ! rg -q 'getBindingsInstaller\(\)' "$android_module" \
  || ! rg -q 'nativeForegroundBindingsInstaller' "$root/android/cpp-relay.cpp" \
  || rg -q -- 'installation->runtime' "$root/android/cpp-relay.cpp" \
  || rg -q 'installForegroundRuntime' "$android_module" "$android_package_source" "$root/src/NativeJazzRelay.ts"; then
  echo "Android foreground factory no longer uses React Native's JSI bindings lifecycle exclusively" >&2
  exit 1
fi

# Pin the assumption behind lazy module registration to the installed React
# Native implementation. Its normal module lookup must invoke and apply a JSI
# bindings installer before caching/returning a Java TurboModule; eager init is
# therefore unnecessary and must not become a hidden lifecycle dependency.
react_native_root=$(node -e 'console.log(require.resolve("react-native/package.json", { paths: [process.argv[1]] }).replace(/\/package\.json$/, ""))' "$root")
turbo_manager="$react_native_root/ReactAndroid/src/main/jni/react/turbomodule/ReactCommon/TurboModuleManager.cpp"
if ! rg -Uq 'getTurboJavaModule[\s\S]*JTurboModuleWithJSIBindings[\s\S]*getBindingsInstaller[\s\S]*installBindings\(runtime, jsCallInvoker_\)' "$turbo_manager" \
  || ! rg -q 'false,[[:space:]]*// needsEagerInit' "$android_package_source"; then
  echo "Android lazy TurboModule lookup no longer proves foreground JSI binding installation" >&2
  exit 1
fi
if ! rg -q 'RCTTurboModuleWithJSIBindings' "$ios_module" \
  || ! rg -q 'installJSIBindingsWithRuntime:' "$ios_implementation" \
  || rg -q -- '- \(void\)installForegroundRuntime' "$ios_implementation"; then
  echo "iOS foreground factory no longer uses React Native's JSI bindings lifecycle exclusively" >&2
  exit 1
fi

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

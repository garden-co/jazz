# React Native device acceptance harness

This is the first-party **Expo development-build / bare-host** acceptance app for the native relay. It is intentionally not an Expo Go test: native relay code and the trusted fixture must be compiled into the Android APK or iOS simulator app.

The app has one durable native relay and a scenario plan requiring two UI runtimes. It emits newline-delimited `JAZZ_DEVICE_RESULT {json}` protocol messages automatically on launch. A `passed` state is rejected by the protocol unless it includes platform, device, build, and observation-time evidence. The linked ABI plus trusted-native-admission probe is the sole implemented receipt; every multi-peer or durable scenario remains truthful `todo`, so the full acceptance gate correctly remains red.

## Trusted fixture boundary

`native/android/JazzDeviceFixtureModule.kt` and `native/ios/JazzDeviceFixture.mm` admit a scope entirely in trusted platform code. The relay's opaque 32-byte capability is the only value permitted into JavaScript. The iOS fixture uses fixed non-secret test material and reads the launch nonce, build fingerprint, and simulator UDID only from native process arguments; they must never be populated by Metro variables, intents, a remote config service, or an OTA update. `src/native-fixture.ts` is the narrow JS consumer.

The Expo config plugin copies and registers the Android template during prebuild; its public placeholder `BuildConfig` values make that host compile-shaped without embedding credentials. The iOS Blacksmith job stages the XCFramework, prebuilds, installs pods, registers the fixture, builds, installs, and launches the simulator app. It is a real device receipt gate, but is intentionally incomplete until every TODO scenario is implemented.

## Current acceptance plan

- `linked-abi-admission`: automatic iOS and Android receipt that verifies the embedded ABI and receives a 32-byte capability from the trusted native admission boundary.
- `local-write-subscription`: UI-A write observed by UI-B through one relay.
- `reconnect` and `reopen`: connection recovery and durable process/app relaunch.
- `scope-isolation` and `logout-auth-switch`: separate scope visibility and revocation before replacement.
- `backpressure` and `corrupt-store`: bounded frame recovery and fail-closed storage diagnostics.

Run source checks with `pnpm --filter rn-device-acceptance verify`. After a real development build exists, the drivers require `JAZZ_DEVICE_APK` (Android) or `IOS_SIMULATOR_UDID`, `JAZZ_DEVICE_APP`, and immutable `JAZZ_DEVICE_BUILD_FINGERPRINT` (iOS). Each launch receives a fresh nonce and requires exactly one, fresh, strictly ordered receipt for every implemented scenario, bound to that platform/device/build/nonce. They reject TODO/blocked/failed, duplicate, unknown, stale, partial, or foreign receipts; a green process cannot be manufactured by a fixture log line.

## Local Android bootstrap

The reproducible, lane-local tool cache is `.cache/android-device-acceptance`:

```bash
dev/rn-device-acceptance/scripts/bootstrap-android.sh --emulator
export JAZZ_DEVICE_TOOLCACHE="$PWD/.cache/android-device-acceptance"
export JAVA_HOME="$JAZZ_DEVICE_TOOLCACHE/jdk"
export ANDROID_SDK_ROOT="$JAZZ_DEVICE_TOOLCACHE/sdk"
export PATH="$JAZZ_DEVICE_TOOLCACHE/cargo/bin:$ANDROID_SDK_ROOT/platform-tools:$ANDROID_SDK_ROOT/emulator:$PATH"
JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION=4.1.2 pnpm --filter jazz-rn build:relay:android
export JAZZ_DEVICE_RELAY_SOURCE_REVISION="$(git rev-parse HEAD)"
JAZZ_DEVICE_APK="$PWD/dev/rn-device-acceptance/android/app/build/outputs/apk/release/app-release.apk" \
  NODE_ENV=production pnpm --filter rn-device-acceptance device:android
```

It pins Temurin 17.0.16+8, command-line tools 13114758, NDK 27.1.12297006,
Android API/build tools 36, cargo-ndk 4.1.2, and (with `--emulator`) the API 35
Google APIs x86_64 image. The cache is ignored and never needs system Java,
Android SDK, adb, or a global cargo-ndk installation. `device:android` compares
the fixture's immutable `Build.FINGERPRINT` with adb's `ro.build.fingerprint`;
it also rejects a staged relay unless its manifest names the supplied source
revision and exactly the arm64-v8a, armeabi-v7a, x86, and x86_64 static-library
slices with matching hashes. The receipt's artifact fingerprint is SHA-256
computed by trusted Android code from its installed `applicationInfo.sourceDir`;
the driver independently hashes that same package-manager path after install.
Neither is a host-supplied intent echo. App-scoped Android IDs and adb transport
serials are intentionally not used. Bootstrap checks pinned SHA-256 values for
the Temurin, Android command-line-tools, and NDK archives and fails closed on a
corrupt pre-existing cache archive.

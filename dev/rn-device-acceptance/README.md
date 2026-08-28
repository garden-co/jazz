# React Native device acceptance harness

This is the first-party **Expo development-build / bare-host** acceptance app for the native relay. It is intentionally not an Expo Go test: native relay code and the trusted fixture must be compiled into the Android APK or iOS simulator app.

The app has one durable native relay and a scenario plan requiring two UI runtimes. It emits newline-delimited `JAZZ_DEVICE_RESULT {json}` protocol messages. A `passed` state is rejected by the protocol unless it includes platform, device, build, and observation-time evidence. Current scenarios are all `todo`; the UI only emits that truthful plan and does not claim a device receipt.

## Trusted fixture boundary

`native/android/JazzDeviceFixtureModule.kt` and `native/ios/JazzDeviceFixture.mm` admit a scope entirely in trusted platform code. The relay's opaque 32-byte capability is the only value permitted into JavaScript. The templates use build-time test-only fixture placeholders; they must never be populated by Metro variables, intents, a remote config service, or an OTA update. `src/native-fixture.ts` is the narrow JS consumer.

The Expo config plugin copies and registers the Android template during prebuild; its public placeholder `BuildConfig` values make that host compile-shaped without embedding credentials. A real device job must replace only its non-secret test fixture material after staging a matching relay artifact. iOS registration and both platform build receipts remain TODO; this source scaffold is not device acceptance.

## Current acceptance plan

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
JAZZ_DEVICE_APK="$PWD/dev/rn-device-acceptance/android/app/build/outputs/apk/release/app-release.apk" \
  NODE_ENV=production pnpm --filter rn-device-acceptance device:android
```

It pins Temurin 17.0.16+8, command-line tools 13114758, NDK 27.1.12297006,
Android API/build tools 36, cargo-ndk 4.1.2, and (with `--emulator`) the API 35
Google APIs x86_64 image. The cache is ignored and never needs system Java,
Android SDK, adb, or a global cargo-ndk installation. `device:android` compares
the fixture's immutable `Build.FINGERPRINT` with adb's `ro.build.fingerprint`;
app-scoped Android IDs and adb transport serials are intentionally not used.

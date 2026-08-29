# React Native device acceptance harness

This is the first-party **Expo development-build / bare-host** acceptance app for the native relay. It is intentionally not an Expo Go test: native relay code and the trusted fixture must be compiled into the Android APK or iOS simulator app.

The app has one durable native relay and a scenario plan requiring two UI peers. It emits newline-delimited `JAZZ_DEVICE_RESULT {json}` protocol messages automatically on launch. A `passed` state is rejected by the protocol unless it includes platform, device, build, and observation-time evidence. Linked relay admission, foreground JSI byte-ABI execution, foreground mergeable/exclusive transaction commands, local A→B subscription observation, logout revocation, trusted A→B auth-scope switching, and native path-selected data isolation are implemented receipts. The transaction receipt sends canonical fixture cell bytes through JSI to Rust for insert/update/upsert/delete, checks commit `txId`, rollback, and terminal/cross-foreground handle rejection. The local-write-subscription receipt opens two foreground aliases in **one installed JSI runtime** against one admitted relay, starts B's subscription, commits A's fixed row, and requires B to observe the Rust-produced binding delta. It is not a proof of two physical JSI runtimes: that installed-device scenario remains explicit acceptance debt. The isolation receipt makes the two native-selected auth scopes write distinct fixed rows, then proves each foreground can materialize only its own row after trusted native A→B replacement. The device driver terminates the full process and a verification launch repeats both directions through a fresh JS bridge and native relay owner. JavaScript never selects a scope, path, schema, identity, or row payload.

## High-level foreground source receipt

`packages/jazz-tools/src/react-native/create-jazz-client.test.ts` contains the
source-level contract for the capability-gated foreground path. It loads the
**built installed-package** `jazz-rn/relay` entry point (not a Jazz-owned test
shim), has the native test host install its JSI factory, then opens an already
verified local session through the normal public Db API. The receipt performs a
schema-backed insert, query, subscription, local settlement, and shutdown
against that alias. It proves that this flow neither inspects WASM nor uses the
generic TurboModule frame executor. Remote-tier reads still fail closed.

That is deliberately a source/ABI contract, complementary to—not a substitute
for—the installed Android/iOS device receipts. The device app wires the same
high-level scenario after trusted admission; the platform fixture still admits
only opaque capabilities and must not smuggle arbitrary path or write authority
across that boundary.

## Trusted fixture boundary

`native/android/JazzDeviceFixtureModule.kt` and `native/ios/JazzDeviceFixture.mm` admit a scope entirely in trusted platform code. The relay's opaque 32-byte capability is the only value permitted into JavaScript. The iOS fixture uses fixed non-secret test material and reads the launch nonce, build fingerprint, and simulator UDID only from native process arguments; they must never be populated by Metro variables, intents, a remote config service, or an OTA update. `src/native-fixture.ts` is the narrow JS consumer.

The Expo config plugin copies and registers the Android template during prebuild; its public placeholder `BuildConfig` values make that host compile-shaped without embedding credentials. The iOS Blacksmith job stages the XCFramework, prebuilds, installs pods, registers the fixture, builds, installs, and launches the simulator app. It is a real device receipt gate, but is intentionally incomplete until every TODO scenario is implemented.

## Current acceptance plan

- `linked-abi-admission`: automatic iOS and Android receipt that verifies the embedded ABI and receives a 32-byte capability from the trusted native admission boundary.
- `foreground-byte-abi`: opens an installed JSI foreground from that capability and sends the v1 postcard `Probe`, `Tick`, and `Close` command bytes. It requires the expected responses, then keeps a second foreground open until native logout makes its next byte command fail.
- `foreground-write-transaction`: opens the installed JSI factory and sends the native ABI's mergeable and exclusive transaction command bytes. Rust decodes the fixture cells under the native `todos(title: Text)` schema; JavaScript never implements a row codec.
- `logout-revocation`: opens a relay/client alias, has trusted native code revoke it, proves the old capability and aliases no longer work, then proves a fresh native admission can open and attach.
- `logout-auth-switch`: admits scope A, verifies its relay/client aliases, then has trusted native code revoke A before deriving scope B's distinct path and identity. Old A capability bytes and aliases cannot open or attach after the switch; B receives a new capability and independently opens and attaches.
- `local-write-subscription`: UI-A write observed by UI-B through one relay, with both aliases in one installed JSI runtime.
- `independent-jsi-runtime-subscription`: two physical JSI runtimes through one relay (explicit TODO; do not infer this from the same-runtime alias receipt).
- `reconnect`: connection recovery (still TODO).
- `reopen`: the drivers run a seed launch, deliberately terminate the full Android/iOS app process, then run a verification launch. Fresh A and B foregrounds must each read only their own row committed by the seed process.
- `scope-isolation`: persists canonical A and B fixture rows through separate installed JSI foregrounds. Trusted Android/iOS code alone replaces A with B (whose SQLite path is chosen in compiled native code); both directions must reject the other scope's row, both before and after a full process restart. It never exposes a generic host-reset or path-selection API to JavaScript.
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

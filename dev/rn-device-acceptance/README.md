# React Native device acceptance harness

This is the first-party **Expo development-build / bare-host** acceptance app for the native relay. It is intentionally not an Expo Go test: native relay code and the trusted fixture must be compiled into the Android APK or iOS simulator app.

The app has one durable native relay and a scenario plan requiring two UI runtimes. It emits newline-delimited `JAZZ_DEVICE_RESULT {json}` protocol messages. A `passed` state is rejected by the protocol unless it includes platform, device, build, and observation-time evidence. Current scenarios are all `todo`; the UI only emits that truthful plan and does not claim a device receipt.

## Trusted fixture boundary

`native/android/JazzDeviceFixtureModule.kt` and `native/ios/JazzDeviceFixture.mm` admit a scope entirely in trusted platform code. The relay's opaque 32-byte capability is the only value permitted into JavaScript. The templates use build-time test-only fixture placeholders; they must never be populated by Metro variables, intents, a remote config service, or an OTA update. `src/native-fixture.ts` is the narrow JS consumer.

The Expo config plugin copies those templates during prebuild. Native host registration and CI fixture material are deliberately tracked as TODO because their generated-host shapes and the verified fixture schema/identity must be validated with a real staged relay artifact. The injected code is therefore not an asserted build receipt.

## Current acceptance plan

- `local-write-subscription`: UI-A write observed by UI-B through one relay.
- `reconnect` and `reopen`: connection recovery and durable process/app relaunch.
- `scope-isolation` and `logout-auth-switch`: separate scope visibility and revocation before replacement.
- `backpressure` and `corrupt-store`: bounded frame recovery and fail-closed storage diagnostics.

Run source checks with `pnpm --filter rn-device-acceptance verify`. After a real development build exists, the drivers require `JAZZ_DEVICE_APK` (Android) or `IOS_SIMULATOR_UDID`, `JAZZ_DEVICE_APP`, and immutable `JAZZ_DEVICE_BUILD_FINGERPRINT` (iOS). Each launch receives a fresh nonce and requires exactly one, fresh, strictly ordered receipt for every expected scenario, bound to that platform/device/build/nonce. They reject TODO/blocked/failed, duplicate, unknown, stale, partial, or foreign receipts; a green process cannot be manufactured by a fixture log line.

---
name: jazz-expo
description: Build and troubleshoot Jazz applications in Expo and React Native. Use for jazz-rn installation and codegen, development builds, Expo polyfills, Metro and withJazz setup, jazz-tools/react-native bindings, secure local-first identity, native persistent storage and dataPath, simulator or device sync URLs, offline cold starts, native lifecycle, and iOS or Android verification. Do not use Expo Go for Jazz native runtime work.
---

# Jazz Expo and React Native

Treat Expo as a native Jazz runtime, not a browser build with different components. Preserve native
module discovery, initialization order, secure identity storage, durable database storage, and
device-reachable networking.

## Start from the native project

1. Read the installed Expo, React Native, `jazz-tools`, and `jazz-rn` versions plus the existing app
   config, entry point, Metro config, and native build scripts.
2. Confirm whether the app uses Expo prebuild/development builds or a committed native project.
   Jazz's native module does not run in Expo Go.
3. Locate identity storage, the intended native database path, the sync server URL for each target,
   and the owner of the Jazz client lifecycle.
4. Read the reference that matches the task:
   - [native-setup.md](references/native-setup.md) for dependencies, imports, polyfills, Metro,
     provider setup, native builds, and reactive queries.
   - [storage-networking-and-lifecycle.md](references/storage-networking-and-lifecycle.md) for
     SecureStore identity, SQLite persistence, simulator/device URLs, offline behavior, and cleanup.

## Preserve native initialization order

- Install `jazz-rn` as a direct application dependency at the same compatible version as
  `jazz-tools`; React Native codegen must discover its Turbo Module.
- Import `jazz-tools/expo/polyfills` in the entry point before any module that imports Jazz.
- Import providers and data hooks from `jazz-tools/react-native`. Import Expo-specific secure
  identity helpers from `jazz-tools/expo`.
- Keep Hermes and New Architecture settings consistent with the installed working example and
  `jazz-rn` requirements.
- Use a development/native build (`expo run:ios` or `expo run:android`) and verify generated native
  projects after dependency or codegen changes.

## Keep identity and row storage separate

- Store local-first identity through `ExpoAuthSecretStore` or `useLocalFirstAuth()`; it uses
  `expo-secure-store` and cryptographic randomness.
- Treat the secret as the user's identity. Never put it in AsyncStorage, logs, source control, or an
  `EXPO_PUBLIC_*` variable.
- Give offline-capable apps an explicit stable `dataPath` in application-owned persistent storage.
  Do not assume the native runtime's fallback temporary path survives OS cleanup.
- Decide account switching, identity deletion, and SQLite deletion separately. Clearing one does
  not automatically clear the other.

## Make the sync server reachable

- Use host loopback for the iOS simulator only when it reaches the development machine.
- Use `10.0.2.2` for the standard Android emulator and the development machine's LAN address for a
  physical device.
- Verify the injected server URL on the target device; do not assume a host-loopback value is
  rewritten automatically by every installed plugin version.
- Keep app IDs and server URLs public, but keep admin and backend secrets outside the mobile bundle.
- Account for local-network permissions, cleartext-development policy, firewall rules, and server
  bind address when testing physical devices.

## Own lifecycle deliberately

- Keep provider config stable across renders. The provider owns and shuts down the client it creates.
- Wait for secure identity before mounting `JazzProvider`; do not generate a new secret during
  render or startup races.
- Shut down manually created native clients when their owner terminates. Do not manually shut down
  a provider-owned client.
- Treat sync as foreground/process-active unless the installed app explicitly implements and tests
  background execution.

## Cross into adjacent work deliberately

- Load `jazz-auth` for external providers, recovery, sign-in/out, or identity upgrades.
- Load `jazz-core` for ordinary React queries, writes, includes, and loading states.
- Load `jazz-schema-evolution` for established schema or permission changes.
- Load `jazz-testing` when the requested work includes TypeScript tests beyond native build and
  device verification.

## Verify both platforms

1. Run schema validation and TypeScript checks.
2. Prebuild iOS and Android and confirm `jazz-rn` codegen/linkage.
3. Run native development builds rather than Expo Go.
4. Cold-relaunch and confirm the session user ID and offline rows persist.
5. Write while offline, reconnect, and await edge settlement.
6. Change query inputs and confirm active UI subscriptions follow them.
7. Test iOS simulator, Android emulator, and a physical-device URL when that target is supported.

## Avoid these failure modes

- Do not import Jazz before Expo polyfills.
- Do not rely on a transitive `jazz-rn` installation.
- Do not describe a successful Metro bundle as proof that native codegen linked.
- Do not store identity and database state in the same lifecycle bucket.
- Do not use `localhost` blindly on Android or physical devices.
- Do not promise background sync without a platform-specific implementation and test.

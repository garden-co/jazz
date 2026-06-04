---
"jazz-tools": patch
---

`jazz-tools/expo/polyfills` now installs `globalThis.crypto.getRandomValues` from `expo-crypto` when React Native/Hermes does not provide `globalThis.crypto`, fixing anonymous/local-first `createDb({ appId })` startup in Expo apps.

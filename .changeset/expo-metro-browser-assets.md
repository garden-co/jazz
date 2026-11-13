---
"jazz-tools": patch
---

Expo: the local-first web client runs on Expo web once the Metro config is wrapped in `withJazz` from the new `jazz-tools/expo/metro`; before, its Wasm request was a 404.

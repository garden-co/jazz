---
"jazz-tools": patch
---

`jazz-rn` is now an optional peer dependency of `jazz-tools` instead of a regular dependency. React Native / Expo apps must add `jazz-rn` to their own `package.json`; web/Node apps are unaffected (jazz-wasm continues to be bundled internally). When the peer is missing, the new `loadJazzRn` loader surfaces an explicit install hint instead of a generic resolution failure.

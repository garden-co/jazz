---
"jazz-tools": patch
---

**Breaking change — action required for Expo / React Native users:** you must now install `jazz-rn` as a direct dependency in every Expo / React Native project (e.g. `npm install jazz-rn` / `pnpm add jazz-rn` / `yarn add jazz-rn`). It used to be pulled in transitively through `jazz-tools`, but is now an optional peer dependency, so it will no longer be installed for you. Web/Node apps are unaffected (jazz-wasm continues to be bundled internally). If `jazz-rn` is missing at runtime, the new `loadJazzRn` loader surfaces an explicit install hint instead of a generic module-resolution error.

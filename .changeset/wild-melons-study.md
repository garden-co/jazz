---
"cojson": minor
"cojson-core-rn": minor
"jazz-tools": minor
---

## Full native crypto (0.20.0)

With this release we complete the migration to a pure Rust toolchain and remove the JavaScript crypto compatibility layer. The native Rust core now runs everywhere: React Native, Edge runtimes, all server-side environments, and the web.

## 💥 Breaking changes

### Crypto providers / fallback behavior

- **Removed `PureJSCrypto`** from `cojson` (including the `cojson/crypto/PureJSCrypto` export).
- **Removed `RNQuickCrypto`** from `jazz-tools`.
- **No more fallback to JavaScript crypto**: if crypto fails to initialize, Jazz now throws an error instead of falling back silently.
- **React Native + Expo**: **`RNCrypto` (via `cojson-core-rn`) is now the default**.

Full migration guide: `https://jazz.tools/docs/upgrade/0-20-0`

---
"jazz-tools": patch
---

Docs/example: document the correct Expo setup for `jazz-tools`.

`jazz-tools` emits `import.meta.url` from its runtime, so Expo apps must enable `unstable_transformImportMeta` in `babel-preset-expo` or Hermes fails to parse the bundle. `@expo/metro-config` only auto-detects `.babelrc`, `.babelrc.js`, and `babel.config.js` — `.cjs`/`.mjs` variants are silently ignored, and `config.transformer.extendsBabelConfigPath` is a no-op on Expo's pipeline. The Expo install docs and the `todo-client-localfirst-expo` example now use a plain CJS `babel.config.js` with `unstable_transformImportMeta: true`, drop the stray `.babelrc` shim, and stop declaring `"type": "module"`. `metro.config.mjs` stays ESM so it can top-level `await withJazz(...)`.

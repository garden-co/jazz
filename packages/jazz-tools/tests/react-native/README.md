# React Native public API harness

This Linux Vitest harness runs the public RN `createDb` path through
`NativeForegroundDb`, the shipped RN postcard codecs, and the same Rust
`NativeRelayHost` lease C ABI used by mobile JSI. The optional NAPI bridge only
copies bytes and schedules owner wakes on Node's event loop. It does not use
`NapiDb`, emulate SQL, or prove mobile JSI/TurboModule installation.

Produce and consume in the same checkout (the bridge is absent by default):

```sh
JAZZ_RN_TEST_BRIDGE=1 pnpm build:correctness-artifacts
pnpm --dir packages/jazz-tools exec tsc --project tests/react-native/tsconfig.json
JAZZ_RN_TEST_BRIDGE=1 node dev/gates/run-correctness-consumer.mjs -- pnpm --dir packages/jazz-tools exec vitest run --config vitest.react-native.config.ts
```

The dedicated suite fails if the sealed native artifact or bridge is absent.
It does not silently skip unsupported APIs. Local reads may validly be empty
before relay delivery; assertions wait for concrete rows/subscription markers.
Mobile device acceptance remains a separate test surface.

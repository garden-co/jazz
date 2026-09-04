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

Use `withNativeRelayFixture(app, async fixture => { ... })` for public API tests.
`app` comes from `schema.defineApp`; `fixture.createDb()` makes another RN root
on the same real relay and tracks its shutdown. The helper attempts all cleanup
steps and preserves both test and cleanup failures. `createNativeRelayFixture`
is available for tests that need explicit lifecycle control.

Optional settings are `appId`, a public admitted `session`, and
`upstream: { serverUrl, jwt }`. Upstream fixtures require the public session;
they hand the JWT only to native private-session admission and attach the
canonical schema through the production C ABI. The public Db receives only
its cookie-session mirror and opaque native capability. Tests own real local
Edge/Core topology setup. Trusted fixture controls are `nativeHost.revoke`,
`beginPrivateSession`, and `attachCanonicalSchema`; there is no simulated
transport or row engine.

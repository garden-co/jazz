# Rust scoped backend client against TS createJazz (local e2e, WIP)

The vitest file `packages/jazz-tools/src/backend/rust-scoped-client.e2e-local.integration.test.ts`
starts TS `createJazz` with `backendClients: [trustWorkload({ issuer, audience, claims: { sub: "worker" }, as: (t) => ({ userId: `machine:${t.sub}` }) })]`.
It uses a stand-in OIDC issuer on loopback (discovery plus a JWKS, ES256 via `jose`, as in
`backend-clients.integration.test.ts`). Then it runs this binary twice:

1. With a token for `sub: "worker"`. The client signs in, connects, inserts a todo with
   `owner = session.user`, and reads it back with `ReadTier::Remote`. The test checks
   stdout and then `jazz.db`.
2. With a token for `sub: "intruder"`, which must fail at open (`Error::SignedOut`).

The binary gets everything from env: `SERVER_URL` (`<origin>/jazz`), `APP_ID` (`jazz.appId`)
and `TOKEN` (the signed JWT; its `Auth::bearer` provider returns it as is). The schema is
`jazz.app.json` here: the test loads it with `testApp(readFileSync(RUST_APP_JSON))`, and
`build.rs` generates the Rust bindings from it.

## Run

```bash
pnpm install --filter jazz-tools... --filter jazz-napi
(cd crates/jazz-napi && CARGO_BUILD_JOBS=2 pnpm build:debug)
# vitest needs the generated fingerprint modules. Without a wasm build, stub them:
fp=$(cd packages/jazz-tools && node -e 'console.log(require("jazz-napi").nativeArtifactFingerprint())')
echo "export const EXPECTED_NAPI_ARTIFACT_FINGERPRINT = \"$fp\";" > packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts
echo 'export const EXPECTED_WASM_ARTIFACT_FINGERPRINT = "unused";' > packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts
(cd dev/e2e/rust-scoped-client && CARGO_TARGET_DIR=../../../target cargo build)
cd packages/jazz-tools && RUST_CLIENT=../../target/debug/scoped-client \
  RUST_APP_JSON=../../dev/e2e/rust-scoped-client/jazz.app.json \
  pnpm exec vitest run --config vitest.config.ts --silent=false \
  src/backend/rust-scoped-client.e2e-local.integration.test.ts
```

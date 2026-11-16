# Rust backend client over the HTTP stream, against TS createJazz (local e2e)

`packages/jazz-tools/src/backend/rust-http-stream.e2e-local.integration.test.ts`
serves a stock `createJazz` main backend without `attach()`, so it can't upgrade
sockets and answers them with `426` and `x-jazz-stream: 1`, as on a host
without WebSockets. It runs this binary as a trusted backend client
(`Config::credential` with a workload token from a stand-in OIDC issuer): the
client's socket fails, it falls back to the HTTP stream, writes, stays idle
for 50 s (past the socket's 45 s silence deadline), writes again and closes.
The test checks that both rows reached the main backend, that the stream was
used (and no socket got through), and that the link never dropped.

The test is skipped unless `RUST_CLIENT` is set.

## Run

```bash
pnpm install
(cd crates/jazz-napi && CARGO_BUILD_JOBS=2 pnpm build:debug)
# Without a wasm build, stub the fingerprint modules vitest imports:
fp=$(cd packages/jazz-tools && node -e 'console.log(require("jazz-napi").nativeArtifactFingerprint())')
echo "export const EXPECTED_NAPI_ARTIFACT_FINGERPRINT = \"$fp\";" > packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts
echo 'export const EXPECTED_WASM_ARTIFACT_FINGERPRINT = "unused";' > packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts
(cd dev/e2e/rust-http-stream-client && CARGO_TARGET_DIR=../../../target cargo build)
cd packages/jazz-tools && RUST_CLIENT=../../target/debug/rust-http-stream-client \
  RUST_APP_JSON=../../dev/e2e/rust-http-stream-client/jazz.app.json \
  pnpm exec vitest run --config vitest.config.ts \
  src/backend/rust-http-stream.e2e-local.integration.test.ts
```

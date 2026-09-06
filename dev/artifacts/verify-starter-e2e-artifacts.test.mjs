import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifyStarterE2EArtifacts } from "./verify-starter-e2e-artifacts.mjs";

const fingerprintA = "a".repeat(64);
const fingerprintB = "b".repeat(64);

function writeFingerprint(path, symbol, fingerprint) {
  writeFileSync(path, `export const ${symbol} = ${JSON.stringify(fingerprint)};\n`);
}

function fixture({
  wasm = fingerprintA,
  napiManifest = fingerprintA,
  napiPointer = napiManifest,
  napiActual = napiPointer,
  compiledWasm = wasm,
  compiledNapi = napiManifest,
} = {}) {
  const root = mkdtempSync(join(tmpdir(), "jazz-starter-e2e-artifacts-"));
  const wasmDir = join(root, "crates/jazz-wasm/pkg");
  const runtimeDir = join(root, "packages/jazz-tools/dist/runtime");
  const generationDir = join(root, "crates/jazz-napi/.native-artifacts/generation-test");
  mkdirSync(wasmDir, { recursive: true });
  mkdirSync(runtimeDir, { recursive: true });
  mkdirSync(generationDir, { recursive: true });
  writeFileSync(
    join(wasmDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "wasm", profile: "release", nativeArtifactFingerprint: wasm }),
  );
  writeFileSync(
    join(generationDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: napiManifest }),
  );
  writeFileSync(
    join(generationDir, "index.js"),
    `module.exports = { nativeArtifactFingerprint: () => ${JSON.stringify(napiActual)} };\n`,
  );
  writeFileSync(
    join(root, "crates/jazz-napi/native-binding.pointer.cjs"),
    `const nativeBinding = require("./.native-artifacts/generation-test/index.js");\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: ${JSON.stringify(napiPointer)} };\n`,
  );
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-wasm.js"),
    "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
    compiledWasm,
  );
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-napi.js"),
    "EXPECTED_NAPI_ARTIFACT_FINGERPRINT",
    compiledNapi,
  );
  return { root };
}

test("starter E2E artifact hand-off checks self-contained staged receipts against injected WASM and active NAPI bindings", async () => {
  const { root } = fixture();
  await verifyStarterE2EArtifacts(root, async () => fingerprintA);
});

test("starter E2E artifact hand-off rejects a stale Jazz Tools expectation", async () => {
  const { root } = fixture({ compiledWasm: fingerprintB });
  await assert.rejects(
    () => verifyStarterE2EArtifacts(root, async () => fingerprintA),
    /WASM package hand-off mismatch/,
  );
});

test("starter E2E artifact hand-off rejects a packaged WASM runtime with a coherent stale receipt", async () => {
  const { root } = fixture({ wasm: fingerprintB, compiledWasm: fingerprintB });
  await assert.rejects(
    () => verifyStarterE2EArtifacts(root, async () => fingerprintA),
    /manifest expects b{64}, module returns a{64}/,
  );
});

test("starter E2E artifact hand-off rejects an active NAPI binding that disagrees with its manifest and Jazz Tools", async () => {
  const { root } = fixture({
    napiManifest: fingerprintB,
    napiPointer: fingerprintB,
    napiActual: fingerprintA,
    compiledNapi: fingerprintB,
  });
  await assert.rejects(
    () => verifyStarterE2EArtifacts(root, async () => fingerprintA),
    /manifest and jazz-tools expect b{64}, pointer expects b{64}, module returns a{64}/,
  );
});

test("starter E2E artifact hand-off rejects a NAPI pointer receipt that disagrees with its active manifest", async () => {
  const { root } = fixture({ napiPointer: fingerprintB });
  await assert.rejects(
    () => verifyStarterE2EArtifacts(root, async () => fingerprintA),
    /manifest and jazz-tools expect a{64}, pointer expects b{64}, module returns b{64}/,
  );
});

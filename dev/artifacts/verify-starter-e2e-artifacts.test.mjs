import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifyStarterE2EArtifacts } from "./verify-starter-e2e-artifacts.mjs";

function writeFingerprint(path, symbol, fingerprint) {
  writeFileSync(path, `export const ${symbol} = ${JSON.stringify(fingerprint)};\n`);
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-starter-e2e-artifacts-"));
  const wasmDir = join(root, "crates/jazz-wasm/pkg");
  const runtimeDir = join(root, "packages/jazz-tools/dist/runtime");
  const napiDir = join(root, "crates/jazz-napi");
  mkdirSync(wasmDir, { recursive: true });
  mkdirSync(runtimeDir, { recursive: true });
  mkdirSync(napiDir, { recursive: true });
  return { root, runtimeDir, wasmDir, napiDir };
}

function writeFixtureReceipts({ wasmDir, napiDir, runtimeDir }) {
  const fingerprint = "a".repeat(64);
  writeFileSync(
    join(wasmDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "wasm", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
  const generation = join(napiDir, ".native-artifacts/generation-test");
  mkdirSync(generation, { recursive: true });
  writeFileSync(
    join(generation, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
  writeFileSync(
    join(napiDir, "native-binding.pointer.cjs"),
    'module.exports = require("./.native-artifacts/generation-test/index.js");\n',
  );
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-wasm.js"),
    "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
    fingerprint,
  );
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-napi.js"),
    "EXPECTED_NAPI_ARTIFACT_FINGERPRINT",
    fingerprint,
  );
}

test("starter E2E artifact hand-off executes packaged WASM and rejects a stale Jazz Tools expectation", async () => {
  const fixtureState = fixture();
  writeFixtureReceipts(fixtureState);
  const { root, runtimeDir } = fixtureState;
  await verifyStarterE2EArtifacts(root, async () => "a".repeat(64));
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-wasm.js"),
    "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
    "b".repeat(64),
  );
  await assert.rejects(
    () => verifyStarterE2EArtifacts(root, async () => "a".repeat(64)),
    /WASM package hand-off mismatch/,
  );
});

import assert from "node:assert/strict";
import { cpSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { verifyStarterE2EArtifacts } from "./verify-starter-e2e-artifacts.mjs";

const repositoryRoot = resolve(import.meta.dirname, "../..");

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
  for (const file of ["jazz_wasm.js", "jazz_wasm_bg.wasm"])
    cpSync(join(repositoryRoot, "crates/jazz-wasm/pkg", file), join(wasmDir, file));
  const fingerprint = "51c31773364d88405c4ac753e5ed7bfaf7b0a2eca40d9a0ac7b2c79be31bf0e9";
  writeFileSync(
    join(wasmDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "wasm", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
  writeFileSync(
    join(napiDir, "jazz-napi.linux-x64-gnu.manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
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
  return { root, runtimeDir };
}

test("starter E2E artifact hand-off executes packaged WASM and rejects a stale Jazz Tools expectation", async () => {
  const { root, runtimeDir } = fixture();
  await verifyStarterE2EArtifacts(root);
  writeFingerprint(
    join(runtimeDir, "native-artifact-fingerprint-wasm.js"),
    "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
    "b".repeat(64),
  );
  await assert.rejects(() => verifyStarterE2EArtifacts(root), /WASM package hand-off mismatch/);
});

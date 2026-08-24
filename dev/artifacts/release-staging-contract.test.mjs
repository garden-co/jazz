import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { stageNapiLoader } from "./stage-napi-loader.mjs";
import { stageNativeFingerprints } from "./stage-native-fingerprints.mjs";

const fingerprint = "a".repeat(64);

function releaseFixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-release-staging-"));
  for (const path of [
    "crates/jazz-napi/.native-artifacts/generation-test",
    "crates/jazz-wasm/pkg",
    "crates/jazz-napi/provenance",
    "packages/jazz-tools/src/runtime",
  ])
    mkdirSync(join(root, path), { recursive: true });
  return root;
}

test("release NAPI staging rejects a valid sealed manifest whose fingerprint disagrees with the pointer", () => {
  const root = releaseFixture();
  const packageDir = join(root, "crates/jazz-napi");
  const generation = join(packageDir, ".native-artifacts/generation-test");
  writeFileSync(
    join(packageDir, "native-binding.cjs"),
    `const nativeBinding = require("./.native-artifacts/generation-test/index.js");\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: "${fingerprint}" };\n`,
  );
  writeFileSync(join(generation, "jazz-napi.linux-x64-gnu.node"), "native bytes");
  writeFileSync(join(generation, "index.js"), "module.exports = {};\n");
  writeFileSync(
    join(generation, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: "b".repeat(64) }),
  );

  assert.throws(
    () => stageNapiLoader(root, "linux-x64-gnu"),
    /pointer fingerprint does not match its sealed manifest/,
  );
  assert.equal(
    readFileSync(join(packageDir, "native-binding.cjs"), "utf8").includes(fingerprint),
    true,
  );
});

test("release fingerprint staging verifies downloaded WASM bytes before deriving expectations", () => {
  const root = releaseFixture();
  const wasmDir = join(root, "crates/jazz-wasm/pkg");
  const wasm = join(wasmDir, "jazz_wasm_bg.wasm");
  writeFileSync(wasm, "expected bytes");
  writeFileSync(
    join(wasmDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({
      kind: "wasm",
      profile: "release",
      nativeArtifactFingerprint: fingerprint,
      artifacts: [
        {
          file: "jazz_wasm_bg.wasm",
          sha256: createHash("sha256").update("expected bytes").digest("hex"),
        },
      ],
    }),
  );
  writeFileSync(
    join(root, "crates/jazz-napi/provenance/jazz-napi.linux-x64-gnu.manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );

  stageNativeFingerprints(root);
  assert.match(
    readFileSync(
      join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts"),
      "utf8",
    ),
    new RegExp(fingerprint),
  );
  writeFileSync(wasm, "tampered bytes");
  assert.throws(() => stageNativeFingerprints(root), /downloaded WASM artifact hash mismatch/);
});

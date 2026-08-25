import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { stageNapiLoader } from "./stage-napi-loader.mjs";
import { stageNativeFingerprints } from "./stage-native-fingerprints.mjs";

const fingerprint = "a".repeat(64);
const wasmFiles = ["jazz_wasm_bg.wasm", "jazz_wasm_bg.wasm.d.ts", "jazz_wasm.d.ts", "jazz_wasm.js"];

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

function writeWasmRelease(root, { artifacts = undefined } = {}) {
  const wasmDir = join(root, "crates/jazz-wasm/pkg");
  const generated = wasmFiles.map((file) => {
    const bytes = `expected ${file}`;
    writeFileSync(join(wasmDir, file), bytes);
    return { file, sha256: createHash("sha256").update(bytes).digest("hex") };
  });
  writeFileSync(
    join(wasmDir, ".jazz-artifact-manifest.json"),
    JSON.stringify({
      kind: "wasm",
      profile: "release",
      nativeArtifactFingerprint: fingerprint,
      artifacts: artifacts ?? generated,
    }),
  );
}

function writeReleaseNapiManifest(root) {
  writeFileSync(
    join(root, "crates/jazz-napi/provenance/jazz-napi.linux-x64-gnu.manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
}

test("release NAPI staging rejects a valid sealed manifest whose fingerprint disagrees with the pointer", () => {
  const root = releaseFixture();
  const packageDir = join(root, "crates/jazz-napi");
  const generation = join(packageDir, ".native-artifacts/generation-test");
  writeFileSync(
    join(packageDir, "native-binding.pointer.cjs"),
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
    readFileSync(join(packageDir, "native-binding.pointer.cjs"), "utf8").includes(fingerprint),
    true,
  );
});

test("release NAPI staging rejects a symlinked active native binding", () => {
  const root = releaseFixture();
  const packageDir = join(root, "crates/jazz-napi");
  const generation = join(packageDir, ".native-artifacts/generation-test");
  writeFileSync(
    join(packageDir, "native-binding.pointer.cjs"),
    `const nativeBinding = require("./.native-artifacts/generation-test/index.js");\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: "${fingerprint}" };\n`,
  );
  const outside = join(root, "outside.node");
  writeFileSync(outside, "outside native bytes");
  symlinkSync(outside, join(generation, "jazz-napi.linux-x64-gnu.node"));
  writeFileSync(join(generation, "index.js"), "module.exports = {};\n");
  writeFileSync(
    join(generation, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
  assert.throws(() => stageNapiLoader(root, "linux-x64-gnu"), /real regular file/);
});

test("release fingerprint staging verifies downloaded WASM bytes before deriving expectations", () => {
  const root = releaseFixture();
  const wasmDir = join(root, "crates/jazz-wasm/pkg");
  const wasm = join(wasmDir, "jazz_wasm_bg.wasm");
  writeWasmRelease(root);
  writeReleaseNapiManifest(root);

  stageNativeFingerprints(root);
  const generatedNapi = readFileSync(
    join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts"),
    "utf8",
  );
  const generatedWasm = readFileSync(
    join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts"),
    "utf8",
  );
  stageNativeFingerprints(root);
  assert.equal(
    readFileSync(
      join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts"),
      "utf8",
    ),
    generatedNapi,
  );
  assert.equal(
    readFileSync(
      join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts"),
      "utf8",
    ),
    generatedWasm,
  );
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

test("release fingerprint staging rejects omitted, duplicate, empty, and malformed WASM artifact entries", () => {
  const cases = [
    { name: "omitted", artifacts: [], expected: /list each generated artifact exactly once/ },
    {
      name: "duplicate",
      artifacts: wasmFiles.map((file, index) => ({
        file: index === 3 ? wasmFiles[0] : file,
        sha256: "a".repeat(64),
      })),
      expected: /list each generated artifact exactly once/,
    },
    {
      name: "empty",
      artifacts: wasmFiles.map((file) => ({ file, sha256: "" })),
      expected: /artifact hash mismatch/,
    },
    {
      name: "malformed",
      artifacts: wasmFiles.map((file) => ({ file, sha256: "not-a-hash" })),
      expected: /artifact hash mismatch/,
    },
  ];
  for (const { name, artifacts, expected } of cases) {
    const root = releaseFixture();
    writeWasmRelease(root, { artifacts });
    writeReleaseNapiManifest(root);
    assert.throws(() => stageNativeFingerprints(root), expected, name);
  }
});

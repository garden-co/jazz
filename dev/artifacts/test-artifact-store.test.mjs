import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  correctnessArtifactPointer,
  readCorrectnessArtifactSnapshot,
  snapshotCorrectnessArtifacts,
} from "./test-artifact-store.mjs";

const require = createRequire(import.meta.url);

function fixture(label, wasmFingerprint, napiFingerprint) {
  const root = mkdtempSync(join(tmpdir(), `jazz-artifact-store-${label}-`));
  execFileSync("git", ["init", "--quiet"], { cwd: root });
  const wasm = join(root, "crates", "jazz-wasm", "pkg");
  const napi = join(root, "crates", "jazz-napi", ".native-artifacts", "generation-test");
  const wasmManifest = {
    kind: "wasm",
    profile: "fast",
    nativeArtifactFingerprint: wasmFingerprint,
  };
  const napiManifest = {
    kind: "napi",
    profile: "release",
    nativeArtifactFingerprint: napiFingerprint,
  };
  for (const [path, value] of [
    [join(wasm, "jazz_wasm.js"), `export const label = ${JSON.stringify(label)};`],
    [join(wasm, "package.json"), '{"type":"module"}'],
    [join(wasm, "jazz_wasm_bg.wasm"), `wasm-bytes:${label}`],
    [join(wasm, ".jazz-artifact-manifest.json"), JSON.stringify(wasmManifest)],
    [join(napi, "index.js"), `module.exports = { label: ${JSON.stringify(label)} };`],
    [join(napi, "binding.node"), `napi:${label}`],
    [join(napi, ".jazz-artifact-manifest.json"), JSON.stringify(napiManifest)],
    [
      join(root, "crates", "jazz-napi", "native-binding.pointer.cjs"),
      'module.exports = require("./.native-artifacts/generation-test/index.js");',
    ],
    [
      join(root, "crates", "jazz-napi", "native-artifact-fingerprint.cjs"),
      `module.exports = { expectedNativeArtifactFingerprint: ${JSON.stringify(napiFingerprint)} };`,
    ],
  ]) {
    mkdirSync(path.substring(0, path.lastIndexOf("/")), { recursive: true });
    writeFileSync(path, value);
  }
  return root;
}

test("two worktrees retain independently runnable fingerprint-addressed correctness pairs", async () => {
  const first = fixture("first", "a".repeat(64), "b".repeat(64));
  const second = fixture("second", "c".repeat(64), "d".repeat(64));
  try {
    const firstSnapshot = snapshotCorrectnessArtifacts(first);
    const secondSnapshot = snapshotCorrectnessArtifacts(second);
    assert.notEqual(firstSnapshot.wasmPackage, secondSnapshot.wasmPackage);
    assert.equal(
      (await import(pathToFileURL(join(firstSnapshot.wasmPackage, "jazz_wasm.js")).href)).label,
      "first",
    );
    assert.equal(
      (await import(pathToFileURL(join(secondSnapshot.wasmPackage, "jazz_wasm.js")).href)).label,
      "second",
    );

    // Publishing a new mutable generation in one checkout cannot affect a
    // previously selected snapshot in another checkout.
    writeFileSync(
      join(second, "crates", "jazz-wasm", "pkg", "jazz_wasm.js"),
      'export const label = "replaced";',
    );
    assert.equal(
      (await import(`${pathToFileURL(join(firstSnapshot.wasmPackage, "jazz_wasm.js")).href}?after=other-publish`)).label,
      "first",
    );
    assert.equal(readCorrectnessArtifactSnapshot(first).fingerprint, firstSnapshot.fingerprint);
    assert.equal(readCorrectnessArtifactSnapshot(second).fingerprint, secondSnapshot.fingerprint);
    assert.equal(existsSync(correctnessArtifactPointer(first)), true);
    assert.equal(
      require(join(first, "crates", "jazz-napi", "correctness-native-binding.pointer.cjs")).nativeBinding
        .label,
      "first",
    );
  } finally {
    rmSync(first, { recursive: true, force: true });
    rmSync(second, { recursive: true, force: true });
  }
});

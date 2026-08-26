import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, sep } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  correctnessArtifactPointer,
  readCorrectnessArtifactSnapshot,
  snapshotCorrectnessArtifacts,
} from "./test-artifact-store.mjs";

const require = createRequire(import.meta.url);
const jazzToolsRequire = createRequire(
  new URL("../../packages/jazz-tools/package.json", import.meta.url),
);
const { createServer } = await import(pathToFileURL(jazzToolsRequire.resolve("vite")).href);
const hash = (value) => createHash("sha256").update(value).digest("hex");

function fixture(label, wasmFingerprint, napiFingerprint) {
  const root = mkdtempSync(join(tmpdir(), `jazz-artifact-store-${label}-`));
  execFileSync("git", ["init", "--quiet"], { cwd: root });
  const wasm = join(root, "crates", "jazz-wasm", "pkg");
  const napi = join(root, "crates", "jazz-napi", ".native-artifacts", "generation-test");
  const wasmJs = `export const label = ${JSON.stringify(label)};`;
  const wasmBytes = `wasm-bytes:${label}`;
  const napiJs = `module.exports = { label: ${JSON.stringify(label)} };`;
  const napiBytes = `napi:${label}`;
  const wasmManifest = {
    kind: "wasm",
    profile: "fast",
    nativeArtifactFingerprint: wasmFingerprint,
    artifacts: [
      { file: "jazz_wasm.js", sha256: hash(wasmJs) },
      { file: "jazz_wasm_bg.wasm", sha256: hash(wasmBytes) },
    ],
  };
  const napiManifest = {
    kind: "napi",
    profile: "release",
    nativeArtifactFingerprint: napiFingerprint,
    artifacts: [{ file: "binding.node", sha256: hash(napiBytes) }],
  };
  for (const [path, value] of [
    [join(wasm, "jazz_wasm.js"), wasmJs],
    [join(wasm, "package.json"), '{"type":"module"}'],
    [join(wasm, "jazz_wasm_bg.wasm"), wasmBytes],
    [join(wasm, ".jazz-artifact-manifest.json"), JSON.stringify(wasmManifest)],
    [join(napi, "index.js"), napiJs],
    [join(napi, "binding.node"), napiBytes],
    [join(napi, ".jazz-artifact-manifest.json"), JSON.stringify(napiManifest)],
    [
      join(root, "crates", "jazz-napi", "native-binding.pointer.cjs"),
      'module.exports = require("./.native-artifacts/generation-test/index.js");',
    ],
    [
      join(root, "crates", "jazz-napi", "native-artifact-fingerprint.cjs"),
      // Model an older package-staging expectation. Correctness snapshots must
      // bind their expectation to the immutable generation, not this mutable
      // compatibility loader input.
      `module.exports = { expectedNativeArtifactFingerprint: ${JSON.stringify("0".repeat(64))} };`,
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
      (
        await import(
          `${pathToFileURL(join(firstSnapshot.wasmPackage, "jazz_wasm.js")).href}?after=other-publish`
        )
      ).label,
      "first",
    );
    assert.equal(readCorrectnessArtifactSnapshot(first).fingerprint, firstSnapshot.fingerprint);
    assert.equal(readCorrectnessArtifactSnapshot(second).fingerprint, secondSnapshot.fingerprint);
    assert.equal(existsSync(correctnessArtifactPointer(first)), true);
    const firstNapi = require(
      join(first, "crates", "jazz-napi", "correctness-native-binding.pointer.cjs"),
    );
    assert.equal(firstNapi.nativeBinding.label, "first");
    assert.equal(firstNapi.expectedNativeArtifactFingerprint, "b".repeat(64));
  } finally {
    rmSync(first, { recursive: true, force: true });
    rmSync(second, { recursive: true, force: true });
  }
});

test("snapshot pointers reject malformed fingerprints and path traversal", () => {
  const root = fixture("traversal", "e".repeat(64), "f".repeat(64));
  try {
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeFileSync(
      correctnessArtifactPointer(root),
      JSON.stringify({
        ...snapshot,
        fingerprint: `${"e".repeat(64)}-${"f".repeat(63)}/x`,
        wasmPackage: join(root, "outside"),
      }),
    );
    assert.throws(() => readCorrectnessArtifactSnapshot(root), /invalid snapshot receipt/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("tampered or incomplete stored generations fail on read and reuse", () => {
  const root = fixture("tamper", "1".repeat(64), "2".repeat(64));
  try {
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeFileSync(join(snapshot.wasmPackage, "jazz_wasm.js"), "tampered");
    assert.throws(
      () => readCorrectnessArtifactSnapshot(root),
      /artifact hash mismatch|inventory or hash differs/,
    );
    assert.throws(
      () => snapshotCorrectnessArtifacts(root),
      /artifact hash mismatch|inventory or hash differs/,
    );
    writeFileSync(join(snapshot.wasmPackage, "jazz_wasm.js"), 'export const label = "tamper";');
    rmSync(join(snapshot.wasmPackage, "jazz_wasm_bg.wasm"));
    assert.throws(
      () => readCorrectnessArtifactSnapshot(root),
      /missing wasm artifact|inventory or hash differs/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("source and stored symbolic links are rejected recursively", () => {
  const sourceRoot = fixture("source-link", "3".repeat(64), "4".repeat(64));
  const storedRoot = fixture("stored-link", "5".repeat(64), "6".repeat(64));
  const ancestorRoot = fixture("ancestor-link", "9".repeat(64), "a".repeat(64));
  try {
    const outside = join(sourceRoot, "outside");
    writeFileSync(outside, "outside");
    const sourceFile = join(sourceRoot, "crates", "jazz-wasm", "pkg", "package.json");
    rmSync(sourceFile);
    symlinkSync(outside, sourceFile);
    assert.throws(() => snapshotCorrectnessArtifacts(sourceRoot), /contains a symbolic link/);

    const snapshot = snapshotCorrectnessArtifacts(storedRoot);
    const storedFile = join(snapshot.napiGeneration, "index.js");
    rmSync(storedFile);
    symlinkSync(
      join(storedRoot, "crates", "jazz-napi", ".native-artifacts", "generation-test", "index.js"),
      storedFile,
    );
    assert.throws(() => readCorrectnessArtifactSnapshot(storedRoot), /contains a symbolic link/);

    const outsideStore = join(ancestorRoot, "outside-store");
    mkdirSync(outsideStore);
    symlinkSync(outsideStore, join(ancestorRoot, "target"), "dir");
    assert.throws(
      () => snapshotCorrectnessArtifacts(ancestorRoot),
      /snapshot store has a symbolic-link ancestor/,
    );
  } finally {
    rmSync(sourceRoot, { recursive: true, force: true });
    rmSync(storedRoot, { recursive: true, force: true });
    rmSync(ancestorRoot, { recursive: true, force: true });
  }
});

test("Vite serves the validated snapshot without allowing paths outside the worktree", async () => {
  const root = fixture("vite", "7".repeat(64), "8".repeat(64));
  let server;
  try {
    const snapshot = snapshotCorrectnessArtifacts(root);
    server = await createServer({
      root,
      logLevel: "silent",
      server: { host: "127.0.0.1", port: 0, strictPort: false, fs: { allow: [root] } },
    });
    await server.listen();
    const address = server.httpServer.address();
    assert.equal(typeof address, "object");
    const artifactPath = join(snapshot.wasmPackage, "jazz_wasm_bg.wasm").split(sep).join("/");
    const vitePath = artifactPath.startsWith("/") ? artifactPath : `/${artifactPath}`;
    const response = await fetch(`http://127.0.0.1:${address.port}/@fs${vitePath}`);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "wasm-bytes:vite");
    const denied = await fetch(`http://127.0.0.1:${address.port}/@fs/etc/passwd`);
    assert.equal(denied.status, 403);
  } finally {
    await server?.close();
    rmSync(root, { recursive: true, force: true });
  }
});

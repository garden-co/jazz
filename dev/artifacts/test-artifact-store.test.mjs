import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
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
import {
  correctnessArtifactConsumerEnvironment,
  correctnessArtifactProducerManifest,
  verifyCorrectnessArtifactProducer,
  writeCorrectnessArtifactProducerManifest,
} from "./correctness-artifact-producer.mjs";

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
    [join(root, "target", "debug", "jazz-tools"), `cli:${label}`],
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

test("producer manifest binds immutable artifacts to every relevant source input", () => {
  const root = fixture("producer", "a".repeat(64), "b".repeat(64));
  try {
    // A real commit is deliberate: the manifest must identify a checkout, not
    // merely whatever source files happened to exist while the producer ran.
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const cli = join(root, "target", "debug", "jazz-tools");
    writeFileSync(cli, "first-cli");
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    assert.doesNotThrow(() => verifyCorrectnessArtifactProducer(root));

    // A later mutable CLI build is not consumer authority: the snapshot's CLI
    // is content-addressed and continues to be the only executable selected by
    // a manifest already handed to a consumer.
    writeFileSync(cli, "different-cli");
    assert.doesNotThrow(() => verifyCorrectnessArtifactProducer(root));
    writeFileSync(cli, "first-cli");

    // Planted tracked and untracked source changes must both reject the
    // hand-off even though HEAD itself is unchanged.
    writeFileSync(join(root, "README"), "dirty tracked source");
    assert.throws(() => verifyCorrectnessArtifactProducer(root), /different source inputs/);
    rmSync(join(root, "README"));
    writeFileSync(join(root, "untracked-source"), "dirty untracked source");
    assert.throws(() => verifyCorrectnessArtifactProducer(root), /different source inputs/);
    rmSync(join(root, "untracked-source"));

    // The selected immutable CLI itself is checked on every preflight.
    writeFileSync(snapshot.cliArtifact, "corrupt snapshot CLI");
    assert.throws(
      () => verifyCorrectnessArtifactProducer(root),
      /stored CLI artifact hash differs/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a consumer pins its manifest-selected triple across pointer swaps and partial publication", async () => {
  const root = fixture("manifest-swap", "a".repeat(64), "b".repeat(64));
  try {
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const first = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, first);

    // A later producer can replace both mutable package pointers.  The first
    // consumer still receives only paths named by its immutable manifest.
    const wasm = join(root, "crates", "jazz-wasm", "pkg");
    const nextJs = 'export const label = "second";';
    const nextBytes = "wasm-bytes:second";
    writeFileSync(join(wasm, "jazz_wasm.js"), nextJs);
    writeFileSync(join(wasm, "jazz_wasm_bg.wasm"), nextBytes);
    writeFileSync(
      join(wasm, ".jazz-artifact-manifest.json"),
      JSON.stringify({
        kind: "wasm",
        profile: "fast",
        nativeArtifactFingerprint: "c".repeat(64),
        artifacts: [
          { file: "jazz_wasm.js", sha256: hash(nextJs) },
          { file: "jazz_wasm_bg.wasm", sha256: hash(nextBytes) },
        ],
      }),
    );
    const second = snapshotCorrectnessArtifacts(root);
    assert.notEqual(second.fingerprint, first.fingerprint);
    const environment = correctnessArtifactConsumerEnvironment(root);
    assert.equal(environment.JAZZ_CORRECTNESS_WASM_PACKAGE, first.wasmPackage);
    assert.equal(environment.JAZZ_CORRECTNESS_NAPI_BINDING, join(first.napiGeneration, "index.js"));
    assert.equal(environment.JAZZ_CORRECTNESS_CLI, first.cliArtifact);

    // Deliberately race a mutable pointer publisher in another process.  A
    // consumer preflight resolves only the manifest's content address, so it
    // must observe the first triple on every iteration regardless of which
    // generation happens to be current at that instant.
    const pointer = correctnessArtifactPointer(root);
    const napiPointer = join(root, "crates", "jazz-napi", "correctness-native-binding.pointer.cjs");
    const swapper = spawn(
      process.execPath,
      [
        "-e",
        [
          "const fs=require('node:fs')",
          "const [pointer,napi,first,second]=process.argv.slice(1)",
          "for(let i=0;i<2000;i++) { fs.writeFileSync(pointer, i%2 ? first : second); fs.writeFileSync(napi, 'module.exports = {};\\n') }",
        ].join(";"),
        pointer,
        napiPointer,
        JSON.stringify(first),
        JSON.stringify(second),
      ],
      { stdio: "ignore" },
    );
    let exited = false;
    let observations = 0;
    swapper.once("exit", (code) => {
      assert.equal(code, 0);
      exited = true;
    });
    do {
      const selected = correctnessArtifactConsumerEnvironment(root);
      assert.equal(selected.JAZZ_CORRECTNESS_WASM_PACKAGE, first.wasmPackage);
      assert.equal(selected.JAZZ_CORRECTNESS_NAPI_BINDING, join(first.napiGeneration, "index.js"));
      observations++;
      await new Promise((resolvePromise) => setImmediate(resolvePromise));
    } while (!exited);
    assert.ok(observations > 1, "pointer swap did not overlap consumer preflight");

    // A killed producer leaves only a private temporary manifest.  Existing
    // readers remain on the prior atomic receipt, never a half-written one.
    const manifest = correctnessArtifactProducerManifest(root);
    writeFileSync(`${manifest}.interrupted.tmp`, "{");
    assert.equal(verifyCorrectnessArtifactProducer(root).snapshot.fingerprint, first.fingerprint);
  } finally {
    rmSync(root, { recursive: true, force: true });
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
    rmSync(join(ancestorRoot, "target"), { recursive: true, force: true });
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

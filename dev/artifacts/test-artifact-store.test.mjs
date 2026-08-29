import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  chmodSync,
  linkSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
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
import { runCorrectnessConsumer } from "../gates/run-correctness-consumer.mjs";

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
      // bind their expectation to the sealed generation, not this mutable
      // compatibility loader input.
      `module.exports = { expectedNativeArtifactFingerprint: ${JSON.stringify("0".repeat(64))} };`,
    ],
  ]) {
    mkdirSync(path.substring(0, path.lastIndexOf("/")), { recursive: true });
    writeFileSync(path, value);
  }
  return root;
}

function removeFixture(root) {
  const makeWritable = (path) => {
    const stat = lstatSync(path);
    if (stat.isSymbolicLink()) return;
    if (stat.isDirectory()) {
      for (const entry of readdirSync(path)) makeWritable(join(path, entry));
      chmodSync(path, 0o755);
    } else chmodSync(path, 0o644);
  };
  if (existsSync(root)) makeWritable(root);
  rmSync(root, { recursive: true, force: true });
}

test("two worktrees retain independently runnable fingerprint-addressed correctness pairs", async () => {
  const first = fixture("first", "a".repeat(64), "b".repeat(64));
  const second = fixture("second", "c".repeat(64), "d".repeat(64));
  try {
    const firstSnapshot = snapshotCorrectnessArtifacts(first);
    const secondSnapshot = snapshotCorrectnessArtifacts(second);
    for (const path of [
      join(firstSnapshot.wasmPackage, "jazz_wasm.js"),
      join(firstSnapshot.napiGeneration, "index.js"),
      join(firstSnapshot.wasmPackage, "..", "receipt.json"),
    ]) {
      const stat = statSync(path);
      assert.equal(stat.nlink, 1, `${path} is not a single-link snapshot file`);
      assert.equal(stat.mode & 0o222, 0, `${path} remains writable after publication`);
    }
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
    const firstNapi = require(join(firstSnapshot.napiGeneration, "index.js"));
    assert.equal(firstNapi.label, "first");
    assert.equal(
      existsSync(join(first, "crates", "jazz-napi", "correctness-native-binding.pointer.cjs")),
      false,
      "snapshot publication must not leave a fallback NAPI selector behind",
    );
  } finally {
    removeFixture(first);
    removeFixture(second);
  }
});

test("a sealed publication collision accepts the winner without leaking its private stage", () => {
  const root = fixture("collision", "f".repeat(64), "0".repeat(64));
  try {
    let winner;
    const selected = snapshotCorrectnessArtifacts(root, {
      beforePublish() {
        winner = snapshotCorrectnessArtifacts(root);
      },
    });
    assert.equal(selected.fingerprint, winner.fingerprint);
    assert.equal(readCorrectnessArtifactSnapshot(root).fingerprint, winner.fingerprint);
    const storeEntries = readdirSync(join(root, "target", "correctness-test-artifacts"));
    assert.equal(
      storeEntries.some((entry) => entry.startsWith(".stage-")),
      false,
    );
  } finally {
    removeFixture(root);
  }
});

test("producer manifest binds sealed artifacts to every relevant source input", () => {
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
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    assert.doesNotThrow(() => verifyCorrectnessArtifactProducer(root));

    // A later mutable WASM package publication is not consumer authority: the
    // content-addressed snapshot remains selected by the producer manifest.
    const mutableWasm = join(root, "crates", "jazz-wasm", "pkg", "jazz_wasm.js");
    const originalWasm = readFileSync(mutableWasm, "utf8");
    writeFileSync(mutableWasm, "different mutable WASM package");
    assert.doesNotThrow(() => verifyCorrectnessArtifactProducer(root));
    writeFileSync(mutableWasm, originalWasm);

    // Planted tracked and untracked source changes must both reject the
    // hand-off even though HEAD itself is unchanged.
    writeFileSync(join(root, "README"), "dirty tracked source");
    assert.throws(() => verifyCorrectnessArtifactProducer(root), /different source inputs/);
    rmSync(join(root, "README"));
    writeFileSync(join(root, "untracked-source"), "dirty untracked source");
    assert.throws(() => verifyCorrectnessArtifactProducer(root), /different source inputs/);
    rmSync(join(root, "untracked-source"));

    // The selected WASM itself is checked on every preflight. Read-only mode
    // stops ordinary producer writes, and admission rechecks mode and content.
    const storedWasm = join(snapshot.wasmPackage, "jazz_wasm.js");
    chmodSync(storedWasm, 0o644);
    writeFileSync(storedWasm, "corrupt snapshot WASM");
    assert.throws(
      () => verifyCorrectnessArtifactProducer(root),
      /writable after publication|artifact hash mismatch/,
    );
  } finally {
    removeFixture(root);
  }
});

test("a consumer pins its manifest-selected pair across pointer swaps and partial publication", async () => {
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
    // consumer still receives only paths named by its content-addressed manifest.
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

    // Deliberately race a mutable pointer publisher in another process.  A
    // consumer preflight resolves only the manifest's content address, so it
    // must observe the first pair on every iteration regardless of which
    // generation happens to be current at that instant.
    const pointer = correctnessArtifactPointer(root);
    const swapper = spawn(
      process.execPath,
      [
        "-e",
        [
          "const fs=require('node:fs')",
          "const [pointer,first,second]=process.argv.slice(1)",
          "for(let i=0;i<2000;i++) fs.writeFileSync(pointer, i%2 ? first : second)",
        ].join(";"),
        pointer,
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
    removeFixture(root);
  }
});

test("the consumer wrapper rejects snapshot mutation after preflight", async () => {
  const root = fixture("snapshot-lifecycle", "d".repeat(64), "e".repeat(64));
  try {
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    await assert.rejects(
      runCorrectnessConsumer(
        process.execPath,
        [
          "-e",
          [
            "const fs=require('node:fs')",
            "const file=process.argv[1]",
            "fs.chmodSync(file,0o755)",
            "fs.writeFileSync(file,'accidental concurrent rebuild')",
          ].join(";"),
          join(snapshot.wasmPackage, "jazz_wasm.js"),
        ],
        { cwd: root, rootDir: root },
      ),
      /correctness artifacts changed during consumer execution/,
    );
  } finally {
    removeFixture(root);
  }
});

test("a synchronous spawn failure removes its minted consumer capability", async () => {
  const root = fixture("spawn-failure", "3".repeat(64), "4".repeat(64));
  try {
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    let capabilityPath;
    await assert.rejects(
      runCorrectnessConsumer(process.execPath, ["-e", ""], {
        cwd: root,
        rootDir: root,
        spawnImpl(_executable, _args, options) {
          capabilityPath = options.env.JAZZ_CORRECTNESS_CONSUMER_CAPABILITY;
          throw new Error("planted synchronous spawn failure");
        },
      }),
      /planted synchronous spawn failure/,
    );
    assert.equal(typeof capabilityPath, "string");
    assert.equal(existsSync(capabilityPath), false);
  } finally {
    removeFixture(root);
  }
});

test("one source admission covers nested consumers while snapshot checks continue", async () => {
  const root = fixture("nested-admission", "1".repeat(64), "2".repeat(64));
  try {
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    const publicEnvironment = correctnessArtifactConsumerEnvironment(root);
    const runner = new URL("../gates/run-correctness-consumer.mjs", import.meta.url).href;
    await runCorrectnessConsumer(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        [
          "import fs from 'node:fs'",
          `import { runCorrectnessConsumer } from ${JSON.stringify(runner)}`,
          "fs.writeFileSync(process.argv[1], 'test-owned output')",
          "await runCorrectnessConsumer(process.execPath, ['-e', ''], { rootDir: process.argv[2] })",
        ].join(";"),
        join(root, "test-output"),
        root,
      ],
      { cwd: root, rootDir: root },
    );
    assert.throws(() => verifyCorrectnessArtifactProducer(root), /different source inputs/);
    assert.throws(
      () => runCorrectnessConsumer(process.execPath, ["-e", ""], { cwd: root, rootDir: root }),
      /different source inputs/,
    );
    const priorEnvironment = Object.fromEntries(
      Object.keys(publicEnvironment).map((name) => [name, process.env[name]]),
    );
    try {
      Object.assign(process.env, publicEnvironment);
      assert.throws(
        () => runCorrectnessConsumer(process.execPath, ["-e", ""], { cwd: root, rootDir: root }),
        /different source inputs/,
      );
    } finally {
      for (const [name, value] of Object.entries(priorEnvironment)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  } finally {
    removeFixture(root);
  }
});

test("a capability without a process-start identity cannot bypass fresh source admission", async () => {
  const root = fixture("missing-owner-start", "5".repeat(64), "6".repeat(64));
  try {
    execFileSync("git", ["add", "."], { cwd: root });
    execFileSync(
      "git",
      ["-c", "user.email=test@example.invalid", "-c", "user.name=Test", "commit", "-qm", "fixture"],
      { cwd: root },
    );
    const snapshot = snapshotCorrectnessArtifacts(root);
    writeCorrectnessArtifactProducerManifest(root, snapshot);
    const runner = new URL("../gates/run-correctness-consumer.mjs", import.meta.url).href;
    await assert.rejects(
      runCorrectnessConsumer(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          [
            "import fs from 'node:fs'",
            `import { runCorrectnessConsumer } from ${JSON.stringify(runner)}`,
            "fs.writeFileSync(process.argv[1], 'source drift')",
            "const path=process.env.JAZZ_CORRECTNESS_CONSUMER_CAPABILITY",
            "const capability=JSON.parse(fs.readFileSync(path,'utf8'))",
            "delete capability.ownerStart",
            "fs.writeFileSync(path,JSON.stringify(capability))",
            "await runCorrectnessConsumer(process.execPath, ['-e', ''], { rootDir: process.argv[2] })",
          ].join(";"),
          join(root, "test-output"),
          root,
        ],
        { cwd: root, rootDir: root },
      ),
      /correctness consumer failed/,
    );
  } finally {
    removeFixture(root);
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
    removeFixture(root);
  }
});

test("tampered, hard-linked, or incomplete stored generations fail on read and reuse", () => {
  const root = fixture("tamper", "1".repeat(64), "2".repeat(64));
  try {
    const snapshot = snapshotCorrectnessArtifacts(root);
    const wasmJs = join(snapshot.wasmPackage, "jazz_wasm.js");
    assert.equal(readFileSync(wasmJs).length > 0, true);
    // Publication makes the snapshot non-writable for ordinary consumers,
    // but that is not an integrity boundary when the test runner is root.
    // Deliberately grant write permission and mutate it, then prove every
    // admission path rejects the changed sealed generation.
    chmodSync(wasmJs, 0o644);
    writeFileSync(wasmJs, "tampered");
    assert.throws(
      () => readCorrectnessArtifactSnapshot(root),
      /writable after publication|artifact hash mismatch|inventory or hash differs/,
    );
    assert.throws(
      () => snapshotCorrectnessArtifacts(root),
      /writable after publication|artifact hash mismatch|inventory or hash differs/,
    );
    chmodSync(wasmJs, 0o644);
    writeFileSync(wasmJs, 'export const label = "tamper";');
    chmodSync(snapshot.wasmPackage, 0o755);
    rmSync(join(snapshot.wasmPackage, "jazz_wasm_bg.wasm"));
    assert.throws(
      () => readCorrectnessArtifactSnapshot(root),
      /writable after publication|missing wasm artifact|inventory or hash differs/,
    );
  } finally {
    removeFixture(root);
  }
});

test("stored snapshot rejects a hardlink replacement before admission", () => {
  const root = fixture("hardlink", "b".repeat(64), "c".repeat(64));
  try {
    const snapshot = snapshotCorrectnessArtifacts(root);
    const storedWasm = join(snapshot.wasmPackage, "jazz_wasm.js");
    const replacement = join(root, "replacement-wasm");
    writeFileSync(replacement, readFileSync(storedWasm));
    chmodSync(snapshot.wasmPackage, 0o755);
    rmSync(storedWasm);
    linkSync(replacement, storedWasm);
    chmodSync(snapshot.wasmPackage, 0o555);
    assert.throws(() => readCorrectnessArtifactSnapshot(root), /hardlink/);
  } finally {
    removeFixture(root);
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
    chmodSync(snapshot.napiGeneration, 0o755);
    rmSync(storedFile);
    symlinkSync(
      join(storedRoot, "crates", "jazz-napi", ".native-artifacts", "generation-test", "index.js"),
      storedFile,
    );
    assert.throws(
      () => readCorrectnessArtifactSnapshot(storedRoot),
      /writable after publication|contains a symbolic link/,
    );

    const outsideStore = join(ancestorRoot, "outside-store");
    mkdirSync(outsideStore);
    rmSync(join(ancestorRoot, "target"), { recursive: true, force: true });
    symlinkSync(outsideStore, join(ancestorRoot, "target"), "dir");
    assert.throws(
      () => snapshotCorrectnessArtifacts(ancestorRoot),
      /snapshot store has a symbolic-link ancestor/,
    );
  } finally {
    removeFixture(sourceRoot);
    removeFixture(storedRoot);
    removeFixture(ancestorRoot);
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
    removeFixture(root);
  }
});

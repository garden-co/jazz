import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publishNapiGeneration, validateNapiStage } from "./build.mjs";

const build = readFileSync(new URL("./build.mjs", import.meta.url), "utf8");
const wrapper = readFileSync(
  new URL("../../crates/jazz-napi/scripts/build.js", import.meta.url),
  "utf8",
);
const packageRoot = new URL("../../crates/jazz-napi/", import.meta.url);
const indexCjs = readFileSync(new URL("index.cjs", packageRoot), "utf8");
const indexMjs = readFileSync(new URL("index.mjs", packageRoot), "utf8");
const bootstrap = readFileSync(new URL("native-binding.cjs", packageRoot), "utf8");

function fixture() {
  const root = join(tmpdir(), `jazz-napi-artifact-${process.pid}-${Date.now()}-${Math.random()}`);
  mkdirSync(root, { recursive: true });
  writeFileSync(join(root, "package.json"), '{"main":"index.cjs"}\n');
  writeFileSync(join(root, "index.cjs"), indexCjs);
  writeFileSync(join(root, "index.mjs"), indexMjs);
  writeFileSync(join(root, "native-binding.cjs"), bootstrap);
  return root;
}
function stage(root, name, fingerprint, { complete = true, actual = fingerprint } = {}) {
  const path = join(root, name);
  mkdirSync(path, { recursive: true });
  writeFileSync(join(path, "jazz-napi.linux-x64-gnu.node"), "fixture native bytes");
  if (complete) {
    writeFileSync(join(path, ".jazz-artifact-manifest.json"), "{}\n");
    writeFileSync(join(path, "index.d.ts"), "export declare class NapiDb { tick(): void }\n");
    writeFileSync(
      join(path, "index.js"),
      `class NapiDb { tick() {} } module.exports={ NapiDb, nativeArtifactFingerprint: () => ${JSON.stringify(actual)} };\n`,
    );
  }
  return path;
}
function receipt(root, expression, { esm = false } = {}) {
  return spawnSync(
    process.execPath,
    [...(esm ? ["--input-type=module"] : []), "-e", expression, root],
    { encoding: "utf8" },
  );
}

test("all NAPI entrypoints use one target-aware fail-closed staged build path", () => {
  assert.match(wrapper, /dev\/artifacts\/build\.mjs/);
  assert.match(
    build,
    /--output-dir/,
    "napi-rs must write a private stage, never the live binding path",
  );
  assert.match(build, /acquireArtifactLease\(\)/);
  assert.match(build, /validateNapiStage/);
  assert.match(build, /publishNapiGeneration/);
  assert.match(build, /JAZZ_NAPI_BUILD_FAULT/);
});

test("NAPI pointer publication gives CJS and real ESM named imports the same guarded binding", () => {
  const root = fixture();
  try {
    const fingerprint = "current";
    publishNapiGeneration(stage(root, ".napi-stage-current", fingerprint), root, fingerprint);
    const cjs = receipt(
      root,
      "const b=require(process.argv[1]); if (typeof b.NapiDb !== 'function' || b.nativeArtifactFingerprint() !== 'current') process.exit(12)",
    );
    assert.equal(cjs.status, 0, cjs.stderr);
    const esm = receipt(
      root,
      "const { NapiDb } = await import(process.argv[1] + '/index.mjs'); if (typeof NapiDb !== 'function') process.exit(13)",
      { esm: true },
    );
    assert.equal(esm.status, 0, esm.stderr);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("missing, stale, mismatch, and partial staged generations fail closed without replacing a working reader pointer", () => {
  const root = fixture();
  try {
    const good = "good";
    publishNapiGeneration(stage(root, ".napi-stage-good", good), root, good);
    const prior = readFileSync(join(root, "native-binding.pointer.cjs"), "utf8");
    assert.throws(
      () =>
        publishNapiGeneration(
          stage(root, ".napi-stage-partial", "partial", { complete: false }),
          root,
          "partial",
        ),
      /missing generated loader/,
    );
    assert.equal(readFileSync(join(root, "native-binding.pointer.cjs"), "utf8"), prior);
    const stale = receipt(root, "require(process.argv[1]);");
    assert.equal(stale.status, 0, stale.stderr);
    const mismatchStage = stage(root, ".napi-stage-mismatch", "expected", { actual: "old" });
    publishNapiGeneration(mismatchStage, root, "expected");
    const mismatch = receipt(root, "require(process.argv[1]);");
    assert.notEqual(mismatch.status, 0);
    assert.match(mismatch.stderr, /ABI mismatch/);
    rmSync(join(root, "native-binding.pointer.cjs"));
    const missing = receipt(root, "require(process.argv[1]);");
    assert.notEqual(missing.status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("NAPI publication rejects symlinked generation files before the active pointer changes", () => {
  const root = fixture();
  try {
    publishNapiGeneration(stage(root, ".napi-stage-good", "good"), root, "good");
    const pointer = readFileSync(join(root, "native-binding.pointer.cjs"), "utf8");
    const outside = join(root, "outside.node");
    writeFileSync(outside, "outside");
    const staged = stage(root, ".napi-stage-symlink", "next");
    rmSync(join(staged, "jazz-napi.linux-x64-gnu.node"));
    symlinkSync(outside, join(staged, "jazz-napi.linux-x64-gnu.node"));
    assert.throws(() => publishNapiGeneration(staged, root, "next"), /real regular file/);
    assert.equal(readFileSync(join(root, "native-binding.pointer.cjs"), "utf8"), pointer);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a planted final-pointer failure leaves readers and sealed metadata unchanged", () => {
  const root = fixture();
  const prior = "prior";
  try {
    publishNapiGeneration(stage(root, ".napi-stage-good", prior), root, prior);
    const pointer = readFileSync(join(root, "native-binding.pointer.cjs"), "utf8");
    const generations = readdirSync(join(root, ".native-artifacts")).sort();
    const marker = "marker";
    const manifest = "manifest";
    writeFileSync(join(root, "marker"), marker);
    writeFileSync(join(root, "manifest"), manifest);
    process.env.JAZZ_NAPI_BUILD_FAULT = "pointer-write";
    assert.throws(
      () => publishNapiGeneration(stage(root, ".napi-stage-next", "next"), root, "next"),
      /final-pointer failure/,
    );
    assert.equal(readFileSync(join(root, "native-binding.pointer.cjs"), "utf8"), pointer);
    assert.deepEqual(readdirSync(join(root, ".native-artifacts")).sort(), generations);
    assert.equal(readFileSync(join(root, "marker"), "utf8"), marker);
    assert.equal(readFileSync(join(root, "manifest"), "utf8"), manifest);
  } finally {
    delete process.env.JAZZ_NAPI_BUILD_FAULT;
    rmSync(root, { recursive: true, force: true });
  }
});

test("a newly generated public export absent from the stable package declaration fails before activation", () => {
  const root = fixture();
  try {
    const staged = stage(root, ".napi-stage-new-export", "next");
    const stableDeclarations = join(root, "index.d.ts");
    writeFileSync(stableDeclarations, "export declare class NapiDb { tick(): void }\n");
    writeFileSync(
      join(staged, "index.d.ts"),
      "export declare class NapiDb { tick(): void }\nexport declare function newlyGeneratedExport(): void\n",
    );
    let error;
    try {
      validateNapiStage(staged, "jazz-napi.linux-x64-gnu.node", "next", "cross-target", {
        stableDeclarationsPath: stableDeclarations,
      });
    } catch (caught) {
      error = caught;
    }
    assert.match(
      error.message,
      /generated declarations that differ from the public package type surface/,
    );
    assert.match(error.message, /checked-in declarations: /);
    assert.match(error.message, /generated declarations: /);
    assert.match(error.message, /first difference near line 2/);
    assert.match(error.message, /--- checked-in/);
    assert.match(error.message, /\+\s*2 \| export declare function newlyGeneratedExport/);
    assert.equal(existsSync(join(root, "native-binding.pointer.cjs")), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Turbo keys the tracked package wrappers that define the native ABI contract", () => {
  const turbo = JSON.parse(readFileSync(new URL("../../turbo.json", import.meta.url), "utf8"));
  const inputs = turbo.tasks["jazz-napi#build"].inputs;
  for (const path of ["index.cjs", "index.mjs", "index.d.ts", "native-binding.cjs"])
    assert.ok(inputs.includes(`$TURBO_ROOT$/crates/jazz-napi/${path}`), path);
  const outputs = turbo.tasks["jazz-napi#build"].outputs;
  for (const output of [
    "native-binding.pointer.cjs",
    "native-artifact-fingerprint.cjs",
    ".native-artifacts/**",
  ])
    assert.ok(outputs.includes(output), output);
  for (const obsoleteCanonicalOutput of ["*.node", "index.js", "index.d.ts"])
    assert.equal(outputs.includes(obsoleteCanonicalOutput), false, obsoleteCanonicalOutput);
});

test("independent producers only publish their own expected marker", () => {
  assert.doesNotMatch(build, /native-artifact-fingerprint-\$\{kind\}\.ts/);
  assert.doesNotMatch(
    build,
    /const napi = nativeArtifactFingerprint[\s\S]*const wasm = nativeArtifactFingerprint/,
  );
});

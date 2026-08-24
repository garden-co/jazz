import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publishNapiGeneration } from "./build.mjs";

const build = readFileSync(new URL("./build.mjs", import.meta.url), "utf8");
const wrapper = readFileSync(new URL("../../crates/jazz-napi/scripts/build.js", import.meta.url), "utf8");
const packageRoot = new URL("../../crates/jazz-napi/", import.meta.url);
const indexCjs = readFileSync(new URL("index.cjs", packageRoot), "utf8");
const indexMjs = readFileSync(new URL("index.mjs", packageRoot), "utf8");

function fixture() {
  const root = join(tmpdir(), `jazz-napi-artifact-${process.pid}-${Date.now()}-${Math.random()}`);
  mkdirSync(root, { recursive: true });
  writeFileSync(join(root, "package.json"), '{"main":"index.cjs"}\n');
  writeFileSync(join(root, "index.cjs"), indexCjs);
  writeFileSync(join(root, "index.mjs"), indexMjs);
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
  return spawnSync(process.execPath, [...(esm ? ["--input-type=module"] : []), "-e", expression, root], { encoding: "utf8" });
}

test("all NAPI entrypoints use one target-aware fail-closed staged build path", () => {
  assert.match(wrapper, /dev\/artifacts\/build\.mjs/);
  assert.match(build, /--output-dir/, "napi-rs must write a private stage, never the live binding path");
  assert.match(build, /acquireArtifactLease\(\)/);
  assert.match(build, /validateNapiStage/);
  assert.match(build, /publishNapiGeneration/);
  assert.match(build, /JAZZ_NAPI_BUILD_FAULT/);
});

test("NAPI pointer publication gives CJS and real ESM named imports the same guarded binding", () => {
  const root = fixture();
  try {
    const fingerprint = "current";
    publishNapiGeneration(stage(root, ".napi-stage-current", fingerprint), root, fingerprint, { alreadyLocked: true });
    const cjs = receipt(root, "const b=require(process.argv[1]); if (typeof b.NapiDb !== 'function' || b.nativeArtifactFingerprint() !== 'current') process.exit(12)");
    assert.equal(cjs.status, 0, cjs.stderr);
    const esm = receipt(root, "const { NapiDb } = await import(process.argv[1] + '/index.mjs'); if (typeof NapiDb !== 'function') process.exit(13)", { esm: true });
    assert.equal(esm.status, 0, esm.stderr);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("missing, stale, mismatch, and partial staged generations fail closed without replacing a working reader pointer", () => {
  const root = fixture();
  try {
    const good = "good";
    publishNapiGeneration(stage(root, ".napi-stage-good", good), root, good, { alreadyLocked: true });
    const prior = readFileSync(join(root, "native-binding.cjs"), "utf8");
    assert.throws(() => publishNapiGeneration(stage(root, ".napi-stage-partial", "partial", { complete: false }), root, "partial", { alreadyLocked: true }), /missing its generated loader, declarations, or sealed manifest/);
    assert.equal(readFileSync(join(root, "native-binding.cjs"), "utf8"), prior);
    const stale = receipt(root, "require(process.argv[1]);",);
    assert.equal(stale.status, 0, stale.stderr);
    const mismatchStage = stage(root, ".napi-stage-mismatch", "expected", { actual: "old" });
    publishNapiGeneration(mismatchStage, root, "expected", { alreadyLocked: true });
    const mismatch = receipt(root, "require(process.argv[1]);");
    assert.notEqual(mismatch.status, 0);
    assert.match(mismatch.stderr, /ABI mismatch/);
    rmSync(join(root, "native-binding.cjs"));
    const missing = receipt(root, "require(process.argv[1]);");
    assert.notEqual(missing.status, 0);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("Turbo keys the tracked package wrappers that define the native ABI contract", () => {
  const turbo = JSON.parse(readFileSync(new URL("../../turbo.json", import.meta.url), "utf8"));
  const inputs = turbo.tasks["jazz-napi#build"].inputs;
  for (const path of ["index.cjs", "index.mjs", "index.d.ts"])
    assert.ok(inputs.includes(`$TURBO_ROOT$/crates/jazz-napi/${path}`), path);
  const outputs = turbo.tasks["jazz-napi#build"].outputs;
  for (const output of ["native-binding.cjs", "native-binding.d.ts", "native-artifact-fingerprint.cjs", ".native-artifacts/**"])
    assert.ok(outputs.includes(output), output);
  for (const obsoleteCanonicalOutput of ["*.node", "index.js", "index.d.ts"])
    assert.equal(outputs.includes(obsoleteCanonicalOutput), false, obsoleteCanonicalOutput);
});

test("independent producers only publish their own expected marker", () => {
  assert.match(build, /native-artifact-fingerprint-\$\{kind\}\.ts/);
  assert.match(build, /if \(kind === "napi"\)/);
  assert.doesNotMatch(build, /const napi = nativeArtifactFingerprint[\s\S]*const wasm = nativeArtifactFingerprint/);
});

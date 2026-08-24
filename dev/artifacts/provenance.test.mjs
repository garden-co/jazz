import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  expectedManifest,
  manifestPath,
  verifyManifest,
  verifyPublishedNapiManifest,
  writeManifest,
} from "./provenance.mjs";
import { stageNapiManifests } from "./stage-napi-manifests.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-artifact-provenance-"));
  for (const dir of [
    ".cargo",
    "crates/jazz-wasm/pkg",
    "crates/jazz-wasm/src",
    "crates/jazz/src",
    "crates/groove/src",
    "crates/wasm-tracing/src",
    "crates/jazz-napi/src",
  ])
    mkdirSync(join(root, dir), { recursive: true });
  for (const [path, content] of Object.entries({
    "Cargo.toml": "[workspace]\n",
    "Cargo.lock": "lock-a\n",
    "rust-toolchain.toml": "[toolchain]\nchannel = 'stable'\n",
    ".cargo/config.toml": "[build]\n",
    "package.json": "{}\n",
    "pnpm-lock.yaml": "lockfileVersion: '9.0'\n",
    "crates/jazz-wasm/Cargo.toml": "[package]\nname = 'wasm'\n",
    "crates/jazz-wasm/src/lib.rs": "// source\n",
  }))
    writeFileSync(join(root, path), content);
  return root;
}

process.env.JAZZ_ARTIFACT_GIT_HEAD = "test-head";
process.env.JAZZ_ARTIFACT_GIT_TREE = "test-tree";
process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF = "test-dirty";
process.env.JAZZ_ARTIFACT_TOOL_RUSTC = "rustc test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_PACK = "wasm-pack test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_BINDGEN = "wasm-bindgen test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT = "wasm-opt test";
process.env.JAZZ_ARTIFACT_TOOL_NAPI = "napi test";

test("provenance rejects stale tree, lock, toolchain, and profile", () => {
  const root = fixture();
  writeManifest(root, "wasm", "fast");
  assert.equal(verifyManifest(root, "wasm", "fast"), null);
  assert.match(verifyManifest(root, "wasm", "release"), /profile differs/);

  const manifest = JSON.parse(readFileSync(manifestPath(root, "wasm"), "utf8"));
  manifest.git.tree = "stale";
  writeFileSync(manifestPath(root, "wasm"), JSON.stringify(manifest));
  assert.match(verifyManifest(root, "wasm", "fast"), /git.tree differs/);

  writeManifest(root, "wasm", "fast");
  writeFileSync(join(root, "Cargo.lock"), "lock-b\n");
  assert.match(verifyManifest(root, "wasm", "fast"), /cargoLock differs/);
  writeFileSync(join(root, "Cargo.lock"), "lock-a\n");
  writeFileSync(join(root, "rust-toolchain.toml"), "[toolchain]\nchannel = 'beta'\n");
  assert.match(verifyManifest(root, "wasm", "fast"), /rustToolchain differs/);
});

test("provenance rejects a fingerprint-only sealed manifest drift", () => {
  const root = fixture();
  writeManifest(root, "wasm", "fast");
  const path = manifestPath(root, "wasm");
  const stale = JSON.parse(readFileSync(path, "utf8"));
  stale.nativeArtifactFingerprint = "0".repeat(64);
  writeFileSync(path, JSON.stringify(stale));
  assert.match(verifyManifest(root, "wasm", "fast"), /nativeArtifactFingerprint differs/);
  writeManifest(root, "wasm", "fast");
  assert.equal(verifyManifest(root, "wasm", "fast"), null);
});

test("dirty source changes invalidate the manifest", () => {
  const root = fixture();
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, "crates/jazz-wasm/src/lib.rs"), "// changed\n");
  assert.match(
    verifyManifest(root, "wasm", "release"),
    /packageInputs differs|git.dirtyDiff differs/,
  );
});

test("NAPI provenance excludes only the wrapper's ephemeral staged binding", () => {
  const root = fixture();
  writeManifest(root, "napi", "release");
  const before = expectedManifest(root, "napi", "release").packageInputs;

  // build.mjs uses this exact name while replacing a target binding.  Its
  // presence must not make a manifest stale immediately after the build.
  writeFileSync(
    join(root, "crates/jazz-napi/jazz-napi.linux-x64-gnu.node.staged-123-456"),
    "previous native binding",
  );
  assert.equal(expectedManifest(root, "napi", "release").packageInputs, before);
  assert.equal(verifyManifest(root, "napi", "release"), null);
  rmSync(join(root, "crates/jazz-napi/jazz-napi.linux-x64-gnu.node.staged-123-456"));

  // Turbo writes this receipt after the inner NAPI build wrapper seals its
  // manifest. It is a package-local build output, not a build input.
  mkdirSync(join(root, "crates/jazz-napi/.turbo"));
  writeFileSync(join(root, "crates/jazz-napi/.turbo/turbo-build.log"), "outer task receipt");
  assert.equal(expectedManifest(root, "napi", "release").packageInputs, before);
  assert.equal(verifyManifest(root, "napi", "release"), null);

  // Near misses are ordinary inputs: accepting any made-up binding name,
  // suffix, or appended extension would let generated source evade freshness.
  for (const path of [
    "jazz-napi.linux-x64-gnu.node.staged-123-456.rs",
    "jazz-napi.attacker.node.staged-123-456",
    "jazz-napi.linux-x64-gnu.node.staged-not-a-wrapper",
  ]) {
    const file = join(root, "crates/jazz-napi", path);
    writeFileSync(file, "must remain an input");
    assert.notEqual(expectedManifest(root, "napi", "release").packageInputs, before, path);
    rmSync(file);
  }

  // Planted positive: an actual NAPI source remains a provenance input.
  writeFileSync(join(root, "crates/jazz-napi/src/lib.rs"), "// changed native source\n");
  assert.match(verifyManifest(root, "napi", "release"), /packageInputs differs/);
});

test("WASM provenance covers generated glue and declarations, not only the binary", () => {
  const root = fixture();
  for (const file of [
    "jazz_wasm_bg.wasm",
    "jazz_wasm.js",
    "jazz_wasm.d.ts",
    "jazz_wasm_bg.wasm.d.ts",
  ])
    writeFileSync(join(root, "crates/jazz-wasm/pkg", file), "current");
  writeManifest(root, "wasm", "fast");
  writeFileSync(join(root, "crates/jazz-wasm/pkg/jazz_wasm.js"), "stale glue");
  assert.match(verifyManifest(root, "wasm", "fast"), /artifacts differs/);
});

test("provenance rejects tool and root-package configuration drift", () => {
  const root = fixture();
  writeManifest(root, "wasm", "release");
  const manifest = JSON.parse(readFileSync(manifestPath(root, "wasm"), "utf8"));
  for (const tool of ["rustc", "wasmPack", "wasmBindgen", "wasmOpt"]) {
    const changed = structuredClone(manifest);
    changed.tools[tool] = "stale";
    writeFileSync(manifestPath(root, "wasm"), JSON.stringify(changed));
    assert.match(verifyManifest(root, "wasm", "release"), new RegExp(`tools\\.${tool} differs`));
  }
  const toolHashChanged = structuredClone(manifest);
  toolHashChanged.toolchainInputs = "stale";
  writeFileSync(manifestPath(root, "wasm"), JSON.stringify(toolHashChanged));
  assert.match(verifyManifest(root, "wasm", "release"), /toolchainInputs differs/);
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, "package.json"), '{"changed":true}\n');
  assert.match(verifyManifest(root, "wasm", "release"), /packageInputs differs/);
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, ".cargo/config.toml"), "[build]\ntarget-dir = 'other'\n");
  assert.match(verifyManifest(root, "wasm", "release"), /packageInputs differs/);

  writeManifest(root, "napi", "release");
  const napi = JSON.parse(readFileSync(manifestPath(root, "napi"), "utf8"));
  napi.tools.napi = "stale";
  writeFileSync(manifestPath(root, "napi"), JSON.stringify(napi));
  assert.match(verifyManifest(root, "napi", "release"), /tools.napi differs/);

  writeManifest(root, "napi", "release", "aarch64-apple-darwin");
  assert.match(verifyManifest(root, "napi", "release", "x86_64-apple-darwin"), /target differs/);
});

test("missing optional tools have an explicit remediation value", () => {
  const root = fixture();
  delete process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT;
  process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE = "1";
  assert.match(
    expectedManifest(root, "wasm", "fast").tools.wasmOpt,
    /unavailable: wasm-opt is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build/,
  );
  process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT = "wasm-opt test";
  delete process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE;
});

test("release NAPI CI builds use the manifest-producing wrapper", () => {
  const workflow = readFileSync(
    new URL("../../.github/workflows/build-jazz-packages.yml", import.meta.url),
    "utf8",
  );
  assert.match(
    workflow,
    /node dev\/artifacts\/build\.mjs napi release --target \$\{\{ matrix\.target \}\}/,
  );
  assert.match(
    workflow,
    /node dev\/artifacts\/provenance\.mjs verify napi release --target \$\{\{ matrix\.target \}\}/,
  );
});

test("assembled NAPI packages carry only matching manifests and reject stale or missing inputs", () => {
  const root = fixture();
  const platforms = {
    "linux-x64-gnu": "x86_64-unknown-linux-gnu",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
    "win32-x64-msvc": "x86_64-pc-windows-msvc",
  };
  for (const platform of Object.keys(platforms)) {
    const dir = join(root, "crates/jazz-napi/npm", platform);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "package.json"), '{"files":["*.node"]}\n');
    writeFileSync(join(root, "crates/jazz-napi", `jazz-napi.${platform}.node`), platform);
    writeFileSync(join(dir, `jazz-napi.${platform}.node`), platform);
  }
  writeFileSync(join(root, "crates/jazz-napi/package.json"), '{"files":["index.js"]}\n');
  mkdirSync(join(root, "crates/jazz-napi/artifacts"), { recursive: true });
  for (const [platform, target] of Object.entries(platforms)) {
    const manifest = expectedManifest(root, "napi", "release", target);
    manifest.nativeArtifactFingerprint = "a".repeat(64);
    manifest.packageInputs = "b".repeat(64);
    writeFileSync(
      join(root, "crates/jazz-napi/artifacts", `jazz-napi.${platform}.manifest.json`),
      JSON.stringify(manifest),
    );
  }
  stageNapiManifests(root);
  const node = join(root, "crates/jazz-napi/npm/linux-x64-gnu/jazz-napi.linux-x64-gnu.node");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "crates/jazz-napi/npm/linux-x64-gnu/jazz-napi.linux-x64-gnu.manifest.json"),
      "utf8",
    ),
  );
  assert.equal(verifyPublishedNapiManifest(manifest, platforms["linux-x64-gnu"], node), null);
  assert.match(
    readFileSync(join(root, "crates/jazz-napi/package.json"), "utf8"),
    /provenance\/\*\.manifest\.json/,
  );

  const darwinManifest = join(
    root,
    "crates/jazz-napi/artifacts/jazz-napi.darwin-x64.manifest.json",
  );
  const crossTargetMismatch = JSON.parse(readFileSync(darwinManifest, "utf8"));
  crossTargetMismatch.nativeArtifactFingerprint = "c".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));
  assert.throws(() => stageNapiManifests(root), /different ABI fingerprint or package inputs/);
  crossTargetMismatch.nativeArtifactFingerprint = "a".repeat(64);
  crossTargetMismatch.packageInputs = "d".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));
  assert.throws(() => stageNapiManifests(root), /different ABI fingerprint or package inputs/);
  crossTargetMismatch.packageInputs = "b".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));

  for (const [field, value] of [
    ["nativeArtifactFingerprint", "not-a-fingerprint"],
    ["packageInputs", undefined],
  ]) {
    const malformed = JSON.parse(readFileSync(darwinManifest, "utf8"));
    malformed[field] = value;
    writeFileSync(darwinManifest, JSON.stringify(malformed));
    assert.throws(() => stageNapiManifests(root), /missing native fingerprint or package inputs/);
  }
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));

  writeFileSync(node, "stale");
  assert.match(
    verifyPublishedNapiManifest(manifest, platforms["linux-x64-gnu"], node),
    /does not match/,
  );
  writeFileSync(node, "linux-x64-gnu");
  rmSync(join(root, "crates/jazz-napi/artifacts/jazz-napi.darwin-x64.manifest.json"), {
    force: true,
  });
  assert.throws(() => stageNapiManifests(root), /missing provenance manifest/);
});

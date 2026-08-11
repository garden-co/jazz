import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expectedManifest, manifestPath, verifyManifest, writeManifest } from "./provenance.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-artifact-provenance-"));
  for (const dir of [".cargo", "crates/jazz-wasm/pkg", "crates/jazz-wasm/src", "crates/jazz/src", "crates/groove/src", "crates/opfs-btree/src", "crates/wasm-tracing/src", "crates/jazz-napi/src"]) mkdirSync(join(root, dir), { recursive: true });
  for (const [path, content] of Object.entries({ "Cargo.toml": "[workspace]\n", "Cargo.lock": "lock-a\n", "rust-toolchain.toml": "[toolchain]\nchannel = 'stable'\n", ".cargo/config.toml": "[build]\n", "package.json": "{}\n", "pnpm-lock.yaml": "lockfileVersion: '9.0'\n", "crates/jazz-wasm/Cargo.toml": "[package]\nname = 'wasm'\n", "crates/jazz-wasm/src/lib.rs": "// source\n" })) writeFileSync(join(root, path), content);
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

test("dirty source changes invalidate the manifest", () => {
  const root = fixture();
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, "crates/jazz-wasm/src/lib.rs"), "// changed\n");
  assert.match(verifyManifest(root, "wasm", "release"), /packageInputs differs|git.dirtyDiff differs/);
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
  assert.match(expectedManifest(root, "wasm", "fast").tools.wasmOpt, /unavailable: install wasm-opt or run pnpm ensure:rust-toolchain/);
  process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT = "wasm-opt test";
  delete process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE;
});

test("release NAPI CI builds use the manifest-producing wrapper", () => {
  const workflow = readFileSync(new URL("../../.github/workflows/build-jazz-packages.yml", import.meta.url), "utf8");
  assert.match(workflow, /node dev\/artifacts\/build\.mjs napi release --target \$\{\{ matrix\.target \}\}/);
  assert.match(workflow, /node dev\/artifacts\/provenance\.mjs verify napi release --target \$\{\{ matrix\.target \}\}/);
});

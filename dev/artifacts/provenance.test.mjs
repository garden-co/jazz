import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expectedManifest, manifestPath, verifyManifest, writeManifest } from "./provenance.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-artifact-provenance-"));
  for (const dir of ["crates/jazz-wasm/pkg", "crates/jazz-wasm/src", "crates/jazz/src", "crates/groove/src", "crates/opfs-btree/src", "crates/wasm-tracing/src", "crates/jazz-napi/src"]) mkdirSync(join(root, dir), { recursive: true });
  for (const [path, content] of Object.entries({ "Cargo.toml": "[workspace]\n", "Cargo.lock": "lock-a\n", "rust-toolchain.toml": "[toolchain]\nchannel = 'stable'\n", "crates/jazz-wasm/Cargo.toml": "[package]\nname = 'wasm'\n", "crates/jazz-wasm/src/lib.rs": "// source\n" })) writeFileSync(join(root, path), content);
  return root;
}

process.env.JAZZ_ARTIFACT_GIT_HEAD = "test-head";
process.env.JAZZ_ARTIFACT_GIT_TREE = "test-tree";
process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF = "test-dirty";

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

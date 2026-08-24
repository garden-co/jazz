import assert from "node:assert/strict";
import test from "node:test";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWasmPackageStage, publishWasmPackage } from "./build.mjs";

const packageFiles = [
  "jazz_wasm_bg.wasm",
  "jazz_wasm_bg.wasm.d.ts",
  "jazz_wasm.d.ts",
  "jazz_wasm.js",
  "package.json",
];

function fixture() {
  const root = join(tmpdir(), `jazz-wasm-artifact-${process.pid}-${Date.now()}-${Math.random()}`);
  const wasmRoot = join(root, "crates", "jazz-wasm");
  const pkg = join(wasmRoot, "pkg");
  mkdirSync(pkg, { recursive: true });
  return { root, pkg };
}

function writePackage(path, marker) {
  for (const file of packageFiles) writeFileSync(join(path, file), `${marker}:${file}`);
}

function packageMarkers(path) {
  return packageFiles.map((file) => readFileSync(join(path, file), "utf8"));
}

test("WASM producer rejects a missing staged output without touching the prior package", () => {
  const { root, pkg } = fixture();
  try {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "release");
    writeFileSync(join(stage.path, "jazz_wasm_bg.wasm"), "incomplete");

    assert.throws(
      () => publishWasmPackage(stage.path, pkg),
      /incomplete staged package .*missing jazz_wasm_bg\.wasm\.d\.ts/,
    );
    assert.deepEqual(
      packageMarkers(pkg),
      packageFiles.map((file) => `old:${file}`),
    );
    assert.equal(existsSync(stage.path), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("WASM producers own distinct wasm-opt intermediates and only publish complete packages", async () => {
  const { root, pkg } = fixture();
  try {
    writePackage(pkg, "stale");
    const fast = createWasmPackageStage(root, "fast");
    const release = createWasmPackageStage(root, "release");
    assert.notEqual(fast.path, release.path);
    assert.notEqual(fast.outDir, release.outDir);

    // This is wasm-pack's previously shared scratch name. It is now private to
    // each producer and never appears in the package consumers import.
    writeFileSync(join(fast.path, "jazz_wasm_bg.wasm-opt.wasm"), "fast scratch");
    writeFileSync(join(release.path, "jazz_wasm_bg.wasm-opt.wasm"), "release scratch");
    writePackage(fast.path, "fast");
    writePackage(release.path, "release");

    await Promise.all([
      Promise.resolve().then(() => publishWasmPackage(fast.path, pkg)),
      Promise.resolve().then(() => publishWasmPackage(release.path, pkg)),
    ]);

    const finalMarkers = packageMarkers(pkg);
    assert.ok(
      finalMarkers.every((value) => value.startsWith("fast:") || value.startsWith("release:")),
    );
    assert.ok(
      finalMarkers.every((value) => value.startsWith(finalMarkers[0].split(":")[0])),
      "a publisher must replace the complete package rather than leave profile-mixed files",
    );
    assert.equal(existsSync(join(pkg, "jazz_wasm_bg.wasm-opt.wasm")), false);
    assert.equal(existsSync(fast.path), false);
    assert.equal(existsSync(release.path), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

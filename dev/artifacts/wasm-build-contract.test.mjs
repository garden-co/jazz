import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import test from "node:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWasmPackageStage, publishWasmPackage, recoverWasmPackageTransaction, wasmPackageFiles, writeWasmStageManifest } from "./build.mjs";

function fixture() {
  const root = join(tmpdir(), `jazz-wasm-artifact-${process.pid}-${Date.now()}-${Math.random()}`);
  const wasmRoot = join(root, "crates", "jazz-wasm");
  const pkg = join(wasmRoot, "pkg");
  mkdirSync(pkg, { recursive: true });
  return { root, wasmRoot, pkg, lock: join(root, "artifact.lock") };
}
function writePackage(path, marker) { for (const file of wasmPackageFiles) writeFileSync(join(path, file), `${marker}:${file}`); }
function markers(path) { return wasmPackageFiles.map((file) => readFileSync(join(path, file), "utf8")); }
function withLock(lock, run) {
  const previous = process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
  process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = lock;
  try { return run(); } finally { if (previous === undefined) delete process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH; else process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = previous; }
}

test("WASM producer rejects an incomplete staged generation without touching the prior package", () => {
  const { root, pkg, lock } = fixture();
  try { withLock(lock, () => {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "release");
    writeFileSync(join(stage.path, "jazz_wasm_bg.wasm"), "incomplete");
    assert.throws(() => publishWasmPackage(stage.path, pkg), /incomplete staged package/);
    assert.deepEqual(markers(pkg), wasmPackageFiles.map((file) => `old:${file}`));
  }); } finally { rmSync(root, { recursive: true, force: true }); }
});

test("a staged manifest failure preserves the prior published package", () => {
  const { root, pkg } = fixture();
  const previous = process.env.JAZZ_WASM_BUILD_FAULT;
  try {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "fast"); writePackage(stage.path, "new");
    process.env.JAZZ_WASM_BUILD_FAULT = "manifest-write";
    assert.throws(() => writeWasmStageManifest(stage.path, "fast"), /staged manifest write failure/);
    assert.deepEqual(markers(pkg), wasmPackageFiles.map((file) => `old:${file}`));
    rmSync(stage.path, { recursive: true, force: true });
  } finally { if (previous === undefined) delete process.env.JAZZ_WASM_BUILD_FAULT; else process.env.JAZZ_WASM_BUILD_FAULT = previous; rmSync(root, { recursive: true, force: true }); }
});

test("killed directory swap restores the old generation on the next locked producer", () => {
  const { root, wasmRoot, pkg, lock } = fixture();
  try { withLock(lock, () => {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "fast"); writePackage(stage.path, "new");
    const child = spawnSync(process.execPath, ["--input-type=module", "-e", `import { publishWasmPackage } from ${JSON.stringify(new URL("./build.mjs", import.meta.url).href)}; publishWasmPackage(${JSON.stringify(stage.path)}, ${JSON.stringify(pkg)})`], { env: { ...process.env, JAZZ_WASM_BUILD_FAULT: "after-old-moved", JAZZ_TEST_ARTIFACT_LOCK_PATH: lock } });
    assert.equal(child.signal, "SIGKILL");
    recoverWasmPackageTransaction(wasmRoot);
    assert.deepEqual(markers(pkg), wasmPackageFiles.map((file) => `old:${file}`));
  }); } finally { rmSync(root, { recursive: true, force: true }); }
});

test("two independent fast/release producers serialize and publish one internally consistent generation", async () => {
  const { root, pkg, lock } = fixture();
  const previous = process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
  process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = lock;
  try {
    writePackage(pkg, "old");
    const producer = `import { createWasmPackageStage, publishWasmPackage, wasmPackageFiles } from ${JSON.stringify(new URL("./build.mjs", import.meta.url).href)}; import { writeFileSync } from 'node:fs'; const [root, pkg, profile] = process.argv.slice(1); const stage=createWasmPackageStage(root, profile); for (const file of wasmPackageFiles) writeFileSync(stage.path+'/'+file, profile+':'+file); publishWasmPackage(stage.path,pkg,{profile});`;
    const run = (profile) => new Promise((resolveRun) => {
      const child = spawn(process.execPath, ["--input-type=module", "-e", producer, root, pkg, profile], { env: { ...process.env, JAZZ_TEST_ARTIFACT_LOCK_PATH: lock } });
      let stderr = ""; child.stderr.on("data", (chunk) => { stderr += chunk; });
      child.on("exit", (status) => resolveRun({ status, stderr }));
    });
    const [one, two] = await Promise.all([run("fast"), run("release")]);
    assert.equal(one.status, 0, one.stderr); assert.equal(two.status, 0, two.stderr);
    const final = markers(pkg); const generation = final[0].split(":")[0];
    assert.ok(["fast", "release"].includes(generation)); assert.ok(final.every((value) => value.startsWith(`${generation}:`)));
    assert.equal(existsSync(join(root, "crates", "jazz-wasm", ".pkg-transaction.json")), false);
  } finally { if (previous === undefined) delete process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH; else process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = previous; rmSync(root, { recursive: true, force: true }); }
});

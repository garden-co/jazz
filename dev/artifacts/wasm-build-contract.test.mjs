import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import test from "node:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createWasmPackageStage,
  publishWasmPackage,
  recoverWasmPackageTransaction,
  wasmPackageFiles,
  writeWasmStageManifest,
} from "./build.mjs";
import {
  acquireArtifactBuildLock,
  artifactBuildLease,
  verifyArtifactBuildLease,
} from "../gates/build-test-artifacts.mjs";

function fixture() {
  const root = join(tmpdir(), `jazz-wasm-artifact-${process.pid}-${Date.now()}-${Math.random()}`);
  const wasmRoot = join(root, "crates", "jazz-wasm");
  const pkg = join(wasmRoot, "pkg");
  mkdirSync(pkg, { recursive: true });
  return { root, wasmRoot, pkg, lock: join(root, "artifact.lock") };
}
function writePackage(path, marker) {
  for (const file of wasmPackageFiles) writeFileSync(join(path, file), `${marker}:${file}`);
}
function markers(path) {
  return wasmPackageFiles.map((file) => readFileSync(join(path, file), "utf8"));
}
function withLock(lock, run) {
  const previous = process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
  process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = lock;
  try {
    return run();
  } finally {
    if (previous === undefined) delete process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
    else process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = previous;
  }
}

test("WASM producer rejects an incomplete staged generation without touching the prior package", () => {
  const { root, pkg, lock } = fixture();
  try {
    withLock(lock, () => {
      writePackage(pkg, "old");
      const stage = createWasmPackageStage(root, "release");
      writeFileSync(join(stage.path, "jazz_wasm_bg.wasm"), "incomplete");
      assert.throws(() => publishWasmPackage(stage.path, pkg), /incomplete staged package/);
      assert.deepEqual(
        markers(pkg),
        wasmPackageFiles.map((file) => `old:${file}`),
      );
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a staged manifest failure preserves the prior published package", () => {
  const { root, pkg } = fixture();
  const previous = process.env.JAZZ_WASM_BUILD_FAULT;
  try {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "fast");
    writePackage(stage.path, "new");
    process.env.JAZZ_WASM_BUILD_FAULT = "manifest-write";
    assert.throws(
      () => writeWasmStageManifest(stage.path, "fast"),
      /staged manifest write failure/,
    );
    assert.deepEqual(
      markers(pkg),
      wasmPackageFiles.map((file) => `old:${file}`),
    );
    rmSync(stage.path, { recursive: true, force: true });
  } finally {
    if (previous === undefined) delete process.env.JAZZ_WASM_BUILD_FAULT;
    else process.env.JAZZ_WASM_BUILD_FAULT = previous;
    rmSync(root, { recursive: true, force: true });
  }
});

test("killed directory swap restores the old generation on the next locked producer", () => {
  const { root, wasmRoot, pkg, lock } = fixture();
  try {
    withLock(lock, () => {
      writePackage(pkg, "old");
      const stage = createWasmPackageStage(root, "fast");
      writePackage(stage.path, "new");
      const child = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `import { publishWasmPackage } from ${JSON.stringify(new URL("./build.mjs", import.meta.url).href)}; publishWasmPackage(${JSON.stringify(stage.path)}, ${JSON.stringify(pkg)})`,
        ],
        {
          env: {
            ...process.env,
            JAZZ_WASM_BUILD_FAULT: "after-old-moved",
            JAZZ_TEST_ARTIFACT_LOCK_PATH: lock,
          },
        },
      );
      assert.equal(child.signal, "SIGKILL");
      recoverWasmPackageTransaction(wasmRoot);
      assert.deepEqual(
        markers(pkg),
        wasmPackageFiles.map((file) => `old:${file}`),
      );
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("prepared journal before the first rename retains the intact old package", () => {
  const { root, wasmRoot, pkg } = fixture();
  try {
    writePackage(pkg, "old");
    const stage = createWasmPackageStage(root, "fast");
    writePackage(stage.path, "new");
    const hashes = Object.fromEntries(
      wasmPackageFiles.map((file) => [file, requireHash(join(stage.path, file))]),
    );
    writeFileSync(
      join(wasmRoot, ".pkg-transaction.json"),
      JSON.stringify({
        schema: 1,
        state: "prepared",
        hadCurrent: true,
        stage: stage.outDir,
        backup: ".pkg-backup-never-created",
        hashes,
      }),
    );
    recoverWasmPackageTransaction(wasmRoot);
    assert.deepEqual(
      markers(pkg),
      wasmPackageFiles.map((file) => `old:${file}`),
    );
    assert.equal(existsSync(stage.path), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

function requireHash(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}
test("recovery refuses external stage or backup symlinks", () => {
  const { root, wasmRoot, pkg } = fixture();
  const external = join(root, "external");
  try {
    mkdirSync(external);
    writePackage(external, "external");
    for (const [field, name] of [
      ["stage", ".pkg-stage-external"],
      ["backup", ".pkg-backup-external"],
    ]) {
      writePackage(pkg, "old");
      const stage = createWasmPackageStage(root, "fast");
      writePackage(stage.path, "new");
      const link = join(wasmRoot, name);
      symlinkSync(external, link);
      const hashes = Object.fromEntries(
        wasmPackageFiles.map((file) => [file, requireHash(join(stage.path, file))]),
      );
      const journal = {
        schema: 1,
        state: "old-moved",
        hadCurrent: true,
        stage: stage.outDir,
        backup: ".pkg-backup-safe",
        hashes,
      };
      journal[field] = name;
      writeFileSync(join(wasmRoot, ".pkg-transaction.json"), JSON.stringify(journal));
      assert.throws(() => recoverWasmPackageTransaction(wasmRoot), /must be a real directory/);
      assert.deepEqual(
        markers(external),
        wasmPackageFiles.map((file) => `external:${file}`),
      );
      // Node 24 treats a symlink-to-directory as a directory for rmSync unless
      // recursive deletion is requested. Unlink the link itself so cleanup can
      // never traverse the external directory this receipt protects.
      unlinkSync(link);
      rmSync(join(wasmRoot, ".pkg-transaction.json"), { force: true });
      rmSync(stage.path, { recursive: true, force: true });
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("inherited artifact leases require the exact live clone owner", () => {
  const { root, lock } = fixture();
  try {
    withLock(lock, () => {
      const acquired = acquireArtifactBuildLock(lock);
      const envLease = artifactBuildLease(acquired);
      const lease = {
        token: envLease.JAZZ_ARTIFACT_BUILD_LEASE,
        lockPath: envLease.JAZZ_ARTIFACT_BUILD_LOCK_PATH,
      };
      assert.deepEqual(verifyArtifactBuildLease(lease), lease);
      assert.throws(
        () => verifyArtifactBuildLease({ ...lease, lockPath: `${lock}-other` }),
        /different clone lock/,
      );
      assert.throws(
        () => verifyArtifactBuildLease({ ...lease, token: "forged" }),
        /missing or no longer owned/,
      );
      acquired.release();
      assert.throws(() => verifyArtifactBuildLease(lease), /missing or no longer owned/);
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("WASM publication accepts only the exact live inherited lease", () => {
  const { root, pkg, lock } = fixture();
  try {
    withLock(lock, () => {
      writePackage(pkg, "old");
      const held = acquireArtifactBuildLock(lock);
      const inherited = artifactBuildLease(held);
      const lease = {
        token: inherited.JAZZ_ARTIFACT_BUILD_LEASE,
        lockPath: inherited.JAZZ_ARTIFACT_BUILD_LOCK_PATH,
      };
      const stage = (marker) => {
        const created = createWasmPackageStage(root, "fast");
        writePackage(created.path, marker);
        return created;
      };
      const prior = wasmPackageFiles.map((file) => `old:${file}`);

      for (const [name, supplied] of [
        ["missing", {}],
        ["forged", { ...lease, token: "forged" }],
        ["different selected path", { ...lease, lockPath: `${lock}-other` }],
      ]) {
        const candidate = stage(name);
        assert.throws(
          () => publishWasmPackage(candidate.path, pkg, { lease: supplied }),
          /inherited artifact lease/,
          name,
        );
        assert.equal(existsSync(candidate.path), true, `${name} lease must not consume staging`);
        assert.deepEqual(markers(pkg), prior, `${name} lease must not alter the package`);
        rmSync(candidate.path, { recursive: true, force: true });
      }

      const accepted = stage("inherited");
      publishWasmPackage(accepted.path, pkg, { profile: "fast", lease });
      assert.deepEqual(
        markers(pkg),
        wasmPackageFiles.map((file) => `inherited:${file}`),
      );
      assert.equal(existsSync(lock), true, "an inherited lease must not release its parent lock");
      held.release();

      const dead = stage("dead");
      assert.throws(
        () => publishWasmPackage(dead.path, pkg, { lease }),
        /missing or no longer owned/,
      );
      assert.equal(existsSync(dead.path), true, "a dead lease must not consume staging");
      rmSync(dead.path, { recursive: true, force: true });
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("direct WASM publication acquires and releases the selected lock itself", () => {
  const { root, pkg, lock } = fixture();
  try {
    withLock(lock, () => {
      const stage = createWasmPackageStage(root, "fast");
      writePackage(stage.path, "direct");
      publishWasmPackage(stage.path, pkg, { profile: "fast" });
      assert.deepEqual(
        markers(pkg),
        wasmPackageFiles.map((file) => `direct:${file}`),
      );
      assert.equal(existsSync(lock), false, "direct publication must release its acquired lock");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("independent fast/release producers survive repeated lock handoffs and publish one internally consistent generation", async () => {
  const { root, pkg, lock } = fixture();
  const previous = process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
  process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = lock;
  try {
    writePackage(pkg, "old");
    const producer = `import { createWasmPackageStage, publishWasmPackage, wasmPackageFiles, writeWasmStageManifest } from ${JSON.stringify(new URL("./build.mjs", import.meta.url).href)}; import { writeFileSync } from 'node:fs'; const [root, pkg, profile] = process.argv.slice(1); const stage=createWasmPackageStage(root, profile); for (const file of wasmPackageFiles.filter((file)=>file!=='.jazz-artifact-manifest.json')) writeFileSync(stage.path+'/'+file, profile+':'+file); writeWasmStageManifest(stage.path,profile); publishWasmPackage(stage.path,pkg,{profile});`;
    const run = (profile) =>
      new Promise((resolveRun) => {
        const child = spawn(
          process.execPath,
          ["--input-type=module", "-e", producer, root, pkg, profile],
          { env: { ...process.env, JAZZ_TEST_ARTIFACT_LOCK_PATH: lock } },
        );
        let stderr = "";
        child.stderr.on("data", (chunk) => {
          stderr += chunk;
        });
        child.on("exit", (status) => resolveRun({ status, stderr }));
      });
    // More than one handoff makes the release-between-link-and-read race
    // observable without weakening the producer contract: each child is a
    // normal direct producer and must either own the receipt or wait for it.
    const producers = await Promise.all(
      ["fast", "release", "fast", "release", "fast", "release"].map(run),
    );
    for (const producer of producers) assert.equal(producer.status, 0, producer.stderr);
    const final = markers(pkg);
    const generation = final[0].split(":")[0];
    assert.ok(["fast", "release"].includes(generation));
    assert.ok(final.slice(0, -1).every((value) => value.startsWith(`${generation}:`)));
    const manifest = JSON.parse(readFileSync(join(pkg, ".jazz-artifact-manifest.json"), "utf8"));
    assert.equal(
      manifest.profile,
      generation,
      "manifest must be published with the winning generation",
    );
    assert.equal(existsSync(join(root, "crates", "jazz-wasm", ".pkg-transaction.json")), false);
  } finally {
    if (previous === undefined) delete process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH;
    else process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH = previous;
    rmSync(root, { recursive: true, force: true });
  }
});

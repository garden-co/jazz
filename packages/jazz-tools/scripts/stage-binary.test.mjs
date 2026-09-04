import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { chmod, link, mkdtemp, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import test from "node:test";
import { join } from "node:path";
import { stageBinary } from "./stage-binary.mjs";
import { TARGETS } from "./targets.mjs";
const [targetKey, fileName] = Object.entries(TARGETS)[0];
const [platform, arch] = targetKey.split("-");

test("stages a binary when source and mapped destination are the same lexical path", async () => {
  const nativeDir = await mkdtemp(join(tmpdir(), "jazz-stage-binary-"));
  const destination = join(nativeDir, fileName);
  const originalBytes = Buffer.from("same-path-binary");
  try {
    await writeFile(destination, originalBytes);
    if (process.platform !== "win32") await chmod(destination, 0o644);

    await assert.doesNotReject(stageBinary({ source: destination, platform, arch, nativeDir }));

    assert.deepEqual(await readFile(destination), originalBytes);
    if (process.platform !== "win32") {
      assert.equal((await stat(destination)).mode & 0o777, 0o755);
    }
  } finally {
    await rm(nativeDir, { recursive: true, force: true });
  }
});

test("preserves a staged binary reached through a filesystem alias", async () => {
  const nativeDir = await mkdtemp(join(tmpdir(), "jazz-stage-binary-"));
  const destination = join(nativeDir, fileName);
  const sourceAlias = join(nativeDir, "source-alias");
  const originalBytes = Buffer.from("aliased-binary");
  try {
    await writeFile(destination, originalBytes);
    if (process.platform !== "win32") await chmod(destination, 0o644);
    await link(destination, sourceAlias);

    await assert.doesNotReject(stageBinary({ source: sourceAlias, platform, arch, nativeDir }));

    assert.deepEqual(await readFile(destination), originalBytes);
    assert.deepEqual(await readFile(sourceAlias), originalBytes);
    const destinationStats = await stat(destination, { bigint: true });
    const aliasStats = await stat(sourceAlias, { bigint: true });
    assert.equal(destinationStats.ino, aliasStats.ino, "staging must preserve the hard link");
    if (process.platform !== "win32") {
      assert.equal((await stat(destination)).mode & 0o777, 0o755);
    }
  } finally {
    await rm(nativeDir, { recursive: true, force: true });
  }
});

test("replaces an existing destination with a distinct source binary", async () => {
  const nativeDir = await mkdtemp(join(tmpdir(), "jazz-stage-binary-"));
  const destination = join(nativeDir, fileName);
  const source = join(nativeDir, "new-source");
  const oldDestinationAlias = join(nativeDir, "old-destination-alias");
  const sourceBytes = Buffer.from("new-binary");
  try {
    await writeFile(destination, "old-binary");
    // Keep the old inode alive: replacing a cached executable must unlink it,
    // not overwrite its contents in place (even when the copied bytes match).
    await link(destination, oldDestinationAlias);
    await writeFile(source, sourceBytes);

    await assert.doesNotReject(stageBinary({ source, platform, arch, nativeDir }));

    assert.deepEqual(await readFile(destination), sourceBytes);
    assert.deepEqual(await readFile(source), sourceBytes);
    assert.equal(await readFile(oldDestinationAlias, "utf8"), "old-binary");
    const destinationStats = await stat(destination, { bigint: true });
    const sourceStats = await stat(source, { bigint: true });
    if (destinationStats.ino !== 0n && sourceStats.ino !== 0n) {
      assert.notEqual(destinationStats.ino, sourceStats.ino);
    }
  } finally {
    await rm(nativeDir, { recursive: true, force: true });
  }
});

// Some filesystems expose a device but no usable inode. Exercise that boundary
// while keeping all file operations real, including replacement and permissions.
function hideInodes(t) {
  const actualStat = fs.stat;
  t.mock.method(fs, "stat", async (...args) => {
    const result = await actualStat(...args);
    result.ino = args[1]?.bigint ? 0n : 0;
    return result;
  });
  syncBuiltinESMExports();
  t.after(() => {
    t.mock.restoreAll();
    syncBuiltinESMExports();
  });
}

test("preserves canonical aliases even when inode identity is unavailable", async (t) => {
  const nativeDir = await mkdtemp(join(tmpdir(), "jazz-stage-binary-"));
  t.after(() => rm(nativeDir, { recursive: true, force: true }));
  const destination = join(nativeDir, fileName);
  const sourceAlias = join(nativeDir, "source-symlink");
  await writeFile(destination, "canonical-binary");
  try {
    await symlink(destination, sourceAlias);
  } catch (error) {
    if (process.platform === "win32" && ["EPERM", "EACCES"].includes(error?.code)) {
      t.skip("Creating symbolic links requires additional Windows privileges");
      return;
    }
    throw error;
  }
  hideInodes(t);

  await stageBinary({ source: sourceAlias, platform, arch, nativeDir });

  assert.equal(await readFile(destination, "utf8"), "canonical-binary");
  assert.equal(await readFile(sourceAlias, "utf8"), "canonical-binary");
});

test("does not treat a shared device with unavailable inodes as the same file", async (t) => {
  const nativeDir = await mkdtemp(join(tmpdir(), "jazz-stage-binary-"));
  t.after(() => rm(nativeDir, { recursive: true, force: true }));
  const destination = join(nativeDir, fileName);
  const source = join(nativeDir, "distinct-source");
  await writeFile(destination, "old-binary");
  await writeFile(source, "replacement-binary");
  hideInodes(t);

  await stageBinary({ source, platform, arch, nativeDir });

  assert.equal(await readFile(destination, "utf8"), "replacement-binary");
  assert.equal(await readFile(source, "utf8"), "replacement-binary");
});

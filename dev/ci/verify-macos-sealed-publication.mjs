#!/usr/bin/env node
/** Verify the rename-and-seal sequence used by correctness artifact snapshots. */
import assert from "node:assert/strict";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

if (process.platform !== "darwin") {
  throw new Error("macOS sealed-publication probe must run on Darwin");
}

const root = mkdtempSync(join(tmpdir(), "jazz-sealed-publication-"));
try {
  const stage = mkdtempSync(join(root, ".stage-"));
  const child = join(stage, "sealed-child");
  mkdirSync(child);
  const leaf = join(child, "artifact");
  writeFileSync(leaf, "probe\n");
  // The parent remains writable for Darwin rename; descendants are already sealed.
  chmodSync(leaf, 0o444);
  chmodSync(child, 0o555);
  const published = join(root, "published");
  renameSync(stage, published);
  chmodSync(published, 0o555);
  assert.equal(statSync(published).mode & 0o222, 0, "published root is sealed");
  assert.equal(statSync(join(published, "sealed-child")).mode & 0o222, 0, "child is sealed");
  console.log("macOS sealed publication probe passed");
} finally {
  // This private scratch tree has no published authority after the probe.
  chmodSync(root, 0o700);
  try {
    chmodSync(join(root, "published"), 0o700);
    chmodSync(join(root, "published", "sealed-child"), 0o700);
  } catch {}
  rmSync(root, { recursive: true, force: true });
}

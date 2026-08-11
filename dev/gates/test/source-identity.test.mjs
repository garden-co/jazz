import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { sourceIdentity } from "../source-identity.mjs";

const git = (root, args) => {
  const result = spawnSync("git", ["-C", root, ...args], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
};
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-source-identity-"));
  git(root, ["init", "--quiet"]);
  git(root, ["config", "user.email", "test@example.invalid"]);
  git(root, ["config", "user.name", "Test"]);
  fs.writeFileSync(path.join(root, "tracked.txt"), "base\n");
  git(root, ["add", "tracked.txt"]);
  git(root, ["commit", "--quiet", "-m", "base"]);
  return root;
}
for (const [name, secret, mutate, restore] of [
  [
    "staged",
    "stage-private-value",
    (root) => {
      fs.writeFileSync(path.join(root, "tracked.txt"), "stage-private-value\n");
      git(root, ["add", "tracked.txt"]);
    },
    () => {},
  ],
  [
    "unstaged",
    "worktree-private-value",
    (root) => fs.writeFileSync(path.join(root, "tracked.txt"), "worktree-private-value\n"),
    () => {},
  ],
  [
    "untracked",
    "new-private-value",
    (root) => fs.writeFileSync(path.join(root, "new.txt"), "new-private-value\n"),
    () => {},
  ],
])
  test(`fingerprint changes for ${name} content without retaining it`, () => {
    const root = fixture(),
      clean = sourceIdentity(root);
    mutate(root);
    const dirty = sourceIdentity(root);
    assert.equal(clean.dirty, false);
    assert.equal(dirty.dirty, true);
    assert.notEqual(clean.fingerprint, dirty.fingerprint);
    assert.equal(JSON.stringify(dirty).includes(secret), false);
    restore(root);
  });

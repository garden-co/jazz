import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { checkedOutCommit, sourceIdentity } from "../source-identity.mjs";

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

test("identity reads survive a container checkout ownership boundary", () => {
  const root = fixture();
  const previous = process.env.GIT_TEST_ASSUME_DIFFERENT_OWNER;
  process.env.GIT_TEST_ASSUME_DIFFERENT_OWNER = "1";
  try {
    const identity = sourceIdentity(root);
    assert.equal(identity.dirty, false);
    assert.match(checkedOutCommit(root), /^[0-9a-f]{40}$/);
  } finally {
    if (previous === undefined) delete process.env.GIT_TEST_ASSUME_DIFFERENT_OWNER;
    else process.env.GIT_TEST_ASSUME_DIFFERENT_OWNER = previous;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("untracked nested repository sources retain tracked and untracked byte identity", () => {
  const root = fixture();
  try {
    const clean = sourceIdentity(root);
    const nested = path.join(root, "nested");
    fs.mkdirSync(nested);
    git(nested, ["init", "--quiet"]);
    fs.writeFileSync(path.join(nested, "tracked.rs"), "original source");
    git(nested, ["add", "tracked.rs"]);
    fs.writeFileSync(path.join(nested, "untracked.rs"), "untracked source");
    const initial = sourceIdentity(root);
    assert.equal(initial.dirty, true);
    assert.notEqual(initial.fingerprint, clean.fingerprint);
    fs.writeFileSync(path.join(nested, "tracked.rs"), "changed source");
    const changedTracked = sourceIdentity(root);
    assert.notEqual(changedTracked.fingerprint, initial.fingerprint);
    fs.writeFileSync(path.join(nested, "untracked.rs"), "changed untracked source");
    assert.notEqual(sourceIdentity(root).fingerprint, changedTracked.fingerprint);
    fs.rmSync(nested, { recursive: true });
    assert.equal(sourceIdentity(root).fingerprint, clean.fingerprint);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("nested repository identity applies root-relative exclusions at every depth", () => {
  const root = fixture();
  try {
    const nested = path.join(root, "nested", "deeper");
    fs.mkdirSync(nested, { recursive: true });
    git(path.join(root, "nested"), ["init", "--quiet"]);
    git(nested, ["init", "--quiet"]);
    fs.writeFileSync(path.join(nested, "source.rs"), "source");
    fs.writeFileSync(path.join(nested, "generated.bin"), "first output");
    const options = { excludePathspecs: ["nested/**/generated.bin"] };
    const before = sourceIdentity(root, options);
    fs.writeFileSync(path.join(nested, "generated.bin"), "second output");
    assert.equal(sourceIdentity(root, options).fingerprint, before.fingerprint);
    fs.writeFileSync(path.join(nested, "source.rs"), "changed source");
    assert.notEqual(sourceIdentity(root, options).fingerprint, before.fingerprint);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

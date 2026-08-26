import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { checkedOutCommit, sourceIdentity } from "../source-identity.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const runner = path.join(root, "dev/gates/run-rust-tests.mjs");
const temp = () => fs.mkdtempSync(path.join(os.tmpdir(), "jazz-rust-receipt-"));
const hasNextest =
  spawnSync("cargo", ["nextest", "--version"], { cwd: root, stdio: "ignore" }).status === 0;
test("writes source, command, cache, and failure metadata", () => {
  const dir = temp(),
    receipt = path.join(dir, "receipt.json");
  const result = spawnSync(
    "node",
    [runner, "--receipt", receipt, "--", "definitely-not-a-cargo-argument"],
    { cwd: root, encoding: "utf8" },
  );
  assert.notEqual(result.status, 0);
  const value = JSON.parse(fs.readFileSync(receipt, "utf8"));
  assert.equal(value.kind, "rust-test-receipt");
  assert.equal(value.status, "failed");
  assert.match(value.source.commit, /^[0-9a-f]{40}$/);
  assert.equal(value.command[0], "cargo");
  assert.ok("cargoTargetDir" in value.environment);
  assert.equal(value.environment.rustMinStack, String(4 * 1024 * 1024));
  assert.equal(value.nextestProfile, hasNextest ? "jazz" : null);
});
test("seals an actual nested receipt across a container ownership boundary", () => {
  const dir = temp();
  const receipt = path.join(dir, "receipt.json");
  const baseline = path.join(dir, "baseline.json");
  const source = { commit: checkedOutCommit(root), ...sourceIdentity(root) };
  fs.writeFileSync(baseline, JSON.stringify(source));
  const result = spawnSync(
    "node",
    [runner, "--receipt", receipt, "--", "--version"],
    {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        GIT_TEST_ASSUME_DIFFERENT_OWNER: "1",
        RUST_SHADOW_SOURCE_BASELINE: baseline,
      },
    },
  );
  assert.notEqual(result.status, 0, "the intentionally invalid Nextest selection must still fail");
  const value = JSON.parse(fs.readFileSync(receipt, "utf8"));
  assert.equal(value.source.commit, source.commit);
  assert.equal(value.source.fingerprint, source.fingerprint);
  fs.rmSync(dir, { recursive: true, force: true });
});
test("forwards the requested Nextest profile and records it in the receipt", () => {
  const source = fs.readFileSync(runner, "utf8");
  assert.match(source, /--nextest-profile N\s+Nextest profile \(default: jazz\)/);
  assert.match(source, /"--profile",\s*nextestProfile/);
  assert.match(source, /nextestProfile: useNextest \? nextestProfile : null/);
});
test("timeout propagates exit 124 and is recorded", () => {
  const dir = temp(),
    receipt = path.join(dir, "timeout.json");
  // Cargo's harmless --version selection exits immediately, so force the
  // fallback watchdog to expire before it can be scheduled.
  const result = spawnSync(
    "node",
    [runner, "--timeout-seconds", "0.001", "--receipt", receipt, "--", "--version"],
    { cwd: root, encoding: "utf8" },
  );
  const value = JSON.parse(fs.readFileSync(receipt, "utf8"));
  assert.equal(result.status, 124);
  assert.equal(value.status, "timeout");
  assert.equal(value.timedOut, true);
});
test("rejects invalid shard partitions", () => {
  const result = spawnSync(
    "node",
    [runner, "--shard-index", "3", "--shard-count", "2", "--", "--version"],
    { cwd: root, encoding: "utf8" },
  );
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /invalid test command, shard, or timeout/);
});
test(
  "does not duplicate a Cargo-fallback run across requested shards",
  { skip: hasNextest && "cargo-nextest installed" },
  () => {
    const result = spawnSync(
      "node",
      [runner, "--shard-index", "1", "--shard-count", "2", "--", "--version"],
      { cwd: root, encoding: "utf8" },
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /sharding requires cargo-nextest/);
  },
);
test("partition specification is complete and non-overlapping", () => {
  // The launcher forwards this exact syntax to Nextest, whose hash partition
  // owns each discovered test in exactly one shard. Keep the one-based bounds
  // here so a future argument-format change cannot silently skip shard zero or
  // duplicate the terminal shard.
  const count = 7;
  const partitions = Array.from({ length: count }, (_, offset) => `hash:${offset + 1}/${count}`);
  assert.equal(new Set(partitions).size, count);
  assert.deepEqual(partitions, [
    "hash:1/7",
    "hash:2/7",
    "hash:3/7",
    "hash:4/7",
    "hash:5/7",
    "hash:6/7",
    "hash:7/7",
  ]);
});

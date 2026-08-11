import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const hasNextest = spawnSync("cargo", ["nextest", "--version"], { cwd: root }).status === 0;
function inventory(partition) {
  const args = [
    "nextest",
    "list",
    "--workspace",
    "--lib",
    "--bins",
    "--tests",
    "--features",
    "test",
    "--message-format",
    "json",
  ];
  if (partition) args.push("--partition", partition);
  const result = spawnSync("cargo", args, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  assert.equal(result.status, 0, result.stderr);
  return new Set(
    result.stdout
      .split(/\r?\n/)
      .filter(Boolean)
      .map(JSON.parse)
      .filter((row) => row.type === "test")
      .map((row) => `${row.binary_id}\0${row.name}`),
  );
}
test(
  "nextest hash partitions cover its real inventory once",
  { skip: !hasNextest && "cargo-nextest unavailable; CI installs and exercises this check" },
  () => {
    const all = inventory();
    const one = inventory("hash:1/2");
    const two = inventory("hash:2/2");
    assert.ok(all.size > 0);
    assert.equal([...one].filter((id) => two.has(id)).length, 0);
    assert.deepEqual(new Set([...one, ...two]), all);
  },
);

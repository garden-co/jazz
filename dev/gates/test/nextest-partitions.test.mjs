import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const hasNextest = spawnSync("cargo", ["nextest", "--version"], { cwd: root }).status === 0;
const fixture = fs.readFileSync(
  path.join(root, "dev/gates/test/fixtures/nextest-list-0.9.143.json"),
  "utf8",
);
const isPlainMap = (value) => typeof value === "object" && value !== null && !Array.isArray(value);
const filterStatuses = new Set(["matches", "mismatch"]);

function inventoryFromNextestJson(output) {
  const document = JSON.parse(output);
  assert.ok(isPlainMap(document), "nextest list must emit one JSON object");
  assert.ok(
    Number.isInteger(document["test-count"]) && document["test-count"] >= 0,
    "nextest list is missing a non-negative integer test-count",
  );
  assert.ok(isPlainMap(document["rust-suites"]), "nextest list is missing rust-suites map");

  const all = new Set();
  const selected = new Set();
  for (const suite of Object.values(document["rust-suites"])) {
    assert.ok(isPlainMap(suite), "nextest rust-suites entry must be an object");
    assert.equal(typeof suite?.["binary-id"], "string", "nextest suite is missing binary-id");
    assert.ok(isPlainMap(suite.testcases), "nextest suite is missing testcases map");
    for (const [name, testcase] of Object.entries(suite.testcases)) {
      assert.ok(isPlainMap(testcase), "nextest testcase must be an object");
      assert.ok(isPlainMap(testcase["filter-match"]), "nextest testcase is missing filter-match");
      const status = testcase["filter-match"].status;
      assert.ok(
        filterStatuses.has(status),
        `nextest testcase has unsupported filter-match status: ${String(status)}`,
      );
      assert.equal(
        typeof testcase.ignored,
        "boolean",
        "nextest testcase is missing boolean ignored",
      );
      const id = `${suite["binary-id"]}\0${name}`;
      assert.ok(!all.has(id), `duplicate nextest testcase: ${id}`);
      all.add(id);
      if (status === "matches" && !testcase.ignored) selected.add(id);
    }
  }
  assert.equal(all.size, document["test-count"], "nextest test-count disagrees with rust-suites");
  return selected;
}

test("nextest 0.9.143 single-document output selects only partition matches", () => {
  assert.deepEqual(
    inventoryFromNextestJson(fixture),
    new Set(["pkg::unit\0alpha", "pkg::integration\0gamma"]),
  );
});

test("nextest JSON parser detects a planted partition-membership regression", () => {
  const planted = fixture.replace(
    '"beta": { "kind": "test", "ignored": false, "filter-match": { "status": "mismatch" } }',
    '"beta": { "kind": "test", "ignored": false, "filter-match": { "status": "matches" } }',
  );
  assert.notDeepEqual(inventoryFromNextestJson(planted), inventoryFromNextestJson(fixture));
});

test("nextest JSON parser rejects unknown and missing partition membership", () => {
  const unknown = fixture.replace('"status": "mismatch"', '"status": "future-status"');
  const missing = fixture.replace('"filter-match": { "status": "mismatch" }', '"filter-match": {}');
  assert.throws(
    () => inventoryFromNextestJson(unknown),
    /unsupported filter-match status: future-status/,
  );
  assert.throws(
    () => inventoryFromNextestJson(missing),
    /unsupported filter-match status: undefined/,
  );
});

test("nextest JSON parser rejects missing ignored state", () => {
  assert.throws(
    () => inventoryFromNextestJson(fixture.replace('"ignored": true, ', "")),
    /missing boolean ignored/,
  );
});

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
  return inventoryFromNextestJson(result.stdout);
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

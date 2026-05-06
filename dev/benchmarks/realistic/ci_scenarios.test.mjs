import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

function readJson(path) {
  return JSON.parse(readFileSync(new URL(path, import.meta.url), "utf8"));
}

test("CI scenario fixtures cap the heavy workloads that have been timing out in storage-backed CI", () => {
  const w1Ci = readJson("./ci/scenarios/w1_interactive.json");
  assert.equal(w1Ci.operation_count, 75000);

  const r1Ci = readJson("./ci/scenarios/r1_crud_sustained.json");
  assert.equal(r1Ci.operation_count, 256);

  const r2Ci = readJson("./ci/scenarios/r2_reads_sustained.json");
  assert.equal(r2Ci.operation_count, 256);

  const r2ChurnCi = readJson("./ci/scenarios/r2_reads_with_churn.json");
  assert.equal(r2ChurnCi.operation_count, 256);

  const r4Ci = readJson("./ci/scenarios/r4_fanout_updates.json");
  assert.equal(r4Ci.operation_count, 8);
  assert.deepEqual(r4Ci.fanout_clients, [10, 20]);

  const r5Ci = readJson("./ci/scenarios/r5_permission_recursive.json");
  assert.equal(r5Ci.operation_count, 128);
  assert.equal(r5Ci.shared_chain_depth, 4);
  assert.equal(r5Ci.docs_per_folder, 24);
  assert.equal(r5Ci.denied_docs, 96);
  assert.deepEqual(r5Ci.recursive_depths, [1, 3, 6]);

  const r7Ci = readJson("./ci/scenarios/r7_hotspot_history.json");
  assert.equal(r7Ci.operation_count, 1024);

  const r9Ci = readJson("./ci/scenarios/r9_subscribed_write_path.json");
  assert.equal(r9Ci.scale, 500);
});

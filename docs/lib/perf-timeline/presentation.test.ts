import { test } from "node:test";
import assert from "node:assert/strict";
import {
  estimatedSeconds,
  displayedTime,
  formatThroughput,
  getBenchmarkMetadata,
} from "./presentation.ts";

test("estimates divide time by exactly five and always carry an asterisk", () => {
  assert.equal(estimatedSeconds(10), 2);
  assert.equal(displayedTime(10, true), "2 s*");
  assert.equal(displayedTime(10, false), "10 s");
  assert.equal(displayedTime(0.005, true), "1 ms*");
});
test("estimated throughput is five times measured, with distinct labeling", () => {
  const meta = getBenchmarkMetadata("sequential_insert_1350_rocksdb")!;
  assert.equal(formatThroughput(6, meta), "225 inserts/s");
  assert.equal(formatThroughput(6, meta, true), "1,125 inserts/s*");
  assert.equal(formatThroughput(0, meta), "—");
});

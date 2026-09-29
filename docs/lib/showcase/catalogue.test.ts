import assert from "node:assert/strict";
import test from "node:test";
import { benchmarkMetadata } from "../../../dev/benchmarks/metadata/index.ts";
import { heroExamples, placeBenchmark } from "./catalogue.ts";

const sourceOf = (name: string) => benchmarkMetadata.find((entry) => entry.name === name)?.source;

test("every metric card names a current CodSpeed benchmark of its own example", () => {
  for (const example of heroExamples) {
    for (const metric of example.metrics) {
      const source = sourceOf(metric.benchmark);
      assert.ok(source, `${example.id}: ${metric.benchmark} has no benchmark metadata`);
      assert.equal(placeBenchmark(metric.benchmark, source), example.id, metric.benchmark);
    }
  }
});

test("every example benchmark with metadata has exactly one place on the page", () => {
  for (const entry of benchmarkMetadata) {
    assert.notEqual(placeBenchmark(entry.name, entry.source), null, entry.name);
  }
  assert.equal(
    placeBenchmark(
      "first_sync_local_relay_27518_rocksdb",
      sourceOf("first_sync_local_relay_27518_rocksdb"),
    ),
    "adopter-workloads",
  );
});

test("engine results without metadata go to the engine section; retired names are hidden", () => {
  assert.equal(placeBenchmark("ivm[100]", undefined), "engine");
  assert.equal(placeBenchmark("prepared_warm[5000]", undefined), "engine");
  assert.equal(placeBenchmark("query_board_profile_s_memory", undefined), null);
  assert.equal(placeBenchmark("chat_open_chat[10000]", undefined), null);
});

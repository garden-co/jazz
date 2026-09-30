import assert from "node:assert/strict";
import test from "node:test";
import { benchmarkMetadata } from "../../../dev/benchmarks/metadata/index.ts";
import {
  engineBenchmarks,
  groupBenchmarks,
  heroExamples,
  moreBenchmarkSections,
  placeBenchmark,
} from "./catalogue.ts";

const sourceOf = (name: string) => benchmarkMetadata.find((entry) => entry.name === name)?.source;

test("every metric card names a current CodSpeed benchmark of its own example", () => {
  for (const example of heroExamples) {
    for (const metric of example.metrics) {
      const source = sourceOf(metric.benchmark);
      assert.ok(source, `${example.id}: ${metric.benchmark} has no benchmark metadata`);
      assert.equal(placeBenchmark(source), example.id, metric.benchmark);
    }
  }
});

test("every example benchmark with metadata has exactly one place on the page", () => {
  for (const entry of benchmarkMetadata) {
    assert.notEqual(placeBenchmark(entry.source), null, entry.name);
  }
  assert.equal(
    placeBenchmark(sourceOf("first_sync_local_relay_27518_rocksdb")),
    "adopter-workloads",
  );
});

test("engine rows are the per-merge Groove cases, one per id and scenario", () => {
  const engine = moreBenchmarkSections.find((section) => section.id === "engine");
  assert.equal(engine?.benchmarks, engineBenchmarks);
  assert.equal(new Set(engineBenchmarks.map((row) => row.id)).size, engineBenchmarks.length);
  assert.equal(new Set(engineBenchmarks.map((row) => row.uri)).size, engineBenchmarks.length);
  for (const row of engineBenchmarks) {
    // Only the sizes CodSpeed measures on every merge (GROOVE_BENCH_SWEEP unset).
    assert.match(row.name, /^(prepared_warm\[5000\]|prepared_cold\[5000\]|ivm\[100\])$/);
    assert.ok(row.uri.endsWith(`::${row.name}`), row.uri);
    assert.ok(row.scenario, row.uri);
  }
  // Each name is listed once per scenario of its bench.
  const scenarios = (name: string) =>
    engineBenchmarks.filter((row) => row.name === name).map((row) => row.scenario);
  assert.deepEqual(scenarios("prepared_cold[5000]"), ["Author posts", "Feed", "Top-20 feed"]);
  assert.deepEqual(scenarios("ivm[100]"), ["Feed", "Top-20 feed", "Tasks"]);
});

test("engine rows are keyed by id; retired names and unmeasured sizes are hidden", () => {
  type E = { bench: { id: string; name: string }; median: number };
  const entry = (id: string, name: string, median: number): E => ({ bench: { id, name }, median });
  const [authorPosts, feed, top20] = engineBenchmarks.filter(
    (row) => row.name === "prepared_cold[5000]",
  );
  const all = [
    entry(authorPosts.id, "prepared_cold[5000]", 0.218),
    entry(feed.id, "prepared_cold[5000]", 0.457),
    entry(top20.id, "prepared_cold[5000]", 0.475),
    entry("retired-size", "prepared_cold[500]", 0.02),
    entry("retired-name", "query_board_profile_s_memory", 0.1),
  ];
  // As the page builds them: one entry per id, and the newest per name.
  const byId = new Map(all.map((e) => [e.bench.id, e]));
  const byName = new Map(all.map((e) => [e.bench.name, e]));
  const groups = groupBenchmarks(byName, byId, sourceOf);
  assert.deepEqual(
    groups.get("engine")?.map((row) => [row.bench.name, row.scenario, row.median]),
    [
      ["prepared_cold[5000]", "Author posts", 0.218],
      ["prepared_cold[5000]", "Feed", 0.457],
      ["prepared_cold[5000]", "Top-20 feed", 0.475],
    ],
  );
  const listed = [...groups.values()].flat().map((row) => row.bench.id);
  assert.ok(!listed.includes("retired-size"));
  assert.ok(!listed.includes("retired-name"));
  assert.equal(placeBenchmark(undefined), null);
});

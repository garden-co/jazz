import assert from "node:assert/strict";
import { statSync } from "node:fs";
import test from "node:test";
import { MAX_BYTES } from "../../scripts/example-videos/encode.mjs";
import { loadWalkthrough, walkthroughIds } from "../../scripts/example-videos/walkthrough.mjs";
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

test("every walkthrough video is a committed MP4 under the size budget, with a JPEG poster", () => {
  const publicFile = (path: string) => new URL(`../../public${path}`, import.meta.url);
  for (const example of heroExamples) {
    if (!example.video) {
      assert.ok(example.plannedVideo, `${example.id}: no video and no plannedVideo`);
      continue;
    }
    const { src, poster, caption } = example.video;
    assert.equal(src, `/examples/videos/${example.id}.mp4`);
    assert.equal(poster, `/examples/videos/${example.id}.jpg`);
    assert.ok(caption.trim(), `${example.id}: empty caption`);
    assert.ok(statSync(publicFile(src)).size <= MAX_BYTES, `${src} is over ${MAX_BYTES} bytes`);
    assert.ok(statSync(publicFile(poster)).size > 0, `${poster} is empty`);
  }
});

test("every walkthrough video has a storyboard, and its caption is the storyboard's summary", async () => {
  const ids = await walkthroughIds();
  for (const example of heroExamples) {
    if (!example.video) continue;
    assert.ok(
      ids.includes(example.id),
      `${example.id}: no walkthroughs/${example.id}.storyboard.ts`,
    );
    const { storyboard } = await loadWalkthrough(example.id);
    assert.equal(example.video.caption, storyboard.summary, example.id);
  }
});

test("every storyboard beat is one the recorder knows, and every action exists", async () => {
  const kinds = [
    "caption",
    "title",
    "do",
    "wait",
    "wifi",
    "full",
    "split",
    "show",
    "poster",
    "see",
    "notSee",
  ];
  for (const id of await walkthroughIds()) {
    const { storyboard, actions, app, server } = await loadWalkthrough(id);
    assert.equal(typeof app, "string", `${id}: no app`);
    assert.equal(typeof server, "function", `${id}: no server`);
    const beats = [...(storyboard.offCamera ?? []), ...storyboard.opening, ...storyboard.beats];
    for (const beat of beats) {
      const kind = kinds.find((k) => k in beat);
      assert.ok(kind, `${id}: unknown beat ${JSON.stringify(beat)}`);
      if ("do" in beat)
        assert.equal(typeof actions[beat.do], "function", `${id}: no action ${beat.do}`);
      const device = "on" in beat ? beat.on : "full" in beat ? beat.full : undefined;
      if (device) assert.ok(device in storyboard.devices, `${id}: no device ${device}`);
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

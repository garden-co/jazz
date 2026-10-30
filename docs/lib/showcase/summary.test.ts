import assert from "node:assert/strict";
import test from "node:test";
import type { Benchmark, Point, Stage } from "../perf-timeline/model.ts";
import { change, stitchFormerHistory, summarize } from "./summary.ts";

let serial = 0;
function point(stage: Stage, date: string, median: number, extra: Partial<Point> = {}): Point {
  serial++;
  return {
    min: median,
    median,
    max: median,
    measuredAt: date,
    backfill: null,
    runId: `run${serial}`,
    resultId: `result${serial}`,
    date,
    sha: `${serial}`.padStart(40, "0"),
    title: "commit",
    branch: stage === "open" ? "perf/x" : "main",
    pr: stage === "open" ? 1 : null,
    prStatus: stage === "open" ? "OPEN" : null,
    stage,
    release: null,
    includedInRelease: null,
    runStatus: "COMPLETED",
    series: stage === "open" ? "pr:1" : "main",
    ...extra,
  };
}
const bench = (points: Point[]): Benchmark => ({ id: "b", name: "b", points });

test("headline is the newest released measurement, one history entry per release", () => {
  const summary = summarize(
    bench([
      point("released", "2026-09-01T00:00:00Z", 4, { includedInRelease: "v1" }),
      point("released", "2026-09-02T00:00:00Z", 3, { includedInRelease: "v1" }),
      point("released", "2026-09-10T00:00:00Z", 2, { release: "v2" }),
      point("main", "2026-09-11T00:00:00Z", 1.5),
      point("open", "2026-09-12T00:00:00Z", 0.1),
    ]),
  )!;
  assert.equal(summary.basis, "release");
  assert.equal(summary.label, "v2");
  assert.equal(summary.headline.median, 2);
  assert.deepEqual(
    summary.history.map((h) => [h.label, h.point.median]),
    [
      ["v1", 3],
      ["v2", 2],
    ],
  );
  assert.equal(summary.unreleased?.median, 1.5);
});

test("falls back to main history when no release is attributable, never to open PRs", () => {
  const summary = summarize(
    bench([
      point("main", "2026-09-01T00:00:00Z", 4),
      point("main", "2026-09-02T00:00:00Z", 3),
      point("open", "2026-09-03T00:00:00Z", 0.1),
    ]),
  )!;
  assert.equal(summary.basis, "main");
  assert.equal(summary.headline.median, 3);
  assert.equal(summary.history.length, 2);
  assert.equal(summary.unreleased, null);
  assert.equal(summarize(bench([point("open", "2026-09-03T00:00:00Z", 1)])), null);
});

test("change is negative when faster", () => {
  assert.equal(change(2, 1), -0.5);
});

test("a declared-equivalent rename keeps its former release history", () => {
  const stitched = new Map([["new_name", "old_name"]]);
  const old: Benchmark = {
    id: "old",
    name: "old_name",
    points: [
      point("released", "2026-08-01", 4, { release: "v2.0.0-alpha.54" }),
      point("released", "2026-09-01", 3, { release: "v2.0.0-alpha.58" }),
    ],
  };
  const current: Benchmark = {
    id: "new",
    name: "new_name",
    points: [point("main", "2026-09-29", 2)],
  };
  const unrelated: Benchmark = {
    id: "other",
    name: "other",
    points: [point("main", "2026-09-29", 1)],
  };
  const result = stitchFormerHistory([old, current, unrelated], stitched);
  const merged = result.find((bench) => bench.name === "new_name")!;
  assert.equal(merged.id, "new");
  assert.deepEqual(
    merged.points.map((p) => p.median),
    [4, 3, 2],
  );
  const summary = summarize(merged)!;
  assert.deepEqual(
    summary.history.map((entry) => entry.label),
    ["v2.0.0-alpha.54", "v2.0.0-alpha.58"],
  );
  assert.equal(summary.unreleased?.median, 2);
  assert.equal(
    result.find((bench) => bench.name === "other"),
    unrelated,
  );
  // Before the new name has results, the former's history stands in for it.
  const early = stitchFormerHistory([old], stitched).find((bench) => bench.name === "new_name")!;
  assert.deepEqual(
    early.points.map((p) => p.median),
    [4, 3],
  );
  // Names that are not declared equivalent are left alone.
  assert.deepEqual(stitchFormerHistory([old, current], new Map()), [old, current]);
});

test("the StagePlan task-list cards continue the todo suite's history", () => {
  const stitched = stitchFormerHistory([
    { id: "a", name: "sequential_insert_1350_rocksdb", points: [point("main", "2026-09-01", 3)] },
    { id: "b", name: "query_board_profile_s_rocksdb", points: [point("main", "2026-09-01", 0.04)] },
  ]);
  assert.ok(stitched.some((bench) => bench.name === "stage_plan_add_task_1350"));
  // W1 moves restart their history.
  assert.ok(!stitched.some((bench) => bench.name === "stage_plan_open_board"));
});

import assert from "node:assert/strict";
import test from "node:test";
import type { Benchmark, Point, Stage } from "../perf-timeline/model.ts";
import { change, summarize } from "./summary.ts";

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

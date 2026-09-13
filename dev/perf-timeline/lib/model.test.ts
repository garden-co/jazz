import { test } from "node:test";
import assert from "node:assert/strict";
import { buildTimeline, formatTime, calendarDay, type RawRun } from "./model.ts";

const run = (id: string, branch = "main", prStatus?: string): RawRun => ({
  id,
  date: "2026-09-13T12:00:00Z",
  status: "COMPLETED",
  event: "Push",
  commit: {
    hash: id,
    message: "checkpoint",
    branch: {
      name: branch,
      pullRequest: prStatus
        ? { number: Number(id.replace(/\D/g, "")) || 1, title: "trial", status: prStatus }
        : null,
    },
  },
  results: [
    {
      id: `result-${id}`,
      benchmark: { id: "bench-1", name: "read" },
      walltime: { min: 0.9, median: 1, max: 1.1 },
    },
  ],
});

test("only exact releases, main and open PRs are retained throughout the dataset", () => {
  const data = buildTimeline(
    [
      run("release"),
      run("main"),
      run("1", "feature", "OPEN"),
      run("2", "old", "MERGED"),
      run("3", "abandoned", "CLOSED"),
      run("4", "unknown"),
    ],
    [{ name: "v2.0.0", sha: "release", url: "https://example.com/tag" }],
  );
  const points = new Map(data.benchmarks[0].points.map((p) => [p.sha, p]));
  assert.equal(points.get("release")?.stage, "released");
  assert.equal(points.get("main")?.stage, "main");
  assert.equal(points.get("1")?.stage, "open");
  assert.equal(points.has("2"), false);
  assert.equal(points.has("3"), false);
  assert.equal(points.has("4"), false);
  assert.equal(data.excludedRuns, 3);
  assert.equal(points.size, 3);
});

test("benchmarks with only excluded runs disappear from navigation data", () => {
  assert.deepEqual(
    buildTimeline([run("past", "trial", "MERGED"), run("other", "other")], []).benchmarks,
    [],
  );
});

test("calendar-day labels explicitly use UTC across midnight and year boundaries", () => {
  assert.equal(calendarDay("2026-09-13T23:30:00-07:00"), "2026-09-14");
  assert.equal(calendarDay("2026-01-01T00:30:00+02:00"), "2025-12-31");
});

test("does not fabricate released timings from neighboring commits", () => {
  const data = buildTimeline(
    [run("before"), run("after")],
    [{ name: "v2.0.0", sha: "unmeasured", url: "https://example.com" }],
  );
  assert.equal(data.benchmarks[0].points.length, 2);
  assert.ok(data.benchmarks[0].points.every((p) => p.stage === "main"));
});

test("retains independent reruns, deduplicates result IDs and sorts chronologically", () => {
  const early = run("early");
  early.date = "2026-09-12T12:00:00Z";
  const later = run("later");
  later.commit.hash = early.commit.hash;
  const data = buildTimeline([later, early, later], []);
  assert.deepEqual(
    data.benchmarks[0].points.map((p) => p.runId),
    ["early", "later"],
  );
});

test("null simulation metrics and incomplete jobs are not zero wallclock points", () => {
  const simulation = run("simulation");
  simulation.results[0].walltime = null;
  const pending = run("pending");
  pending.results = [];
  pending.status = "PROCESSING";
  const invalid = run("invalid");
  invalid.results[0].walltime!.median = NaN;
  const zero = run("zero");
  zero.results[0].walltime!.min = 0;
  const inverted = run("inverted");
  inverted.results[0].walltime!.min = 2;
  const partial = run("partial");
  partial.status = "FAILURE";
  const data = buildTimeline([simulation, pending, invalid, zero, inverted, partial], []);
  assert.equal(data.excludedResults, 4);
  assert.equal(data.benchmarks[0].points.length, 1);
  assert.equal(data.benchmarks[0].points[0].runStatus, "FAILURE");
});

test("benchmark identity, not display name, separates measurements", () => {
  const other = run("other");
  other.results[0].benchmark.id = "bench-2";
  assert.equal(buildTimeline([run("first"), other], []).benchmarks.length, 2);
});

test("formats seconds without confusing milliseconds or microseconds", () => {
  assert.equal(formatTime(42.154), "42.15 s");
  assert.equal(formatTime(0.042154), "42.15 ms");
  assert.equal(formatTime(0.000042154), "42.15 µs");
});

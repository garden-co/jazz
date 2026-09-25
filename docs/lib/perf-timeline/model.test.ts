import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildTimeline,
  mayBeAdmitted,
  formatTime,
  calendarDay,
  checkpoint,
  plotGeometry,
  type RawRun,
} from "./model.ts";

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

test("results are only needed for runs mayBeAdmitted accepts", () => {
  // The loader fetches results only for runs mayBeAdmitted accepts, so every
  // run buildTimeline admits must pass it, and dropping the others' results
  // must not change the timeline.
  const releases = [{ name: "v2.0.0", sha: "tagged", url: "https://example.com/tag" }];
  const harness = { ...run("harness", "bench/backfill"), date: "2026-09-14T12:00:00Z" };
  const backfills = [
    {
      releaseTag: "v2.0.0",
      engineSha: "tagged",
      harnessSha: "harness",
      harnessSourceSha: "source",
      effectiveDate: "2026-09-10T00:00:00Z",
      dateSource: "npm jazz-tools time[2.0.0]",
      workflowUrl: "https://example.com/workflow",
      receipts: [{ runId: "harness", resultId: "result-harness", benchmarkName: "read" }],
    },
  ];
  const runs = [
    run("main"),
    { ...run("tagged", "release"), date: "2026-09-12T12:00:00Z" },
    run("1", "feature", "OPEN"),
    run("2", "old", "MERGED"),
    run("3", "abandoned", "CLOSED"),
    run("4", "unknown"),
    harness,
  ];
  const full = buildTimeline(runs, releases, "now", new Map(), backfills);
  for (const r of runs) {
    const alone = buildTimeline([r], releases, "now", new Map(), backfills);
    if (alone.excludedRuns === 0) assert.ok(mayBeAdmitted(r, releases, backfills), r.id);
  }
  const trimmed = runs.map((r) =>
    mayBeAdmitted(r, releases, backfills) ? r : { ...r, results: [] },
  );
  assert.deepEqual(buildTimeline(trimmed, releases, "now", new Map(), backfills), full);
  assert.deepEqual(
    runs.filter((r) => mayBeAdmitted(r, releases, backfills)).map((r) => r.id),
    ["main", "tagged", "1", "harness"],
  );
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

test("proven main ancestors are released history without becoming release measurements", () => {
  const data = buildTimeline(
    [run("ancestor"), run("after"), run("1", "trial", "OPEN"), run("2", "closed", "MERGED")],
    [{ name: "v2.0.0", sha: "tag", url: "https://example.com" }],
    undefined,
    new Map([
      ["ancestor", "v2.0.0"],
      ["1", "v2.0.0"],
      ["2", "v2.0.0"],
    ]),
  );
  const points = new Map(data.benchmarks[0].points.map((p) => [p.sha, p]));
  const ancestor = points.get("ancestor")!;
  assert.equal(ancestor.stage, "released");
  assert.equal(ancestor.includedInRelease, "v2.0.0");
  assert.equal(ancestor.release, null);
  assert.equal(checkpoint(ancestor), "ancesto");
  assert.equal(points.get("after")?.stage, "main");
  assert.equal(points.get("1")?.stage, "open");
  assert.equal(points.has("2"), false);
});

test("shared plot geometry preserves full-history peaks and zero/log domains", () => {
  const points = buildTimeline(
    Array.from({ length: 52 }, (_, i) => {
      const r = run(String(i));
      r.results[0].walltime = {
        min: i === 0 ? 100 : 18,
        median: i === 0 ? 110 : 19,
        max: i === 0 ? 120 : 20,
      };
      r.date = new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString();
      return r;
    }),
    [],
  ).benchmarks[0].points;
  const geometry = plotGeometry(points, false, false);
  assert.equal(geometry.x(0), 0);
  assert.equal(geometry.x(51), 1);
  assert.equal(geometry.tick(0), 0);
  assert.ok(geometry.tick(1) > 110);
  assert.ok(geometry.y(19) > 0.8);
  assert.ok(plotGeometry(points, false, true).tick(1) > 120);
  assert.ok(plotGeometry(points, true, false).tick(0) > 0);
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

test("timing ticks are rounded in displayed units, mirrored without altering raw data", () => {
  const r = run("ticks");
  r.results[0].walltime = { min: 0.2, median: 0.9, max: 1 };
  const points = buildTimeline([r], []).benchmarks[0].points;
  const estimated = plotGeometry(points, false, false, 5);
  assert.deepEqual(
    estimated.ticks.map((v) => Number((v / 5).toPrecision(12))),
    [0, 0.05, 0.1, 0.15, 0.2],
  );
  assert.equal(points[0].median, 0.9);
  assert.ok(estimated.y(0.9) > 0 && estimated.y(0.9) < 1);
  const log = plotGeometry(points, true, true, 5);
  assert.deepEqual(
    log.ticks.map((v) => Number((v / 5).toPrecision(12))),
    [0.02, 0.05, 0.1, 0.2, 0.5],
  );
  assert.ok(log.y(points[0].min) < 1);
  assert.ok(log.y(points[0].max) > 0);
});

test("log ticks stay bounded for wide ranges and usable for identical submillisecond samples", () => {
  const small = run("small");
  small.results[0].walltime = { min: 1e-6, median: 1e-6, max: 1e-6 };
  const large = run("large");
  large.results[0].walltime = { min: 100, median: 100, max: 100 };
  const points = buildTimeline([small, large], []).benchmarks[0].points;
  const wide = plotGeometry(points, true, true);
  assert.ok(wide.ticks.length <= 10);
  assert.ok(wide.ticks.every((v) => Math.abs(Math.log10(v) - Math.round(Math.log10(v))) < 1e-10));
  const single = plotGeometry([points.find((p) => p.sha === "small")!], true, true, 5);
  assert.ok(single.ticks.length >= 2);
  assert.ok(Number.isFinite(single.y(1e-6)));
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

test("historical backfill places only approved receipts on release day with true provenance", () => {
  const measured = run("harness", "bench/historical");
  measured.results.push({ ...measured.results[0], id: "carried-forward" });
  const release = { name: "v2.0.0", sha: "engine", url: "https://example.com/tag" };
  const backfill = {
    releaseTag: release.name,
    engineSha: release.sha,
    harnessSha: "harness",
    harnessSourceSha: "source",
    effectiveDate: "2026-09-10T04:26:57.178Z",
    dateSource: "release publication",
    workflowUrl: "https://example.com/workflow",
    receipts: [{ runId: "harness", resultId: "result-harness", benchmarkName: "read" }],
  };
  const data = buildTimeline([measured], [release], undefined, undefined, [backfill]);
  assert.equal(data.benchmarks[0].points.length, 1);
  const point = data.benchmarks[0].points[0];
  assert.equal(point.stage, "released");
  assert.equal(point.series, "main");
  assert.equal(point.date, backfill.effectiveDate);
  assert.equal(point.measuredAt, measured.date);
  assert.equal(point.sha, "harness");
  assert.equal(point.backfill?.engineSha, "engine");
  assert.equal(point.release, null);
  assert.equal(point.includedInRelease, null);
  assert.equal(checkpoint(point), "v2.0.0");
  assert.equal(data.excludedResults, 1);
  assert.equal(point.median, measured.results[0].walltime!.median);
  for (const invalid of [
    { ...backfill, harnessSha: "other" },
    { ...backfill, engineSha: "other" },
    { ...backfill, releaseTag: "v3.0.0" },
    { ...backfill, effectiveDate: "invalid" },
    { ...backfill, effectiveDate: "2099-01-01" },
    { ...backfill, receipts: [] },
    { ...backfill, receipts: [{ ...backfill.receipts[0], runId: "other" }] },
    { ...backfill, receipts: [{ ...backfill.receipts[0], resultId: "other" }] },
    { ...backfill, receipts: [{ ...backfill.receipts[0], benchmarkName: "other" }] },
  ]) {
    assert.equal(
      buildTimeline([measured], [release], undefined, undefined, [invalid]).benchmarks.length,
      0,
    );
  }
});

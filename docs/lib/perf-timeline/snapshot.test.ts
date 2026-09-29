import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSnapshot, settledAfterMs, type ResultCache } from "./snapshot.ts";
import { withStalenessWarning } from "./source.ts";

const now = new Date("2026-09-29T05:00:00Z");
const daysAgo = (days: number) => new Date(now.getTime() - days * 86400000).toISOString();
const result = (id: string, median: number) => [
  { id, benchmark: { id: "bench", name: "bench" }, walltime: { min: median, median, max: median } },
];
const runs = [
  { id: "old", date: daysAgo(30), branch: "main" },
  { id: "recent", date: daysAgo(1), branch: "main" },
  { id: "uncached", date: daysAgo(30), branch: "main" },
  { id: "closed-pr", date: daysAgo(30), branch: "feature" },
].map(({ id, date, branch }) => ({
  id,
  date,
  status: "COMPLETED",
  event: "Push",
  commit: { hash: id.padEnd(40, "0"), message: id, branch: { name: branch, pullRequest: null } },
}));

function fakeCodSpeed(asked: string[]): typeof fetch {
  return (async (_url: string, init: RequestInit) => {
    const { query } = JSON.parse(init.body as string);
    const run = /run\(id: "([^"]+)"\)/.exec(query)?.[1];
    if (run) asked.push(run);
    const repository = run ? { run: { results: result(`${run}-fresh`, 2) } } : { runs };
    return new Response(JSON.stringify({ data: { repository } }));
  }) as typeof fetch;
}

test("settled runs reuse the previous snapshot; recent and new runs are re-read", async () => {
  const previous: ResultCache = {
    runs: {
      old: { date: runs[0].date, results: result("old-cached", 1) },
      recent: { date: runs[1].date, results: result("recent-cached", 1) },
    },
  };
  const asked: string[] = [];
  const { timeline, cache } = await buildSnapshot({
    tags: [],
    containingTags: () => new Set(),
    previous,
    fetchImpl: fakeCodSpeed(asked),
    now,
  });
  assert.ok(settledAfterMs < 30 * 86400000);
  // Closed-PR runs cannot be admitted, so their results are never requested.
  assert.deepEqual(asked.sort(), ["recent", "uncached"]);
  assert.deepEqual(Object.keys(cache.runs).sort(), ["old", "recent", "uncached"]);
  assert.deepEqual(timeline.benchmarks[0].points.map((p) => p.resultId).sort(), [
    "old-cached",
    "recent-fresh",
    "uncached-fresh",
  ]);
  assert.equal(timeline.fetchedAt, now.toISOString());
});

test("a snapshot older than two days carries a visible warning", () => {
  const timeline = {
    fetchedAt: daysAgo(3),
    benchmarks: [],
    releases: [],
    runCount: 0,
    excludedRuns: 0,
    excludedResults: 0,
    warnings: [],
  };
  assert.deepEqual(
    withStalenessWarning({ ...timeline, fetchedAt: daysAgo(1) }, now.getTime()).warnings,
    [],
  );
  assert.match(
    withStalenessWarning(timeline, now.getTime()).warnings[0],
    /last refreshed 2026-09-26/,
  );
});

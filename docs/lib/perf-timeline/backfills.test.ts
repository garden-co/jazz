import { test } from "node:test";
import assert from "node:assert/strict";
import { historicalBackfills, type HistoricalBackfill } from "./backfills.ts";
import { buildTimeline, type RawRun } from "./model.ts";
import { getBenchmarkMetadata } from "../../../dev/benchmarks/metadata/index.ts";

const sha = /^[0-9a-f]{40}$/;
const codspeedId = /^[0-9a-f]{24}$/;

test("registered backfills carry complete, explicit and non-duplicated provenance", () => {
  const resultIds = new Set<string>();
  const harnesses = new Set<string>();
  const releases = new Map<string, HistoricalBackfill>();
  for (const backfill of historicalBackfills) {
    for (const key of ["engineSha", "harnessSha", "harnessSourceSha"] as const)
      assert.match(backfill[key], sha, `${backfill.releaseTag} ${key}`);
    assert.notEqual(backfill.harnessSha, backfill.engineSha);
    assert.match(
      backfill.workflowUrl,
      /^https:\/\/github\.com\/garden-co\/jazz\/actions\/runs\/\d+$/,
    );
    assert.match(backfill.dateSource, /^npm jazz-tools time\[/);
    assert.ok(Number.isFinite(Date.parse(backfill.effectiveDate)));
    assert.ok(!harnesses.has(backfill.harnessSha), "one registry entry per harness commit");
    harnesses.add(backfill.harnessSha);
    // Several harness branches may backfill one release; they must agree on it.
    const sibling = releases.get(backfill.releaseTag);
    if (sibling) {
      assert.equal(backfill.engineSha, sibling.engineSha);
      assert.equal(backfill.effectiveDate, sibling.effectiveDate);
      assert.equal(backfill.dateSource, sibling.dateSource);
    }
    releases.set(backfill.releaseTag, backfill);
    assert.ok(backfill.receipts.length > 0);
    for (const receipt of backfill.receipts) {
      assert.match(receipt.runId, codspeedId);
      assert.match(receipt.resultId, codspeedId);
      assert.ok(!resultIds.has(receipt.resultId), `duplicate result ${receipt.resultId}`);
      resultIds.add(receipt.resultId);
      assert.ok(
        getBenchmarkMetadata(receipt.benchmarkName),
        `${receipt.benchmarkName} is a catalogued wallclock benchmark`,
      );
    }
  }
});

test("separate harness commits backfilling one release both place only their own receipts", () => {
  const release = { name: "v2.0.0", sha: "engine", url: "https://example.com/tag" };
  const measured = (harness: string, benchmark: string): RawRun => ({
    id: `run-${harness}`,
    date: "2026-09-24T06:00:00Z",
    status: "COMPLETED",
    event: "WorkflowDispatch",
    commit: {
      hash: harness,
      message: "harness",
      branch: { name: `bench/${harness}`, pullRequest: null },
    },
    results: [
      {
        id: `result-${harness}`,
        benchmark: { id: benchmark, name: benchmark },
        walltime: { min: 1, median: 2, max: 3 },
      },
      {
        // CodSpeed also reports older results from a partial run; never registered.
        id: `carried-${harness}`,
        benchmark: { id: "other", name: "other" },
        walltime: { min: 1, median: 2, max: 3 },
      },
    ],
  });
  const backfill = (harness: string, benchmark: string): HistoricalBackfill => ({
    releaseTag: release.name,
    engineSha: release.sha,
    harnessSha: harness,
    harnessSourceSha: "source",
    effectiveDate: "2026-09-10T04:26:57.178Z",
    dateSource: "npm jazz-tools time[2.0.0]",
    workflowUrl: "https://example.com/workflow",
    receipts: [
      { runId: `run-${harness}`, resultId: `result-${harness}`, benchmarkName: benchmark },
    ],
  });
  const data = buildTimeline(
    [measured("first", "a"), measured("second", "b")],
    [release],
    undefined,
    undefined,
    [backfill("first", "a"), backfill("second", "b")],
  );
  assert.deepEqual(
    data.benchmarks.map((b) => [b.name, b.points.map((p) => [p.sha, p.stage, p.date])]),
    [
      ["a", [["first", "released", "2026-09-10T04:26:57.178Z"]]],
      ["b", [["second", "released", "2026-09-10T04:26:57.178Z"]]],
    ],
  );
  assert.equal(data.excludedResults, 2);
});

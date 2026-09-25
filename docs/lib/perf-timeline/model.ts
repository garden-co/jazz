import type { HistoricalBackfill } from "./backfills.ts";

export type Stage = "released" | "main" | "open";
export type Distribution = { min: number; median: number; max: number };
export type RawRun = {
  id: string;
  date: string;
  status: string;
  event: string;
  commit: {
    hash: string;
    message: string;
    branch: {
      name: string;
      pullRequest: { number: number; title: string; status: string } | null;
    } | null;
  };
  results: { id: string; benchmark: { id: string; name: string }; walltime: Distribution | null }[];
};
export type Release = { name: string; sha: string; url: string };
export type Point = Distribution & {
  measuredAt: string;
  backfill: HistoricalBackfill | null;
  runId: string;
  resultId: string;
  date: string;
  sha: string;
  title: string;
  branch: string;
  pr: number | null;
  prStatus: string | null;
  stage: Stage;
  release: string | null;
  includedInRelease: string | null;
  runStatus: string;
  series: string;
};
export type Benchmark = { id: string; name: string; points: Point[] };
export type Timeline = {
  fetchedAt: string;
  benchmarks: Benchmark[];
  releases: Release[];
  runCount: number;
  excludedRuns: number;
  excludedResults: number;
  warnings: string[];
};

export const stages: Record<Stage, { label: string; color: string; dash: string }> = {
  released: { label: "Released", color: "#b66822", dash: "" },
  main: { label: "Main", color: "#167968", dash: "" },
  open: { label: "Open PR", color: "#7761b8", dash: "7 5" },
};

/**
 * Whether `buildTimeline` could place any result of this run. Every run it
 * admits passes this check, so results only need fetching for these runs.
 */
export function mayBeAdmitted(
  run: Pick<RawRun, "id" | "commit">,
  releases: readonly Release[],
  backfills: readonly HistoricalBackfill[] = [],
): boolean {
  const branch = run.commit.branch;
  return (
    branch?.name === "main" ||
    branch?.pullRequest?.status === "OPEN" ||
    releases.some((r) => r.sha === run.commit.hash) ||
    backfills.some((b) => b.receipts.some((r) => r.runId === run.id))
  );
}

export function buildTimeline(
  runs: RawRun[],
  releases: Release[],
  now = new Date().toISOString(),
  releaseAncestors: ReadonlyMap<string, string> = new Map(),
  backfills: readonly HistoricalBackfill[] = [],
): Timeline {
  const tagged = new Map(releases.map((r) => [r.sha, r.name]));
  const benchmarks = new Map<string, Benchmark>();
  let excludedResults = 0;
  let excludedRuns = 0;
  // A result ID is the measurement receipt. Do not average reruns, manufacture
  // zeros for missing jobs, or mix instruction-simulation results into seconds.
  const seen = new Set<string>();
  for (const run of runs) {
    if (!Number.isFinite(Date.parse(run.date))) continue;
    const branch = run.commit.branch;
    const pr = branch?.pullRequest;
    const historical = backfills.find(
      (b) =>
        b.harnessSha === run.commit.hash &&
        tagged.get(b.engineSha) === b.releaseTag &&
        Number.isFinite(Date.parse(b.effectiveDate)) &&
        Date.parse(b.effectiveDate) <= Date.parse(run.date) &&
        b.receipts.some((r) => r.runId === run.id),
    );
    const release = tagged.get(run.commit.hash) ?? null;
    const includedInRelease =
      release ?? (branch?.name === "main" ? (releaseAncestors.get(run.commit.hash) ?? null) : null);
    const stage: Stage | null = historical
      ? "released"
      : includedInRelease
        ? "released"
        : branch?.name === "main"
          ? "main"
          : pr?.status === "OPEN"
            ? "open"
            : null;
    // Exclude these at the source boundary, not merely from the chart: no
    // sidebar, sparkline, receipt, filter or API result should retain them.
    if (!stage) {
      excludedRuns++;
      continue;
    }
    for (const result of run.results) {
      if (
        historical &&
        !historical.receipts.some(
          (r) =>
            r.runId === run.id &&
            r.resultId === result.id &&
            r.benchmarkName === result.benchmark.name,
        )
      ) {
        excludedResults++;
        continue;
      }
      const time = result.walltime;
      if (
        !time ||
        ![time.min, time.median, time.max].every((v) => Number.isFinite(v) && v > 0) ||
        time.min > time.median ||
        time.median > time.max
      ) {
        excludedResults++;
        continue;
      }
      if (seen.has(result.id)) continue;
      seen.add(result.id);
      const bench = benchmarks.get(result.benchmark.id) ?? { ...result.benchmark, points: [] };
      bench.points.push({
        ...time,
        runId: run.id,
        resultId: result.id,
        date: historical?.effectiveDate ?? run.date,
        measuredAt: run.date,
        backfill: historical ?? null,
        sha: run.commit.hash,
        title: run.commit.message,
        branch: branch?.name ?? "unknown",
        pr: pr?.number ?? null,
        prStatus: pr?.status ?? null,
        stage,
        release,
        includedInRelease,
        runStatus: run.status,
        // Historical PR trials are not measurements of the merged main tree.
        // Never draw a continuous path across unrelated PRs.
        series:
          stage === "main" || stage === "released"
            ? "main"
            : pr
              ? `pr:${pr.number}`
              : `branch:${branch?.name ?? run.commit.hash}`,
      });
      benchmarks.set(bench.id, bench);
    }
  }
  for (const bench of benchmarks.values())
    bench.points.sort((a, b) => a.date.localeCompare(b.date) || a.runId.localeCompare(b.runId));
  return {
    fetchedAt: now,
    benchmarks: [...benchmarks.values()].sort((a, b) => a.name.localeCompare(b.name)),
    releases,
    runCount: runs.length,
    excludedRuns,
    excludedResults,
    warnings: [],
  };
}

export function formatTime(seconds: number): string {
  if (seconds === 0) return "0 s";
  if (seconds >= 1) return `${seconds.toLocaleString("en-US", { maximumFractionDigits: 2 })} s`;
  if (seconds >= 0.001)
    return `${(seconds * 1000).toLocaleString("en-US", { maximumFractionDigits: 2 })} ms`;
  return `${(seconds * 1e6).toLocaleString("en-US", { maximumFractionDigits: 2 })} µs`;
}

export function checkpoint(point: Point): string {
  if (point.backfill) return point.backfill.releaseTag;
  return (
    point.release ?? (point.pr ? `#${point.pr} · ${point.sha.slice(0, 7)}` : point.sha.slice(0, 7))
  );
}

export function calendarDay(timestamp: string): string {
  return new Date(timestamp).toISOString().slice(0, 10);
}

// One normalized geometry for both plots: identical history, domain, padding
// and log transform. A preview must not silently zoom into recent noise.
export function plotGeometry(points: Point[], logarithmic: boolean, spread: boolean, divisor = 1) {
  const values = points
    .flatMap((p) => (spread ? [p.min, p.max] : [p.median]))
    .map((v) => v / divisor);
  const paddedLow = logarithmic ? Math.min(...values) * 0.8 : 0;
  const paddedHigh = Math.max(...values) * 1.1;
  let ticks: number[];
  if (logarithmic) {
    // Round to 1/2/5 per decade; wide ranges use powers of ten to avoid crowding.
    const start = Math.floor(Math.log10(paddedLow));
    const end = Math.ceil(Math.log10(paddedHigh));
    const stride = Math.max(1, Math.ceil((end - start) / 6));
    const factors = end - start <= 3 ? [1, 2, 5] : [1];
    const candidates: number[] = [];
    for (let exponent = start - stride; exponent <= end + stride; exponent += stride) {
      for (const factor of factors) candidates.push(factor * 10 ** exponent);
    }
    const lower = candidates.findLastIndex((v) => v <= paddedLow);
    const upper = candidates.findIndex((v) => v >= paddedHigh);
    ticks = candidates.slice(lower, upper + 1);
  } else {
    const roughStep = paddedHigh / 4;
    const magnitude = 10 ** Math.floor(Math.log10(roughStep));
    const step = [1, 2, 5, 10].find((v) => v * magnitude >= roughStep)! * magnitude;
    ticks = Array.from({ length: Math.ceil(paddedHigh / step) + 1 }, (_, i) => i * step);
  }
  // Return raw-second coordinates so receipts and data remain untouched.
  ticks = ticks.map((v) => v * divisor);
  const low = ticks[0];
  const high = ticks[ticks.length - 1];
  const transform = (v: number) => (logarithmic ? Math.log10(v) : v);
  return {
    ticks,
    x: (i: number) => (points.length === 1 ? 0.5 : i / (points.length - 1)),
    y: (v: number) => (transform(high) - transform(v)) / (transform(high) - transform(low) || 1),
    tick: (ratio: number) =>
      logarithmic
        ? 10 ** (transform(low) + ratio * (transform(high) - transform(low)))
        : high * ratio,
  };
}

export type Stage = "released" | "main" | "open" | "archived" | "unknown";
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
  runStatus: string;
  series: string;
};
export type Benchmark = { id: string; name: string; points: Point[] };
export type Timeline = {
  fetchedAt: string;
  benchmarks: Benchmark[];
  releases: Release[];
  runCount: number;
  excludedResults: number;
  warnings: string[];
};

export const stages: Record<Stage, { label: string; color: string; dash: string }> = {
  released: { label: "Released", color: "#b66822", dash: "" },
  main: { label: "Main", color: "#167968", dash: "" },
  open: { label: "Open PR", color: "#7761b8", dash: "7 5" },
  archived: { label: "Past PR trial", color: "#939086", dash: "2 5" },
  unknown: { label: "Other branch", color: "#758594", dash: "3 5" },
};

export function buildTimeline(
  runs: RawRun[],
  releases: Release[],
  now = new Date().toISOString(),
): Timeline {
  const tagged = new Map(releases.map((r) => [r.sha, r.name]));
  const benchmarks = new Map<string, Benchmark>();
  let excludedResults = 0;
  // A result ID is the measurement receipt. Do not average reruns, manufacture
  // zeros for missing jobs, or mix instruction-simulation results into seconds.
  const seen = new Set<string>();
  for (const run of runs) {
    if (!Number.isFinite(Date.parse(run.date))) continue;
    const branch = run.commit.branch;
    const pr = branch?.pullRequest;
    const release = tagged.get(run.commit.hash) ?? null;
    const stage: Stage = release
      ? "released"
      : branch?.name === "main"
        ? "main"
        : pr?.status === "OPEN"
          ? "open"
          : pr && ["MERGED", "CLOSED"].includes(pr.status)
            ? "archived"
            : "unknown";
    for (const result of run.results) {
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
        date: run.date,
        sha: run.commit.hash,
        title: run.commit.message,
        branch: branch?.name ?? "unknown",
        pr: pr?.number ?? null,
        prStatus: pr?.status ?? null,
        stage,
        release,
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
  return (
    point.release ?? (point.pr ? `#${point.pr} · ${point.sha.slice(0, 7)}` : point.sha.slice(0, 7))
  );
}

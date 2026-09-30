import { stitchedFormerNames } from "../../../dev/benchmarks/metadata/index.ts";
import type { Benchmark, Point } from "../perf-timeline/model.ts";

export type HistoryEntry = { label: string; point: Point };
export type MetricSummary = {
  /** The number shown: the newest released measurement, else the newest main one. */
  headline: Point;
  /** "release" when the headline is attributed to a version tag; "main" otherwise. */
  basis: "release" | "main";
  label: string;
  /** Oldest first. One entry per release, or recent main runs when nothing is released. */
  history: HistoryEntry[];
  /** Newest main measurement after the headline release, if any. */
  unreleased: Point | null;
};

export function releaseLabel(point: Point): string | null {
  return point.backfill?.releaseTag ?? point.release ?? point.includedInRelease ?? null;
}

function shortSha(point: Point): string {
  return point.sha.slice(0, 7);
}

/**
 * Summarize one benchmark for a metric card. Open-PR experiments never count.
 * A release's number is its newest attributed measurement: an exact tag run,
 * an audited backfill, or a main commit proven to be contained in the tag.
 */
export function summarize(bench: Benchmark, historyLength = 10): MetricSummary | null {
  const settled = bench.points.filter((p) => p.stage !== "open");
  if (!settled.length) return null;
  const released = settled.filter((p) => p.stage === "released" && releaseLabel(p));
  if (released.length) {
    const byRelease = new Map<string, Point>();
    for (const point of released) byRelease.set(releaseLabel(point)!, point);
    const history = [...byRelease.entries()]
      .map(([label, point]) => ({ label, point }))
      .sort((a, b) => a.point.date.localeCompare(b.point.date))
      .slice(-historyLength);
    const newest = history.at(-1)!;
    const mainAfter = settled.filter((p) => p.stage === "main" && p.date > newest.point.date);
    return {
      headline: newest.point,
      basis: "release",
      label: newest.label,
      history,
      unreleased: mainAfter.at(-1) ?? null,
    };
  }
  const history = settled.slice(-historyLength).map((point) => ({
    label: `${point.date.slice(0, 10)} · ${shortSha(point)}`,
    point,
  }));
  const newest = history.at(-1)!;
  return { headline: newest.point, basis: "main", label: newest.label, history, unreleased: null };
}

/** Relative change of `current` against `previous`, negative when faster. */
export function change(previous: number, current: number): number {
  return (current - previous) / previous;
}

/**
 * Continue each declared-equivalent former name's history under its current
 * name (`stitchedFormerNames`), so a renamed metric card keeps its release
 * history. The current name's benchmark gains the former's points, oldest
 * first; if the current name has no results yet, the former's stand in under
 * the current name. Other benchmarks are returned unchanged, and renames whose
 * numbers changed are never stitched.
 */
export function stitchFormerHistory(
  benchmarks: readonly Benchmark[],
  stitched: ReadonlyMap<string, string> = stitchedFormerNames,
): Benchmark[] {
  const formerOf = new Map([...stitched].map(([current, former]) => [former, current]));
  const formerPoints = new Map<string, { id: string; points: Point[] }>();
  for (const bench of benchmarks) {
    const current = formerOf.get(bench.name);
    if (!current) continue;
    const previous = formerPoints.get(current);
    formerPoints.set(current, {
      id: previous?.id ?? bench.id,
      points: [...(previous?.points ?? []), ...bench.points],
    });
  }
  const byDate = (a: Point, b: Point) => a.date.localeCompare(b.date);
  const result = benchmarks.map((bench) => {
    const former = formerPoints.get(bench.name);
    if (!former) return bench;
    formerPoints.delete(bench.name);
    return { ...bench, points: [...former.points, ...bench.points].sort(byDate) };
  });
  for (const [current, former] of formerPoints)
    result.push({ id: former.id, name: current, points: [...former.points].sort(byDate) });
  return result;
}

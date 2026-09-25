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

import type { Timeline } from "./model.ts";

// The site never calls CodSpeed or GitHub's API. A scheduled workflow
// (.github/workflows/perf-timeline-snapshot.yml) builds the timeline once a
// day and after every release, and uploads it to the `perf-timeline-data`
// release. See README.md.
export const snapshotTag = "perf-timeline-data";
export const defaultSnapshotUrl = `https://github.com/garden-co/jazz/releases/download/${snapshotTag}/timeline.json`;
// A daily job that has missed two runs is worth telling readers about.
const staleAfterMs = 48 * 60 * 60 * 1000;

export function withStalenessWarning(timeline: Timeline, now = Date.now()): Timeline {
  const fetched = Date.parse(timeline.fetchedAt);
  if (Number.isFinite(fetched) && now - fetched <= staleAfterMs) return timeline;
  return {
    ...timeline,
    warnings: [
      `Benchmark history was last refreshed ${timeline.fetchedAt.slice(0, 10)}; newer runs are not shown yet.`,
      ...timeline.warnings,
    ],
  };
}

export async function loadTimeline(): Promise<Timeline> {
  const response = await fetch(process.env.PERF_TIMELINE_SNAPSHOT_URL || defaultSnapshotUrl, {
    // The snapshot changes at most a few times a day; the route's CDN cache
    // absorbs page traffic, so this only runs on revalidation.
    cache: "no-store",
    signal: AbortSignal.timeout(20000),
  });
  if (!response.ok) throw new Error(`Benchmark snapshot unavailable (HTTP ${response.status}).`);
  const timeline = (await response.json()) as Timeline;
  if (!Array.isArray(timeline?.benchmarks) || typeof timeline.fetchedAt !== "string")
    throw new Error("Benchmark snapshot is malformed.");
  return withStalenessWarning(timeline);
}

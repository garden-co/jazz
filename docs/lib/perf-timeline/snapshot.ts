import { historicalBackfills } from "./backfills.ts";
import { buildTimeline, mayBeAdmitted, type RawRun, type Release, type Timeline } from "./model.ts";
import { resolveReleaseAncestors } from "./releases.ts";

// Builds the benchmark snapshot the docs site serves. Only the scheduled
// snapshot workflow (.github/workflows/perf-timeline-snapshot.yml) runs this;
// the site itself never calls CodSpeed or GitHub's API.

// See dev/benchmarks/CODSPEED_GQL.md. This is the public, read-only API, not
// the browser's rotating persisted-query hashes or an embedded user token.
// One query for every run's results times out at CodSpeed's gateway (HTTP 502
// at ~400 runs / ~23k results), and `runs` takes no pagination arguments. So
// list runs without results, then fetch results one run at a time, only for
// runs the timeline can admit and the previous snapshot does not already hold.
const runsQuery = `query JazzWallclockRuns {
  repository(owner: "garden-co", name: "jazz") {
    runs {
      id date status event
      commit { hash message branch { name pullRequest { number title status } } }
    }
  }
}`;
// Results can still be processing shortly after a run, and re-running a CI job
// can replace results inside an existing run, so recent runs are re-read.
export const settledAfterMs = 7 * 24 * 60 * 60 * 1000;
const concurrentResultQueries = 4;

export type ResultCache = { runs: Record<string, { date: string; results: RawRun["results"] }> };
export type Snapshot = { timeline: Timeline; cache: ResultCache };
type Fetch = typeof fetch;

async function codspeed<T>(fetchImpl: Fetch, query: string): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt) await new Promise((resolve) => setTimeout(resolve, 2000 * 2 ** attempt));
    try {
      const response = await fetchImpl("https://gql.codspeed.io/", {
        method: "POST",
        headers: { "Content-Type": "application/json", "User-Agent": "jazz-perf-timeline" },
        body: JSON.stringify({ query }),
        signal: AbortSignal.timeout(60000),
      });
      if (!response.ok) throw new Error(`CodSpeed HTTP ${response.status}`);
      const payload = await response.json();
      if (payload.errors?.length || !payload.data?.repository)
        throw new Error(`CodSpeed error: ${JSON.stringify(payload.errors ?? payload)}`);
      return payload.data.repository as T;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

export async function buildSnapshot({
  tags,
  containingTags,
  previous,
  fetchImpl = fetch,
  now = new Date(),
}: {
  /** Version tags, oldest first. */
  tags: Release[];
  containingTags: (sha: string) => ReadonlySet<string> | null;
  previous: ResultCache | null;
  fetchImpl?: Fetch;
  now?: Date;
}): Promise<Snapshot> {
  const listed = await codspeed<{ runs: Omit<RawRun, "results">[] }>(fetchImpl, runsQuery);
  if (!Array.isArray(listed.runs)) throw new Error("CodSpeed returned no run list.");
  const admissible = listed.runs.filter((run) => mayBeAdmitted(run, tags, historicalBackfills));

  const cache: ResultCache = { runs: {} };
  const missing: typeof admissible = [];
  for (const run of admissible) {
    const cached = previous?.runs[run.id];
    const settled = now.getTime() - Date.parse(run.date) > settledAfterMs;
    if (cached && settled) cache.runs[run.id] = cached;
    else missing.push(run);
  }
  let next = 0;
  const worker = async () => {
    while (next < missing.length) {
      const run = missing[next++];
      const repository = await codspeed<{ run: Pick<RawRun, "results"> | null }>(
        fetchImpl,
        `query JazzWallclockRunResults { repository(owner: "garden-co", name: "jazz") { run(id: ${JSON.stringify(run.id)}) { results { id benchmark { id name } walltime { min median max } } } } }`,
      );
      // A run CodSpeed cannot return yet is retried next build, never cached empty.
      if (repository.run) cache.runs[run.id] = { date: run.date, results: repository.run.results };
    }
  };
  await Promise.all(Array.from({ length: concurrentResultQueries }, worker));

  // Runs the timeline cannot admit keep an empty result list; buildTimeline
  // excludes them before reading results, so its output is unchanged.
  const runs: RawRun[] = listed.runs.map((run) => ({
    ...run,
    results: cache.runs[run.id]?.results ?? [],
  }));
  const ancestry = resolveReleaseAncestors(runs, tags, containingTags);
  const timeline = buildTimeline(
    runs,
    // Newest first, as the site has always listed releases.
    [...tags].reverse(),
    now.toISOString(),
    ancestry.included,
    historicalBackfills,
  );
  timeline.warnings.push(...ancestry.warnings);
  return { timeline, cache };
}

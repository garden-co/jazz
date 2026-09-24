import { historicalBackfills } from "./backfills";
import { buildTimeline, mayBeAdmitted, type RawRun, type Release, type Timeline } from "./model";
import { resolveReleaseAncestors } from "./releases";

// See dev/benchmarks/CODSPEED_GQL.md. This is the public, read-only API, not
// the browser's rotating persisted-query hashes or an embedded user token.
// One query for every run's results times out at CodSpeed's gateway (HTTP 502
// at ~400 runs / ~23k results), and `runs` takes no pagination arguments. So
// list runs without results, then fetch results only for runs the timeline can
// admit, a few runs per request.
const runsQuery = `query JazzWallclockRuns {
  repository(owner: "garden-co", name: "jazz") {
    runs {
      id date status event
      commit { hash message branch { name pullRequest { number title status } } }
    }
  }
}`;
// CodSpeed rejects queries with more than 15 aliases.
const runsPerResultsQuery = 15;
const concurrentResultsQueries = 4;

async function codspeed<T>(query: string, timeoutMs: number): Promise<T> {
  const response = await fetch("https://gql.codspeed.io/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    next: { revalidate: 300 },
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) throw new Error(`CodSpeed is unavailable (HTTP ${response.status}).`);
  const payload = await response.json();
  if (payload.errors?.length || !payload.data?.repository) {
    throw new Error("CodSpeed could not return benchmark history. Please retry shortly.");
  }
  return payload.data.repository as T;
}

async function runResults(
  ids: string[],
  deadline: number,
): Promise<Map<string, RawRun["results"]>> {
  const batches: string[][] = [];
  for (let i = 0; i < ids.length; i += runsPerResultsQuery)
    batches.push(ids.slice(i, i + runsPerResultsQuery));
  const results = new Map<string, RawRun["results"]>();
  let next = 0;
  const worker = async () => {
    while (next < batches.length) {
      const fields = batches[next++]
        .map(
          (id, i) =>
            `r${i}: run(id: ${JSON.stringify(id)}) { id results { id benchmark { id name } walltime { min median max } } }`,
        )
        .join("\n");
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error("CodSpeed result fetch deadline exceeded.");
      const repository = await codspeed<Record<string, Pick<RawRun, "id" | "results"> | null>>(
        `query JazzWallclockResults { repository(owner: "garden-co", name: "jazz") { ${fields} } }`,
        Math.min(remaining, 20000),
      );
      for (const run of Object.values(repository)) if (run) results.set(run.id, run.results ?? []);
    }
  };
  await Promise.all(Array.from({ length: concurrentResultsQueries }, worker));
  return results;
}

async function versionTags(): Promise<Release[]> {
  const tags: Release[] = [];
  // Bounded pagination, explicitly reported if exhausted. No GitHub secret is
  // required; an optional read-only token can raise the public API rate limit.
  for (let page = 1; page <= 5; page++) {
    const response = await fetch(
      `https://api.github.com/repos/garden-co/jazz/tags?per_page=100&page=${page}`,
      {
        headers: {
          Accept: "application/vnd.github+json",
          "User-Agent": "jazz-perf-timeline",
          ...(process.env.PERF_GITHUB_TOKEN
            ? { Authorization: `Bearer ${process.env.PERF_GITHUB_TOKEN}` }
            : {}),
        },
        next: { revalidate: 3600 },
        signal: AbortSignal.timeout(15000),
      },
    );
    if (!response.ok) throw new Error(`GitHub release tags unavailable (HTTP ${response.status}).`);
    const pageTags = (await response.json()) as { name: string; commit: { sha: string } }[];
    for (const tag of pageTags)
      if (/^v?\d+\.\d+\.\d+(?:[-+].+)?$/.test(tag.name)) {
        tags.push({
          name: tag.name,
          sha: tag.commit.sha,
          url: `https://github.com/garden-co/jazz/tree/${encodeURIComponent(tag.name)}`,
        });
      }
    if (pageTags.length < 100) return tags;
  }
  throw new Error("Release tag lookup exceeded 500 tags; release classification is unavailable.");
}

export async function loadTimeline(): Promise<Timeline> {
  const deadline = Date.now() + 50000;
  const tagsPromise = versionTags().then(
    (tags) => ({ tags, warning: null }),
    (error: Error) => ({ tags: [] as Release[], warning: error.message }),
  );
  const listed = await codspeed<{ runs: Omit<RawRun, "results">[] }>(runsQuery, 20000);
  if (!Array.isArray(listed.runs)) {
    throw new Error("CodSpeed could not return benchmark history. Please retry shortly.");
  }
  const { tags, warning } = await tagsPromise;
  const admissible = listed.runs
    .filter((run) => mayBeAdmitted(run, tags, historicalBackfills))
    .map((run) => run.id);
  const results = await runResults(admissible, deadline - 5000);
  // Runs the timeline cannot admit keep an empty result list; buildTimeline
  // excludes them before reading results, so its output is unchanged.
  const runs: RawRun[] = listed.runs.map((run) => ({
    ...run,
    results: results.get(run.id) ?? [],
  }));
  const ancestry = await resolveReleaseAncestors(
    runs,
    tags,
    async (sha, release) => {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error("Ancestry deadline exceeded");
      const comparison = await fetch(
        `https://api.github.com/repos/garden-co/jazz/compare/${encodeURIComponent(sha)}...${encodeURIComponent(release)}?per_page=1`,
        {
          headers: {
            Accept: "application/vnd.github+json",
            "User-Agent": "jazz-perf-timeline",
            ...(process.env.PERF_GITHUB_TOKEN
              ? { Authorization: `Bearer ${process.env.PERF_GITHUB_TOKEN}` }
              : {}),
          },
          // Both coordinates are immutable SHAs. Reuse ancestry evidence across
          // refreshes and runs; a new/moved tag naturally uses a different key.
          next: { revalidate: 86400 },
          signal: AbortSignal.timeout(Math.min(remaining, 10000)),
        },
      );
      if (!comparison.ok) throw new Error(`GitHub comparison HTTP ${comparison.status}`);
      const result = await comparison.json();
      if (!["ahead", "identical", "behind", "diverged"].includes(result.status))
        throw new Error("Invalid comparison");
      return result.status;
    },
    process.env.PERF_GITHUB_TOKEN ? 200 : 40,
  );
  const data = buildTimeline(runs, tags, undefined, ancestry.included, historicalBackfills);
  data.warnings.push(...ancestry.warnings);
  if (warning) data.warnings.push(warning);
  return data;
}

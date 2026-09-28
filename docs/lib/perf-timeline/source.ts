import { historicalBackfills } from "./backfills";
import { buildTimeline, mayBeAdmitted, type RawRun, type Release, type Timeline } from "./model";
import { resolveReleaseAncestors } from "./releases";

// See dev/benchmarks/CODSPEED_GQL.md. This is the public, read-only API, not
// the browser's rotating persisted-query hashes or an embedded user token.
// One query for every run's results times out at CodSpeed's gateway (HTTP 502
// at ~400 runs / ~23k results), and `runs` takes no pagination arguments. So
// list runs without results, then fetch results one run at a time, only for
// runs the timeline can admit. Per-run requests let settled runs stay cached.
const runsQuery = `query JazzWallclockRuns {
  repository(owner: "garden-co", name: "jazz") {
    runs {
      id date status event
      commit { hash message branch { name pullRequest { number title status } } }
    }
  }
}`;
const listRevalidateSeconds = 300;
// Results can still be processing shortly after a run, and re-running a CI job
// can replace results inside an existing run, so settled runs expire daily.
const settledAfterMs = 2 * 60 * 60 * 1000;
const settledRevalidateSeconds = 86400;
const concurrentResultQueries = 6;

async function codspeed<T>(query: string, revalidate: number, timeoutMs: number): Promise<T> {
  const response = await fetch("https://gql.codspeed.io/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    next: { revalidate },
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
  runs: Pick<RawRun, "id" | "date">[],
  deadline: number,
): Promise<Map<string, RawRun["results"]>> {
  const results = new Map<string, RawRun["results"]>();
  let next = 0;
  const worker = async () => {
    while (next < runs.length) {
      const run = runs[next++];
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error("CodSpeed result fetch deadline exceeded.");
      const settled = Date.now() - Date.parse(run.date) > settledAfterMs;
      const repository = await codspeed<{ run: Pick<RawRun, "results"> | null }>(
        `query JazzWallclockRunResults { repository(owner: "garden-co", name: "jazz") { run(id: ${JSON.stringify(run.id)}) { results { id benchmark { id name } walltime { min median max } } } } }`,
        settled ? settledRevalidateSeconds : listRevalidateSeconds,
        Math.min(remaining, 15000),
      );
      results.set(run.id, repository.run?.results ?? []);
    }
  };
  await Promise.all(Array.from({ length: concurrentResultQueries }, worker));
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
  const listed = await codspeed<{ runs: Omit<RawRun, "results">[] }>(
    runsQuery,
    listRevalidateSeconds,
    20000,
  );
  if (!Array.isArray(listed.runs)) {
    throw new Error("CodSpeed could not return benchmark history. Please retry shortly.");
  }
  const { tags, warning } = await tagsPromise;
  const admissible = listed.runs.filter((run) => mayBeAdmitted(run, tags, historicalBackfills));
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

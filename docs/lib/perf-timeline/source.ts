import { historicalBackfills } from "./backfills";
import { buildTimeline, type RawRun, type Release, type Timeline } from "./model";
import { resolveReleaseAncestors } from "./releases";

// See dev/benchmarks/CODSPEED_GQL.md. This is the public, read-only API, not
// the browser's rotating persisted-query hashes or an embedded user token.
const query = `query JazzWallclockTimeline {
  repository(owner: "garden-co", name: "jazz") {
    runs {
      id date status event
      commit { hash message branch { name pullRequest { number title status } } }
      results { id benchmark { id name } walltime { min median max } }
    }
  }
}`;

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
  const response = await fetch("https://gql.codspeed.io/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    next: { revalidate: 300 },
    signal: AbortSignal.timeout(45000),
  });
  if (!response.ok) throw new Error(`CodSpeed is unavailable (HTTP ${response.status}).`);
  const payload = await response.json();
  if (payload.errors?.length || !Array.isArray(payload.data?.repository?.runs)) {
    throw new Error("CodSpeed could not return benchmark history. Please retry shortly.");
  }
  const { tags, warning } = await tagsPromise;
  const runs = payload.data.repository.runs as RawRun[];
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

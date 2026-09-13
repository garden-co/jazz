import { buildTimeline, type RawRun, type Release, type Timeline } from "./model";

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
  const data = buildTimeline(payload.data.repository.runs as RawRun[], tags);
  if (warning) data.warnings.push(warning);
  return data;
}

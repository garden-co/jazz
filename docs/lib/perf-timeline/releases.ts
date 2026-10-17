import type { RawRun, Release } from "./model.ts";

export function isVersionTag(name: string): boolean {
  return /^v?\d+\.\d+\.\d+(?:[-+].+)?$/.test(name);
}

/**
 * Attribute each measured main commit to the oldest release that contains it.
 *
 * `tags` must be ordered oldest first. `containingTags(sha)` returns the names
 * of every tag whose commit has `sha` as an ancestor (from a local git
 * checkout), or null when that commit is unknown locally. Unknown commits stay
 * unreleased with a visible warning; missing evidence never fabricates a
 * release measurement.
 */
export function resolveReleaseAncestors(
  runs: RawRun[],
  tags: Release[],
  containingTags: (sha: string) => ReadonlySet<string> | null,
) {
  const included = new Map<string, string>();
  const warnings = new Set<string>();
  const commits = new Set(
    runs
      .filter((run) => run.commit.branch?.name === "main" && run.results.some((r) => r.walltime))
      .map((run) => run.commit.hash),
  );
  for (const sha of commits) {
    const exact = tags.find((tag) => tag.sha === sha);
    if (exact) {
      included.set(sha, exact.name);
      continue;
    }
    const containing = containingTags(sha);
    if (!containing) {
      warnings.add("Some measured main commits are missing from the release history checkout.");
      continue;
    }
    const oldest = tags.find((tag) => containing.has(tag.name));
    if (oldest) included.set(sha, oldest.name);
  }
  return { included, warnings: [...warnings] };
}

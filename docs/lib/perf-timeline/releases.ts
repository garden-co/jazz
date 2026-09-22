import type { RawRun, Release } from "./model.ts";

export function isAncestorComparison(status: string): boolean {
  // Compare BASE=measured commit to HEAD=release: ahead/identical proves
  // inclusion. Behind or diverged does not, regardless of commit dates.
  return status === "ahead" || status === "identical";
}

export async function resolveReleaseAncestors(
  runs: RawRun[],
  tags: Release[],
  compare: (commit: string, release: string) => Promise<string>,
  limit = 40,
) {
  const included = new Map<string, string>();
  const warnings = new Set<string>();
  const commits = [
    ...new Set(
      runs
        .filter((run) => run.commit.branch?.name === "main" && run.results.some((r) => r.walltime))
        .map((run) => run.commit.hash),
    ),
  ];
  let next = 0,
    requests = 0,
    failed = false;
  async function worker() {
    while (next < commits.length && !failed) {
      const sha = commits[next++];
      for (const tag of tags) {
        if (sha === tag.sha) {
          included.set(sha, tag.name);
          break;
        }
        if (failed) break;
        if (requests++ >= limit) {
          warnings.add(
            "Release ancestry lookup reached its request budget; some main commits have unverified release status.",
          );
          return;
        }
        try {
          if (isAncestorComparison(await compare(sha, tag.sha))) {
            included.set(sha, tag.name);
            break;
          }
        } catch {
          failed = true;
          warnings.add(
            "GitHub release ancestry is temporarily unavailable; some main commits have unverified release status.",
          );
          break;
        }
      }
    }
  }
  await Promise.all(Array.from({ length: 3 }, worker));
  return { included, warnings: [...warnings] };
}

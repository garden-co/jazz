import { pathToFileURL } from "node:url";

const NEW_CORE_BASE_REF = "codex/jazz-core-engine-swap";
const GITHUB_LOOKUP_TIMEOUT_MS = 5_000;

function isNonEmptyString(value) {
  return typeof value === "string" && value.length > 0;
}

function pullRequestUrl(env) {
  const {
    VERCEL_GIT_REPO_OWNER: owner,
    VERCEL_GIT_REPO_SLUG: repo,
    VERCEL_GIT_PULL_REQUEST_ID: pullRequestId,
  } = env;

  if (!isNonEmptyString(owner) || !isNonEmptyString(repo) || !/^\d+$/.test(pullRequestId ?? "")) {
    return null;
  }

  return `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/pulls/${pullRequestId}`;
}

/**
 * Return true only when Vercel positively identifies this preview as a PR
 * targeting the new-core integration branch. All missing or failed lookups
 * deliberately return false, so Vercel continues with the build.
 */
export async function shouldSkipNewCorePreview({
  env = process.env,
  fetchImpl = globalThis.fetch,
  createAbortSignal = (timeoutMs) => AbortSignal.timeout(timeoutMs),
} = {}) {
  const url = pullRequestUrl(env);
  if (env.VERCEL_ENV !== "preview" || !url || typeof fetchImpl !== "function") {
    return false;
  }

  try {
    const signal = createAbortSignal(GITHUB_LOOKUP_TIMEOUT_MS);
    const response = await fetchImpl(url, {
      signal,
      headers: {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    });

    if (!response.ok) {
      return false;
    }

    const pullRequest = await response.json();
    return pullRequest?.base?.ref === NEW_CORE_BASE_REF;
  } catch {
    return false;
  }
}

async function main() {
  // Vercel ignore-command semantics: 0 means skip; 1 means build.
  process.exitCode = (await shouldSkipNewCorePreview()) ? 0 : 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}

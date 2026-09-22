import { pathToFileURL } from "node:url";

const DOCS_LABEL = "docs";
const GITHUB_LOOKUP_TIMEOUT_MS = 5_000;
const TRUSTED_ASSOCIATIONS = new Set(["OWNER", "MEMBER", "COLLABORATOR"]);

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

/** Skip unless GitHub positively identifies an allowed docs preview. */
export async function shouldSkipDocsDeploy({
  env = process.env,
  fetchImpl = globalThis.fetch,
  createAbortSignal = (timeoutMs) => AbortSignal.timeout(timeoutMs),
} = {}) {
  const url = pullRequestUrl(env);
  if (env.VERCEL_ENV !== "preview" || !url || typeof fetchImpl !== "function") return true;

  try {
    const response = await fetchImpl(url, {
      signal: createAbortSignal(GITHUB_LOOKUP_TIMEOUT_MS),
      headers: {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    });
    if (!response.ok) return true;

    const pullRequest = await response.json();
    const repository = `${env.VERCEL_GIT_REPO_OWNER}/${env.VERCEL_GIT_REPO_SLUG}`;
    const labels = Array.isArray(pullRequest?.labels) ? pullRequest.labels : [];
    return !(
      pullRequest?.state === "open" &&
      pullRequest?.head?.repo?.full_name === repository &&
      pullRequest?.base?.repo?.full_name === repository &&
      TRUSTED_ASSOCIATIONS.has(pullRequest?.author_association) &&
      labels.some((label) => label?.name === DOCS_LABEL)
    );
  } catch {
    return true;
  }
}

async function main() {
  // Vercel ignore-command semantics: 0 means skip; 1 means build.
  process.exitCode = (await shouldSkipDocsDeploy()) ? 0 : 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();

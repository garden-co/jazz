#!/usr/bin/env node
/**
 * Starters E2E on a release head tests the release preview's packages: the
 * exact bytes publish-jazz-tools-alpha.yml later publishes. changesets-release-pr
 * dispatches the preview and Starters E2E together, so wait for the preview run
 * of this commit to succeed and report its run ID (`run_id` step output).
 *
 * With PREVIEW_RUN_ID set (a manual dispatch), use that run instead of matching
 * the commit, but still require it to have succeeded.
 */
import { appendFileSync } from "node:fs";

export const PREVIEW_WORKFLOW = "preview-jazz-tools-alpha-release.yml";

/**
 * Decide from the preview runs of one commit. A successful run wins; while any
 * run is still going, keep waiting; if every run finished without success, fail.
 */
export function previewDecision(runs) {
  const succeeded = runs
    .filter((run) => run.status === "completed" && run.conclusion === "success")
    .sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))[0];
  if (succeeded) return { state: "ready", runId: succeeded.id };
  if (runs.some((run) => run.status !== "completed")) return { state: "waiting" };
  if (runs.length > 0)
    return {
      state: "failed",
      reason: `every release preview run finished without success: ${runs
        .map((run) => `${run.id} (${run.conclusion})`)
        .join(", ")}`,
    };
  return { state: "missing" };
}

async function api(path) {
  const response = await fetch(`https://api.github.com/repos/${process.env.REPO}/${path}`, {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${process.env.GH_TOKEN}`,
      "x-github-api-version": "2022-11-28",
    },
  });
  if (!response.ok) throw new Error(`GET ${path} failed: ${response.status}`);
  return response.json();
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function awaitPreview({
  headSha,
  pinnedRunId,
  timeoutMs = 60 * 60_000,
  missingTimeoutMs = 10 * 60_000,
  pollMs = 30_000,
}) {
  const started = Date.now();
  for (;;) {
    const runs = pinnedRunId
      ? [await api(`actions/runs/${pinnedRunId}`)]
      : (await api(`actions/workflows/${PREVIEW_WORKFLOW}/runs?head_sha=${headSha}&per_page=20`))
          .workflow_runs;
    if (pinnedRunId && !runs[0].path?.endsWith(PREVIEW_WORKFLOW))
      throw new Error(`run ${pinnedRunId} is not a ${PREVIEW_WORKFLOW} run`);
    const decision = previewDecision(runs);
    const elapsed = Date.now() - started;
    if (decision.state === "ready") return decision.runId;
    if (decision.state === "failed") throw new Error(decision.reason);
    if (decision.state === "missing" && elapsed > missingTimeoutMs)
      throw new Error(`no ${PREVIEW_WORKFLOW} run exists for ${headSha}`);
    if (elapsed > timeoutMs)
      throw new Error(`release preview for ${headSha} did not succeed within the wait budget`);
    console.log(
      decision.state === "missing"
        ? `waiting for a release preview run of ${headSha} to start`
        : `waiting for release preview run(s) ${runs.map((run) => run.id).join(", ")}`,
    );
    await sleep(pollMs);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const headSha = process.env.HEAD_SHA;
  const pinnedRunId = process.env.PREVIEW_RUN_ID || undefined;
  if (!pinnedRunId && !/^[0-9a-f]{40}$/.test(headSha ?? ""))
    throw new Error("HEAD_SHA must be a full commit SHA");
  const runId = await awaitPreview({ headSha, pinnedRunId });
  console.log(`using release preview run ${runId}`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `run_id=${runId}\n`);
}

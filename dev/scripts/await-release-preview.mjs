#!/usr/bin/env node
/**
 * Starters E2E on a release head tests the release preview's packages: the
 * exact bytes publish-jazz-tools-alpha.yml later publishes. changesets-release-pr
 * dispatches the preview and Starters E2E together, so wait for the preview run
 * of this commit to succeed and report its run ID (`run_id` step output).
 *
 * With PREVIEW_RUN_ID set (a manual dispatch), use that run instead of looking
 * it up, but still require it to be a preview of this commit and branch that
 * succeeded: a green Starters run at HEAD_SHA satisfies the release gate, so it
 * must never have tested another commit's packages.
 */
import { appendFileSync } from "node:fs";

export const PREVIEW_WORKFLOW = "preview-jazz-tools-alpha-release.yml";

/**
 * Why a preview run cannot stand for this commit, or undefined when it can. Both
 * the looked-up and the pinned run go through this, so Starters picks a run the
 * way publish's resolve-preview-artifacts does: same workflow, SHA and branch.
 */
export function previewRunMismatch(run, { headSha, headBranch }) {
  if (!run.path?.endsWith(PREVIEW_WORKFLOW))
    return `run ${run.id} is not a ${PREVIEW_WORKFLOW} run`;
  if (run.head_sha !== headSha)
    return `run ${run.id} previews ${run.head_sha}, not this commit ${headSha}`;
  if (run.head_branch !== headBranch)
    return `run ${run.id} ran on ${run.head_branch}, not ${headBranch}`;
  return undefined;
}

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
  headBranch,
  pinnedRunId,
  timeoutMs = 60 * 60_000,
  missingTimeoutMs = 10 * 60_000,
  pollMs = 30_000,
}) {
  const started = Date.now();
  for (;;) {
    let runs;
    if (pinnedRunId) {
      runs = [await api(`actions/runs/${pinnedRunId}`)];
      const mismatch = previewRunMismatch(runs[0], { headSha, headBranch });
      if (mismatch) throw new Error(mismatch);
    } else {
      const query = new URLSearchParams({ head_sha: headSha, branch: headBranch, per_page: "20" });
      runs = (
        await api(`actions/workflows/${PREVIEW_WORKFLOW}/runs?${query}`)
      ).workflow_runs.filter((run) => !previewRunMismatch(run, { headSha, headBranch }));
    }
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
  const headBranch = process.env.HEAD_BRANCH;
  const pinnedRunId = process.env.PREVIEW_RUN_ID || undefined;
  if (!/^[0-9a-f]{40}$/.test(headSha ?? "")) throw new Error("HEAD_SHA must be a full commit SHA");
  if (!headBranch) throw new Error("HEAD_BRANCH must name the branch the preview ran on");
  const runId = await awaitPreview({ headSha, headBranch, pinnedRunId });
  console.log(`using release preview run ${runId}`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `run_id=${runId}\n`);
}

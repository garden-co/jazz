#!/usr/bin/env node
/**
 * The release candidate gate verifies the release-artifacts.yml run of the
 * commit it was dispatched on (pushes to main and release start one after
 * every merge). Find that run, wait for it while it is still building, and
 * report its ID as the `run_id` step output. The gate needs only some of the
 * artifacts (REQUIRED_ARTIFACTS: comma-separated names), so a run still
 * building others (the Swift and Maven ones) is ready as soon as those are
 * uploaded; the gate separately requires the whole run to succeed
 * (WAIT_FOR=completion). Prints an empty `run_id` when no
 * run of this commit exists after `missingGraceMs` (a run dispatched together
 * with the gate, as changesets-release-pr.yml does, takes a moment to be
 * listed), so the gate builds the artifacts itself.
 *
 *   HEAD_SHA  the commit (40 hex)    REPO   owner/name    GH_TOKEN  actions: read
 */
import { appendFileSync } from "node:fs";

export const ARTIFACTS_WORKFLOW = "release-artifacts.yml";

/**
 * Decide from the release-artifacts runs of one commit. The newest successful
 * run wins; while any run is still going, keep waiting; runs that all failed
 * fail the gate (rebuilding the same commit would not pass either); none at all
 * means build.
 */
export function artifactsDecision(runs, headSha) {
  const own = runs.filter(
    (run) => run.head_sha === headSha && run.path?.endsWith(ARTIFACTS_WORKFLOW),
  );
  const succeeded = own
    .filter((run) => run.status === "completed" && run.conclusion === "success")
    .sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))[0];
  if (succeeded) return { state: "ready", runId: succeeded.id };
  if (own.some((run) => run.status !== "completed")) return { state: "waiting" };
  // A superseded main build was cancelled by a newer push: not a verdict.
  const failed = own.filter((run) => run.conclusion !== "cancelled");
  if (failed.length > 0)
    return {
      state: "failed",
      reason: `every ${ARTIFACTS_WORKFLOW} run of ${headSha} failed: ${failed
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

export async function awaitArtifacts({
  headSha,
  list,
  listArtifacts = async () => [],
  required = [],
  timeoutMs = 60 * 60_000,
  missingGraceMs = 90_000,
  pollMs = 20_000,
  log = console.log,
}) {
  const started = Date.now();
  for (;;) {
    const runs = await list(headSha);
    const decision = artifactsDecision(runs, headSha);
    if (decision.state === "ready") return String(decision.runId);
    if (decision.state === "waiting" && required.length > 0) {
      for (const run of runs.filter((r) => r.head_sha === headSha && r.status !== "completed")) {
        const names = new Set(await listArtifacts(run.id));
        if (required.every((name) => names.has(name))) {
          log(`run ${run.id} is still building, but has uploaded ${required.join(", ")}`);
          return String(run.id);
        }
      }
    }
    if (decision.state === "missing" && Date.now() - started >= missingGraceMs) return "";
    if (decision.state === "failed") throw new Error(decision.reason);
    if (Date.now() - started > timeoutMs)
      throw new Error(`${ARTIFACTS_WORKFLOW} for ${headSha} did not finish within the wait budget`);
    log(
      decision.state === "missing"
        ? `no ${ARTIFACTS_WORKFLOW} run of ${headSha} listed yet`
        : `waiting for the ${ARTIFACTS_WORKFLOW} run of ${headSha}`,
    );
    await sleep(pollMs);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const headSha = process.env.HEAD_SHA;
  if (!/^[0-9a-f]{40}$/.test(headSha ?? "")) throw new Error("HEAD_SHA must be a full commit SHA");
  const list = async (sha) =>
    (
      await api(
        `actions/workflows/${ARTIFACTS_WORKFLOW}/runs?${new URLSearchParams({ head_sha: sha, per_page: "50" })}`,
      )
    ).workflow_runs;
  const listArtifacts = async (runId) =>
    (await api(`actions/runs/${runId}/artifacts?per_page=100`)).artifacts
      .filter((artifact) => !artifact.expired)
      .map((artifact) => artifact.name);
  const required = (process.env.REQUIRED_ARTIFACTS ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
  const runId = await awaitArtifacts({ headSha, list, listArtifacts, required });
  console.log(runId ? `verifying ${ARTIFACTS_WORKFLOW} run ${runId}` : `no ${ARTIFACTS_WORKFLOW} run of ${headSha}: building here`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `run_id=${runId}\n`);
}

#!/usr/bin/env node
/**
 * Supply-chain rule: an input that brings in another run's artifacts must
 * prove that run built the consuming commit. Fails unless run RUN_ID:
 *   - is a run of one of WORKFLOWS (comma-separated workflow file names);
 *   - has head_sha == EXPECTED_SHA;
 *   - and, when MANIFEST is set (a downloaded release-artifacts
 *     manifest.json), that manifest's `sha` is EXPECTED_SHA too.
 *
 *   RUN_ID, EXPECTED_SHA, WORKFLOWS, REPO (owner/name), GH_TOKEN, [MANIFEST]
 */
import { readFileSync } from "node:fs";

/** Why `run` (a GitHub workflow run) can't stand for `expectedSha`, or undefined. */
export function runCommitMismatch(run, { expectedSha, workflows }) {
  if (!run || typeof run !== "object") return "no such run";
  const file = (run.path ?? "").split("@")[0].split("/").pop();
  if (!workflows.includes(file))
    return `run ${run.id} is a ${file || "unknown"} run, not one of ${workflows.join(", ")}`;
  if (run.head_sha !== expectedSha)
    return `run ${run.id} built ${run.head_sha}, not this commit ${expectedSha}`;
  return undefined;
}

export function manifestMismatch(manifest, { expectedSha, runId }) {
  if (manifest.sha !== expectedSha)
    return `release manifest of run ${runId} names ${manifest.sha}, not this commit ${expectedSha}`;
  if (manifest.run_id !== undefined && String(manifest.run_id) !== String(runId))
    return `release manifest names run ${manifest.run_id}, not run ${runId}`;
  return undefined;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { RUN_ID, EXPECTED_SHA, WORKFLOWS, REPO, GH_TOKEN, MANIFEST } = process.env;
  if (!/^[0-9]+$/.test(RUN_ID ?? "")) throw new Error(`RUN_ID must be a run ID, got '${RUN_ID}'`);
  if (!/^[0-9a-f]{40}$/.test(EXPECTED_SHA ?? ""))
    throw new Error("EXPECTED_SHA must be a full commit SHA");
  const workflows = (WORKFLOWS ?? "")
    .split(",")
    .map((w) => w.trim())
    .filter(Boolean);
  if (workflows.length === 0) throw new Error("WORKFLOWS must name the allowed producer workflows");
  const response = await fetch(`https://api.github.com/repos/${REPO}/actions/runs/${RUN_ID}`, {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${GH_TOKEN}`,
      "x-github-api-version": "2022-11-28",
    },
  });
  if (!response.ok) throw new Error(`GET run ${RUN_ID} failed: ${response.status}`);
  const run = await response.json();
  const problem =
    runCommitMismatch(run, { expectedSha: EXPECTED_SHA, workflows }) ??
    (MANIFEST
      ? manifestMismatch(JSON.parse(readFileSync(MANIFEST, "utf8")), {
          expectedSha: EXPECTED_SHA,
          runId: RUN_ID,
        })
      : undefined);
  if (problem) {
    console.error(`::error::${problem}`);
    process.exit(1);
  }
  console.log(
    `run ${RUN_ID} (${run.path}) built ${EXPECTED_SHA}${MANIFEST ? ", and its manifest agrees" : ""}`,
  );
}

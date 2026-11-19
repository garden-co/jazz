#!/usr/bin/env node
// The supply-chain rule for release-publish.yml's imported artifacts. A
// release-artifacts.yml run's artifacts may be published for commit <sha>
// only when all of these hold (source equivalence, not commit identity):
//   1. it built its own head_sha (release-artifacts.yml builds github.sha),
//      a real commit whose tree the git API reports;
//   2. tree(head_sha) == tree(<sha>);
//   3. it ran in this repository (no fork) on `push` or `workflow_dispatch`
//      (never pull_request), on `release` or `changeset-release/*`: the
//      branches whose runs are attested;
//   4. its release-manifest names head_sha, the published tree and version;
//   5. it succeeded.
// resolve-artifacts.sh prefers a run of <sha> itself when there is one.
//
//   SHA=<commit> TREE=<tree> VERSION=<version> GH_REPO=<owner/repo> HEAD_TREE=<tree(run.head_sha)> \
//     node dev/release-publish/producer-run.mjs <run.json> <manifest.json>
// exits 0 when it checks out, else prints why and exits 1.
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const PRODUCER_PATH = ".github/workflows/release-artifacts.yml";
const EVENTS = new Set(["push", "workflow_dispatch"]);

/** Reasons the expected values themselves are unusable (fail closed); empty when fine. */
export function expectedProblems({ sha, tree, version, repo }) {
  const problems = [];
  if (!/^[0-9a-f]{40}$/.test(sha ?? ""))
    problems.push(`SHA ${JSON.stringify(sha)} is not 40 hex characters`);
  if (!/^[0-9a-f]{40}$/.test(tree ?? ""))
    problems.push(`TREE ${JSON.stringify(tree)} is not 40 hex characters`);
  if (!version) problems.push("VERSION is empty");
  if (!/^[\w.-]+\/[\w.-]+$/.test(repo ?? ""))
    problems.push(`GH_REPO ${JSON.stringify(repo)} is not owner/repo`);
  return problems;
}

/** Whether `branch` is one whose release-artifacts.yml runs are attested. */
export function isReleaseBranch(branch) {
  return branch === "release" || /^changeset-release\/.+/.test(branch ?? "");
}

/**
 * Reasons the run (from GET /actions/runs/{id}) may not be imported for a
 * commit with tree `tree`; empty when it may. `headTree` is tree(run.head_sha)
 * from the git API (null when the commit couldn't be read).
 */
export function producerRunProblems(run, { tree, repo, headTree }) {
  const problems = [];
  if (run?.path !== PRODUCER_PATH) problems.push(`run is ${run?.path}, not ${PRODUCER_PATH}`);
  if (run?.head_repository?.full_name !== repo || run?.repository?.full_name !== repo)
    problems.push(`run belongs to ${run?.head_repository?.full_name}, not ${repo}`);
  if (!EVENTS.has(run?.event)) problems.push(`run was triggered by ${run?.event}`);
  if (!isReleaseBranch(run?.head_branch))
    problems.push(`run is on ${run?.head_branch}, not release or changeset-release/*`);
  if (run?.status !== "completed" || run?.conclusion !== "success")
    problems.push(`run is ${run?.status}/${run?.conclusion}`);
  if (!/^[0-9a-f]{40}$/.test(run?.head_sha ?? "")) problems.push(`run head_sha ${run?.head_sha}?`);
  if (!headTree) problems.push(`no commit ${run?.head_sha} in ${repo}`);
  else if (headTree !== tree) problems.push(`run built tree ${headTree}, not ${tree}`);
  return problems;
}

/** Reasons the run's release-manifest doesn't describe what it built and this release; empty when it does. */
export function manifestProblems(manifest, { headSha, tree, version }) {
  const problems = [];
  if (manifest?.sha !== headSha)
    problems.push(`manifest sha is ${manifest?.sha}, not the run's ${headSha}`);
  if (manifest?.source_sha !== undefined && manifest.source_sha !== headSha)
    problems.push(`manifest source_sha is ${manifest.source_sha}, not the run's ${headSha}`);
  if (manifest?.tree !== tree) problems.push(`manifest tree is ${manifest?.tree}, not ${tree}`);
  if (manifest?.version !== version)
    problems.push(`manifest version is ${manifest?.version}, not ${version}`);
  return problems;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [runFile, manifestFile] = process.argv.slice(2);
  const {
    SHA: sha,
    TREE: tree,
    VERSION: version,
    GH_REPO: repo,
    HEAD_TREE: headTree,
  } = process.env;
  const inputProblems = expectedProblems({ sha, tree, version, repo });
  if (inputProblems.length > 0) {
    console.log(`refusing: ${inputProblems.join("; ")}`);
    process.exit(1);
  }
  const run = JSON.parse(readFileSync(runFile, "utf8"));
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
  const problems = [
    ...producerRunProblems(run, { tree, repo, headTree: headTree || null }),
    ...manifestProblems(manifest, { headSha: run.head_sha, tree, version }),
  ];
  if (problems.length > 0) {
    console.log(`run ${run.id}: ${problems.join("; ")}`);
    process.exit(1);
  }
}

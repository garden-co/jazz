#!/usr/bin/env node
// The supply-chain rule for release-publish.yml's imported artifacts: a run
// whose artifacts are published must be a successful release-artifacts.yml
// run of this repository that built exactly the commit being published
// (its head_sha), and its release-manifest must say the same commit, tree
// and version. Nothing tree-equal, nothing from another workflow or fork.
//
//   SHA=<commit> TREE=<tree> VERSION=<version> GH_REPO=<owner/repo> \
//     node dev/release-publish/producer-run.mjs <run.json> <manifest.json>
// exits 0 when both check out, else prints why and exits 1.
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

/** Reasons the run (from GET /actions/runs/{id}) may not be imported; empty when it may. */
export function producerRunProblems(run, { sha, repo }) {
  const problems = [];
  if (run?.path !== PRODUCER_PATH) problems.push(`run is ${run?.path}, not ${PRODUCER_PATH}`);
  if (run?.head_repository?.full_name !== repo || run?.repository?.full_name !== repo)
    problems.push(`run belongs to ${run?.head_repository?.full_name}, not ${repo}`);
  if (!EVENTS.has(run?.event)) problems.push(`run was triggered by ${run?.event}`);
  if (run?.status !== "completed" || run?.conclusion !== "success")
    problems.push(`run is ${run?.status}/${run?.conclusion}`);
  if (run?.head_sha !== sha) problems.push(`run built ${run?.head_sha}, not ${sha}`);
  return problems;
}

/** Reasons the run's release-manifest doesn't describe this release; empty when it does. */
export function manifestProblems(manifest, { sha, tree, version }) {
  const problems = [];
  if (manifest?.sha !== sha) problems.push(`manifest sha is ${manifest?.sha}, not ${sha}`);
  if (manifest?.source_sha !== undefined && manifest.source_sha !== sha)
    problems.push(`manifest source_sha is ${manifest.source_sha}, not ${sha}`);
  if (manifest?.tree !== tree) problems.push(`manifest tree is ${manifest?.tree}, not ${tree}`);
  if (manifest?.version !== version)
    problems.push(`manifest version is ${manifest?.version}, not ${version}`);
  return problems;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [runFile, manifestFile] = process.argv.slice(2);
  const { SHA: sha, TREE: tree, VERSION: version, GH_REPO: repo } = process.env;
  const inputProblems = expectedProblems({ sha, tree, version, repo });
  if (inputProblems.length > 0) {
    console.log(`refusing: ${inputProblems.join("; ")}`);
    process.exit(1);
  }
  const run = JSON.parse(readFileSync(runFile, "utf8"));
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
  const problems = [
    ...producerRunProblems(run, { sha, repo }),
    ...manifestProblems(manifest, { sha, tree, version }),
  ];
  if (problems.length > 0) {
    console.log(`run ${run.id}: ${problems.join("; ")}`);
    process.exit(1);
  }
}

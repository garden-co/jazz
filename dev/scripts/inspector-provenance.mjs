#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { hasTrustedCacheGuard } from "../release-publish/producer-run.mjs";

const shaPattern = /^[a-f0-9]{40}$/;
// Production staging takes only `release`-branch producers, built from
// reviewed code. Not preview-build.yml's pull_request runs: they run from the
// PR's merge ref and restore caches from the PR's own scope, which its author
// controls (#supply-chain, 2026-11-19). A PR's Inspector goes to a preview
// deployment; merge it to stage it for production.
const publishPath = ".github/workflows/publish-jazz-tools-alpha.yml";
// Not preview-jazz-tools-alpha-release.yml: its runs on changeset-release/*
// restore caches from that branch's scope, and it can still be dispatched there.
// release-artifacts.yml builds its run's head_sha, on `release` or
// `changeset-release/*`: there it restores no cache (every restore is gated
// on a trusted ref; dev/gates/test/release-artifacts-trusted-caches.test.mjs
// is the control; #supply-chain, 2026-11-19, jazz#6168).
const releaseArtifactsPath = ".github/workflows/release-artifacts.yml";

export function inspectorRunSource(run, repository) {
  if (
    run.status !== "completed" ||
    run.conclusion !== "success" ||
    run.head_repository?.full_name !== repository ||
    !shaPattern.test(run.head_sha ?? "")
  ) {
    throw new Error("Inspector artifact run must be successful and belong to this repository");
  }
  const releaseEvent = run.event === "push" || run.event === "workflow_dispatch";
  const approved =
    (run.path === publishPath && releaseEvent && run.head_branch === "release") ||
    (run.path === releaseArtifactsPath &&
      releaseEvent &&
      (run.head_branch === "release" || /^changeset-release\/.+/.test(run.head_branch ?? "")));
  if (!approved)
    throw new Error(
      "Inspector artifact run has an unapproved producer workflow, event or source branch",
    );
  // Both release producers build their event ref, so run.head_sha.
  return run.head_sha;
}

export function inspectorBranchRefPath(repository, branch) {
  if (
    !/^[\w.-]+\/[\w.-]+$/.test(repository ?? "") ||
    typeof branch !== "string" ||
    !branch ||
    branch.startsWith("refs/")
  ) {
    throw new Error("Inspector requires a repository and a short branch name");
  }
  // The explicit heads namespace cannot resolve a tag or raw commit SHA.
  return `repos/${repository}/git/ref/heads/${encodeURIComponent(branch)}`;
}

export function verifyInspectorHandoff({
  run,
  repository,
  sourceSha,
  branch,
  artifactCommit,
  deploymentCommit,
  branchRef,
  deploymentReleaseArtifactsWorkflow = "",
}) {
  const artifactSha = inspectorRunSource(run, repository);
  // A changeset-release/* release-artifacts run restored no cache only if the
  // (tree-equal) deployed commit's release-artifacts.yml has the guard.
  if (
    run.path === releaseArtifactsPath &&
    run.head_branch !== "release" &&
    !hasTrustedCacheGuard(deploymentReleaseArtifactsWorkflow)
  ) {
    throw new Error(
      "Inspector artifact run is on changeset-release/*, but the deployed tree's release-artifacts.yml lacks the trusted-cache guard (jazz#6168)",
    );
  }
  inspectorBranchRefPath(repository, branch);
  if (
    !shaPattern.test(sourceSha ?? "") ||
    artifactCommit.sha !== artifactSha ||
    deploymentCommit.sha !== sourceSha
  ) {
    throw new Error("Inspector source commit identity mismatch");
  }
  if (
    !shaPattern.test(artifactCommit.tree?.sha ?? "") ||
    artifactCommit.tree.sha !== deploymentCommit.tree?.sha
  ) {
    throw new Error("Inspector artifact and deployment source trees differ");
  }
  if (
    branchRef.ref !== `refs/heads/${branch}` ||
    branchRef.object?.type !== "commit" ||
    branchRef.object.sha !== sourceSha
  ) {
    throw new Error("Inspector deployment SHA must be the current head of the requested branch");
  }
  return artifactSha;
}

export function verifyInspectorRunFromGitHub(
  env = process.env,
  api = (path) => JSON.parse(execFileSync("gh", ["api", path], { encoding: "utf8" })),
) {
  const repository = env.GITHUB_REPOSITORY;
  const branch = env.SOURCE_BRANCH;
  const branchPath = inspectorBranchRefPath(repository, branch);
  if (!/^[0-9]+$/.test(env.ARTIFACT_RUN_ID ?? "") || !shaPattern.test(env.SOURCE_SHA ?? "")) {
    throw new Error("Inspector requires an exact source SHA and numeric artifact run ID");
  }
  const run = api(`repos/${repository}/actions/runs/${env.ARTIFACT_RUN_ID}`);
  const artifactSha = inspectorRunSource(run, repository);
  return verifyInspectorHandoff({
    run,
    repository,
    branch,
    sourceSha: env.SOURCE_SHA,
    artifactCommit: api(`repos/${repository}/git/commits/${artifactSha}`),
    deploymentCommit: api(`repos/${repository}/git/commits/${env.SOURCE_SHA}`),
    branchRef: api(branchPath),
    deploymentReleaseArtifactsWorkflow:
      run.path === releaseArtifactsPath && run.head_branch !== "release"
        ? releaseArtifactsWorkflowAt(api, repository, env.SOURCE_SHA)
        : "",
  });
}

function releaseArtifactsWorkflowAt(api, repository, sha) {
  try {
    const file = api(
      `repos/${repository}/contents/.github/workflows/release-artifacts.yml?ref=${sha}`,
    );
    return Buffer.from(file.content ?? "", file.encoding ?? "base64").toString("utf8");
  } catch {
    return "";
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const artifactSha = verifyInspectorRunFromGitHub();
    if (!process.env.GITHUB_OUTPUT) throw new Error("Missing GITHUB_OUTPUT");
    appendFileSync(process.env.GITHUB_OUTPUT, `source_sha=${artifactSha}\n`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const shaPattern = /^[a-f0-9]{40}$/;
const previewPath = ".github/workflows/preview-build.yml";
const releasePreviewPath = ".github/workflows/preview-jazz-tools-alpha-release.yml";
const publishPath = ".github/workflows/publish-jazz-tools-alpha.yml";

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
    (run.path === previewPath &&
      run.event === "pull_request" &&
      typeof run.head_branch === "string" &&
      run.head_branch.length > 0) ||
    (run.path === releasePreviewPath &&
      releaseEvent &&
      /^changeset-release\/.+/.test(run.head_branch ?? "")) ||
    (run.path === publishPath && releaseEvent && run.head_branch === "release");
  if (!approved)
    throw new Error(
      "Inspector artifact run has an unapproved producer workflow, event or source branch",
    );
  // preview-build explicitly checks out the PR head; both release callers use
  // their event ref. All three therefore build run.head_sha, not a PR merge SHA.
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
}) {
  const artifactSha = inspectorRunSource(run, repository);
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
  });
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

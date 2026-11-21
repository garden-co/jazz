import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

import { manifestMismatch, runCommitMismatch } from "./require-run-commit.mjs";

const root = path.resolve(import.meta.dirname, "../..");
const sha = "a".repeat(40);
const repo = "garden-co/jazz";
const run = (extra = {}) => ({
  id: 7,
  head_sha: sha,
  event: "push",
  head_repository: { full_name: repo },
  path: ".github/workflows/release-artifacts.yml",
  ...extra,
});
const workflows = ["release-artifacts.yml", "release-candidate.yml"];

test("a run of an allowed workflow that built this commit stands", () => {
  assert.equal(runCommitMismatch(run(), { expectedSha: sha, workflows }), undefined);
  assert.equal(
    runCommitMismatch(run({ path: ".github/workflows/release-candidate.yml@refs/heads/main" }), {
      expectedSha: sha,
      workflows,
    }),
    undefined,
  );
});

test("another commit's run, or another workflow's, is refused", () => {
  assert.match(
    runCommitMismatch(run({ head_sha: "b".repeat(40) }), { expectedSha: sha, workflows }),
    /not this commit/,
  );
  assert.match(
    runCommitMismatch(run({ path: ".github/workflows/ci.yml" }), { expectedSha: sha, workflows }),
    /is a ci\.yml run/,
  );
  assert.match(runCommitMismatch(undefined, { expectedSha: sha, workflows }), /no such run/);
});

test("a pull_request run, or another repository's, never stands for a commit", () => {
  for (const event of ["pull_request", "pull_request_target"])
    assert.match(
      runCommitMismatch(run({ event }), { expectedSha: sha, workflows, repo }),
      new RegExp(`is a ${event} run`),
    );
  assert.match(
    runCommitMismatch(run({ head_repository: { full_name: "someone/fork" } }), {
      expectedSha: sha,
      workflows,
      repo,
    }),
    /built someone\/fork, not garden-co\/jazz/,
  );
  assert.equal(
    runCommitMismatch(run({ event: "workflow_dispatch" }), { expectedSha: sha, workflows, repo }),
    undefined,
  );
});

test("a release manifest must name the same commit and run", () => {
  assert.equal(manifestMismatch({ sha, run_id: 7 }, { expectedSha: sha, runId: "7" }), undefined);
  assert.match(
    manifestMismatch({ sha: "c".repeat(40), run_id: 7 }, { expectedSha: sha, runId: "7" }),
    /names c+/,
  );
  assert.match(
    manifestMismatch({ sha, run_id: 8 }, { expectedSha: sha, runId: "7" }),
    /names run 8/,
  );
});

const workflow = (name) =>
  parse(fs.readFileSync(path.join(root, `.github/workflows/${name}.yml`), "utf8"));
const stepIndex = (steps, pred) => steps.findIndex(pred);

test("Examples E2E checks a given artifacts run built this commit before downloading from it", () => {
  const steps = workflow("examples-e2e").jobs.prepare.steps;
  const check = stepIndex(steps, (s) => s.run?.includes("dev/scripts/require-run-commit.mjs"));
  const download = stepIndex(steps, (s) => s.name === "Download the release preview's packages");
  assert.ok(check !== -1 && download !== -1 && check < download);
  assert.match(steps[check].if, /inputs\.artifacts_run_id != ''/);
});

test("the release candidate gate re-checks the run it picked", () => {
  const steps = workflow("release-candidate").jobs.artifacts.steps;
  assert.ok(steps.some((s) => s.run?.includes("dev/scripts/require-run-commit.mjs")));
});

test("a called publish dry run checks the artifacts run it was given", () => {
  const script = workflow("publish-jazz-tools-alpha").jobs["resolve-preview-artifacts"].steps.find(
    (s) => s.id === "resolve",
  ).with.script;
  assert.match(script, /getWorkflowRun/);
  assert.match(script, /head_sha !== sha/);
  assert.match(script, /startsWith\("pull_request"\)/);
  assert.match(script, /head_repository/);
});

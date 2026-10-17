import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import { RELEASE_TEST_WORKFLOWS, evaluateReleaseTestGate } from "./release-test-gate.mjs";

const root = path.resolve(import.meta.dirname, "../..");
const headSha = "a".repeat(40);
const mergeSha = "b".repeat(40);
const headRef = "changeset-release/release";

function passingRuns() {
  return {
    "ci.yml": [
      {
        id: 1,
        head_sha: headSha,
        event: "workflow_dispatch",
        status: "completed",
        conclusion: "success",
      },
    ],
    "starters-e2e.yml": [
      {
        id: 2,
        head_sha: headSha,
        event: "workflow_dispatch",
        status: "completed",
        conclusion: "success",
      },
    ],
    "rn-device-acceptance.yml": [
      {
        id: 3,
        head_sha: headSha,
        event: "workflow_dispatch",
        status: "completed",
        conclusion: "success",
      },
    ],
  };
}

function fakeGithub({ runs = passingRuns(), trees = { [mergeSha]: "t1", [headSha]: "t1" } } = {}) {
  const requests = [];
  const listWorkflowRuns = Symbol("listWorkflowRuns");
  return {
    requests,
    rest: {
      git: {
        async getCommit({ commit_sha }) {
          return { data: { tree: { sha: trees[commit_sha] } } };
        },
      },
      actions: { listWorkflowRuns },
    },
    async paginate(method, params) {
      assert.equal(method, listWorkflowRuns);
      requests.push(params);
      return runs[params.workflow_id] ?? [];
    },
  };
}

const evaluate = (github) =>
  evaluateReleaseTestGate({ github, owner: "o", repo: "r", sha: mergeSha, headSha, headRef });

test("passes when every release workflow succeeded on the tested head and trees match", async () => {
  const github = fakeGithub();
  const result = await evaluate(github);
  assert.equal(result.ok, true, result.lines.join("\n"));
  assert.deepEqual(
    github.requests.map((request) => [request.workflow_id, request.branch, request.head_sha]),
    RELEASE_TEST_WORKFLOWS.map((workflow) => [workflow.file, headRef, headSha]),
  );
});

test("fails when the published tree is not the tested tree", async () => {
  const result = await evaluate(fakeGithub({ trees: { [mergeSha]: "t2", [headSha]: "t1" } }));
  assert.equal(result.ok, false);
  assert.match(result.lines[0], /differs from tested release PR head tree/);
});

test("fails when a release workflow failed, is still running, or ran on another SHA", async () => {
  for (const run of [
    { conclusion: "failure" },
    { status: "in_progress", conclusion: null },
    { conclusion: "action_required" },
    { head_sha: "c".repeat(40) },
  ]) {
    const runs = passingRuns();
    runs["starters-e2e.yml"] = [{ ...runs["starters-e2e.yml"][0], ...run }];
    const result = await evaluate(fakeGithub({ runs }));
    assert.equal(result.ok, false, JSON.stringify(run));
    assert.ok(result.lines.some((line) => line.startsWith("Starters E2E: no successful")));
  }
});

test("fails when a release workflow never ran", async () => {
  const runs = passingRuns();
  delete runs["ci.yml"];
  const result = await evaluate(fakeGithub({ runs }));
  assert.equal(result.ok, false);
  assert.ok(result.lines.some((line) => /^CI: no successful .*\(no runs\)$/.test(line)));
});

test("a scaffold-only pull_request run does not count as RN device acceptance", async () => {
  const runs = passingRuns();
  runs["rn-device-acceptance.yml"] = [
    { ...runs["rn-device-acceptance.yml"][0], event: "pull_request" },
  ];
  const result = await evaluate(fakeGithub({ runs }));
  assert.equal(result.ok, false);
  assert.ok(
    result.lines.some((line) =>
      line.startsWith("React Native device acceptance: no successful workflow_dispatch run"),
    ),
  );
});

test("an earlier failure does not block a later success on the same head", async () => {
  const runs = passingRuns();
  runs["ci.yml"].unshift({ ...runs["ci.yml"][0], id: 9, conclusion: "failure" });
  assert.equal((await evaluate(fakeGithub({ runs }))).ok, true);
});

test("fails closed without release PR metadata", async () => {
  const result = await evaluateReleaseTestGate({
    github: fakeGithub(),
    owner: "o",
    repo: "r",
    sha: mergeSha,
    headSha: "",
    headRef: "",
  });
  assert.equal(result.ok, false);
});

const releasePr = fs.readFileSync(
  path.join(root, ".github/workflows/changesets-release-pr.yml"),
  "utf8",
);
const publish = fs.readFileSync(
  path.join(root, ".github/workflows/publish-jazz-tools-alpha.yml"),
  "utf8",
);

function job(source, name) {
  const start = source.indexOf(`\n  ${name}:\n`);
  assert.notEqual(start, -1, `missing ${name} job`);
  const next = source.slice(start + 1).search(/\n  [a-z][-a-z0-9]*:\n/);
  return next === -1 ? source.slice(start) : source.slice(start, start + 1 + next);
}

test("the release PR workflow dispatches every release test workflow on the release head", () => {
  for (const workflow of RELEASE_TEST_WORKFLOWS) {
    const dispatch = `gh workflow run ${workflow.file} --repo "\${REPO}" --ref "\${RELEASE_BRANCH}"`;
    assert.ok(releasePr.includes(dispatch), `missing dispatch of ${workflow.file}`);
  }
});

test("npm publication needs the release test gate in publish mode", () => {
  const gate = job(publish, "release-test-gate");
  assert.match(gate, /needs: \[release-push-gate\]/);
  assert.match(gate, /release-test-gate\.mjs/);
  assert.match(gate, /core\.setFailed/);

  const publishNpm = job(publish, "publish-npm");
  assert.match(publishNpm, /- release-test-gate\n/);
  assert.ok(
    publishNpm.includes(
      "needs.release-test-gate.result == 'success' ||\n        (inputs.mode || github.event.inputs.mode || 'publish') == 'dry-run'",
    ),
    "publish-npm must require the release test gate outside dry runs",
  );
});

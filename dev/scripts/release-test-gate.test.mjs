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
    "rn-device-acceptance.yml": [
      {
        id: 3,
        head_sha: headSha,
        event: "workflow_dispatch",
        status: "completed",
        conclusion: "success",
      },
    ],
    "examples-e2e.yml": [
      {
        id: 4,
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
    runs["ci.yml"] = [{ ...runs["ci.yml"][0], ...run }];
    const result = await evaluate(fakeGithub({ runs }));
    assert.equal(result.ok, false, JSON.stringify(run));
    assert.ok(result.lines.some((line) => line.startsWith("CI: no successful")));
  }
});

test("fails when the examples E2E gate did not pass", async () => {
  const runs = passingRuns();
  runs["examples-e2e.yml"] = [{ ...runs["examples-e2e.yml"][0], conclusion: "failure" }];
  const result = await evaluate(fakeGithub({ runs }));
  assert.equal(result.ok, false);
  assert.ok(result.lines.some((line) => line.startsWith("Examples E2E: no successful")));
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
const publish = fs.readFileSync(path.join(root, ".github/workflows/release-publish.yml"), "utf8");

function job(source, name) {
  const start = source.indexOf(`\n  ${name}:\n`);
  assert.notEqual(start, -1, `missing ${name} job`);
  const next = source.slice(start + 1).search(/\n  [a-z][-a-z0-9]*:\n/);
  return next === -1 ? source.slice(start) : source.slice(start, start + 1 + next);
}

test("the release PR workflow dispatches every release test workflow on the release head", () => {
  for (const workflow of RELEASE_TEST_WORKFLOWS) {
    const files = [workflow.file, ...workflow.alternatives.map((alternative) => alternative.file)];
    const dispatched = files.filter((file) =>
      releasePr.includes(`gh workflow run ${file} --repo "\${REPO}" --ref "\${RELEASE_BRANCH}"`),
    );
    assert.ok(dispatched.length > 0, `missing dispatch of ${files.join(" or ")}`);
  }
});

test("publication needs the release test gate in publish mode", () => {
  // release-publish.yml: the resolve job, before anything can be approved.
  const resolve = job(publish, "resolve");
  assert.match(
    resolve,
    /name: Require a tested release commit\n\s+id: gate\n\s+if: inputs\.mode == 'publish'/,
  );
  assert.match(resolve, /release-test-gate\.mjs/);
  assert.match(resolve, /if \(!ok\) core\.setFailed/);
  // Every publishing leg waits for resolve (through ready).
  for (const leg of ["npm", "crates", "maven", "go", "swift"])
    assert.match(job(publish, leg), /needs: \[resolve, ready/);
});

test("a successful release candidate gate on the head stands for RN device acceptance and Examples E2E", async () => {
  const runs = passingRuns();
  delete runs["rn-device-acceptance.yml"];
  delete runs["examples-e2e.yml"];
  runs["release-candidate.yml"] = [
    {
      id: 7,
      head_sha: headSha,
      event: "workflow_dispatch",
      status: "completed",
      conclusion: "success",
    },
  ];
  const result = await evaluate(fakeGithub({ runs }));
  assert.equal(result.ok, true, result.lines.join("\n"));
  assert.ok(
    result.lines.some((line) =>
      /^Examples E2E: passed as part of Release candidate gate run 7/.test(line),
    ),
  );
});

test("a release candidate gate does not stand for CI, nor counts when it failed or ran elsewhere", async () => {
  for (const [drop, patch] of [
    ["ci.yml", {}],
    ["examples-e2e.yml", { conclusion: "failure" }],
    ["examples-e2e.yml", { head_sha: "c".repeat(40) }],
    ["examples-e2e.yml", { event: "push" }],
    ["examples-e2e.yml", { display_title: "Cloud RC probe only, v2.0.0-alpha.68-rc.1" }],
  ]) {
    const runs = passingRuns();
    delete runs[drop];
    runs["release-candidate.yml"] = [
      {
        id: 7,
        head_sha: headSha,
        event: "workflow_dispatch",
        status: "completed",
        conclusion: "success",
        ...patch,
      },
    ];
    const result = await evaluate(fakeGithub({ runs }));
    assert.equal(result.ok, false, `${drop} ${JSON.stringify(patch)}`);
  }
});

test("the release candidate workflow names probe-only runs the way the gate excludes them", () => {
  const rc = fs.readFileSync(path.join(root, ".github/workflows/release-candidate.yml"), "utf8");
  const runName = rc.match(/^run-name: (.*)$/m)?.[1] ?? "";
  assert.match(runName, /inputs\.probe-tag != '' && format\('Cloud RC probe only, \{0\}'/);
});

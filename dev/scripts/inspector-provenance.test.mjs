import assert from "node:assert/strict";
import test from "node:test";
import {
  inspectorRunSource,
  inspectorBranchRefPath,
  verifyInspectorHandoff,
  verifyInspectorRunFromGitHub,
} from "./inspector-provenance.mjs";

const repository = "example/project";
const artifactSha = "a".repeat(40),
  deploySha = "b".repeat(40),
  tree = "c".repeat(40);
function run(overrides = {}) {
  return {
    status: "completed",
    conclusion: "success",
    head_repository: { full_name: repository },
    path: ".github/workflows/preview-jazz-tools-alpha-release.yml",
    event: "workflow_dispatch",
    head_branch: "changeset-release/release",
    head_sha: artifactSha,
    ...overrides,
  };
}
function handoff(overrides = {}) {
  return {
    run: run(),
    repository,
    sourceSha: deploySha,
    branch: "release",
    artifactCommit: { sha: artifactSha, tree: { sha: tree } },
    deploymentCommit: { sha: deploySha, tree: { sha: tree } },
    branchRef: { ref: "refs/heads/release", object: { type: "commit", sha: deploySha } },
    ...overrides,
  };
}

test("accepts the successful release-preview dispatch metadata shape and identical merge tree", () => {
  // Same caller/event/ref shape as release recovery run 35027952996, with synthetic identities.
  assert.equal(verifyInspectorHandoff(handoff()), artifactSha);
});
for (const [path, event, head_branch] of [
  ["preview-build.yml", "pull_request", "feature/synthetic"],
  ["preview-jazz-tools-alpha-release.yml", "push", "changeset-release/release"],
  ["publish-jazz-tools-alpha.yml", "push", "release"],
  ["publish-jazz-tools-alpha.yml", "workflow_dispatch", "release"],
]) {
  test(`accepts approved caller ${path} on ${event}`, () => {
    assert.equal(
      inspectorRunSource(
        run({ path: `.github/workflows/${path}`, event, head_branch }),
        repository,
      ),
      artifactSha,
    );
  });
}
for (const [name, changes] of [
  ["another successful same-repository workflow", { path: ".github/workflows/unrelated.yml" }],
  ["unapproved reusable builder as caller", { path: ".github/workflows/build-jazz-packages.yml" }],
  [
    "preview workflow on wrong event",
    { path: ".github/workflows/preview-build.yml", event: "workflow_dispatch" },
  ],
  ["release preview on arbitrary branch", { head_branch: "feature/arbitrary" }],
  [
    "publisher on arbitrary branch",
    { path: ".github/workflows/publish-jazz-tools-alpha.yml", head_branch: "feature/arbitrary" },
  ],
  [
    "publisher on pull_request_target",
    {
      path: ".github/workflows/publish-jazz-tools-alpha.yml",
      head_branch: "release",
      event: "pull_request_target",
    },
  ],
  ["unsuccessful producer", { conclusion: "failure" }],
  ["unfinished producer", { status: "in_progress" }],
  ["fork producer", { head_repository: { full_name: "other/project" } }],
  ["malformed source SHA", { head_sha: "main" }],
]) {
  test(`rejects ${name}`, () =>
    assert.throws(() => inspectorRunSource(run(changes), repository), /Inspector artifact run/));
}
for (const [name, changes] of [
  [
    "different source tree",
    { deploymentCommit: { sha: deploySha, tree: { sha: "d".repeat(40) } } },
  ],
  ["missing tree identity", { artifactCommit: { sha: artifactSha } }],
  ["wrong artifact commit returned", { artifactCommit: { sha: deploySha, tree: { sha: tree } } }],
  [
    "wrong deployment commit returned",
    { deploymentCommit: { sha: artifactSha, tree: { sha: tree } } },
  ],
  [
    "branch moved",
    { branchRef: { ref: "refs/heads/release", object: { type: "commit", sha: artifactSha } } },
  ],
  [
    "tag instead of branch",
    { branchRef: { ref: "refs/tags/release", object: { type: "commit", sha: deploySha } } },
  ],
  ["raw commit instead of branch", { branchRef: { sha: deploySha } }],
  [
    "wrong branch",
    { branchRef: { ref: "refs/heads/other", object: { type: "commit", sha: deploySha } } },
  ],
]) {
  test(`rejects handoff with ${name}`, () =>
    assert.throws(() => verifyInspectorHandoff(handoff(changes)), /Inspector/));
}

test("GitHub orchestration resolves only an explicit branch ref and verifies commit identity", () => {
  const paths = [];
  const responses = [
    run(),
    handoff().artifactCommit,
    handoff().deploymentCommit,
    handoff().branchRef,
  ];
  assert.equal(
    verifyInspectorRunFromGitHub(
      {
        GITHUB_REPOSITORY: repository,
        SOURCE_BRANCH: "release",
        SOURCE_SHA: deploySha,
        ARTIFACT_RUN_ID: "123",
      },
      (path) => {
        paths.push(path);
        return responses.shift();
      },
    ),
    artifactSha,
  );
  assert.deepEqual(paths, [
    `repos/${repository}/actions/runs/123`,
    `repos/${repository}/git/commits/${artifactSha}`,
    `repos/${repository}/git/commits/${deploySha}`,
    `repos/${repository}/git/ref/heads/release`,
  ]);
  assert.equal(
    inspectorBranchRefPath(repository, "feature/synthetic"),
    `repos/${repository}/git/ref/heads/feature%2Fsynthetic`,
  );
  assert.throws(() => inspectorBranchRefPath(repository, "refs/tags/release"), /short branch name/);
});

test("rejects an unapproved producer before querying commits or branch", () => {
  let requests = 0;
  assert.throws(
    () =>
      verifyInspectorRunFromGitHub(
        {
          GITHUB_REPOSITORY: repository,
          SOURCE_BRANCH: "release",
          SOURCE_SHA: deploySha,
          ARTIFACT_RUN_ID: "123",
        },
        () => {
          requests++;
          return run({ path: ".github/workflows/unrelated.yml" });
        },
      ),
    /unapproved producer/,
  );
  assert.equal(requests, 1);
});

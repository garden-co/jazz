import assert from "node:assert/strict";
import test from "node:test";
import { promoteInspectorDeployment } from "./promote-inspector-deployment.mjs";

const env = {
  VERCEL_ORG_ID: "team_test",
  VERCEL_PROJECT_ID: "prj_test",
  VERCEL_TOKEN: "synthetic",
  DEPLOYMENT_URL: "https://synthetic.vercel.app",
  DEPLOY_SHA: "abc",
};
const deployment = {
  id: "dpl_test",
  url: "synthetic.vercel.app",
  projectId: "prj_test",
  ownerId: "team_test",
  target: "production",
  readyState: "READY",
  readySubstate: "STAGED",
  meta: { githubCommitSha: "abc" },
};
const project = { id: "prj_test", accountId: "team_test" };
function harness({
  dryRun = false,
  deploy = deployment,
  initial = project,
  polls = [
    {
      ...project,
      targets: { production: { id: "dpl_test" } },
      lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "succeeded" },
    },
  ],
  status = 200,
} = {}) {
  const calls = [];
  let projectReads = 0;
  const options = {
    env: { ...env, DRY_RUN: String(dryRun) },
    log() {},
    sleep: async () => {},
    attempts: 2,
    fetchImpl: async (url, init) => {
      calls.push({ url, method: init.method });
      assert.equal(new URL(url).searchParams.get("teamId"), "team_test");
      assert.equal(init.headers.Authorization, "Bearer synthetic");
      assert.ok(!url.includes("/user") && !url.includes("/teams"));
      const body =
        init.method === "POST"
          ? {}
          : url.includes("/v13/deployments/")
            ? deploy
            : projectReads++ === 0
              ? initial
              : (polls.shift() ?? project);
      return {
        ok: status < 400,
        status,
        json: async () => body,
        text: async () => {
          throw new Error("must not read error body");
        },
      };
    },
  };
  return { calls, options };
}
test("dry run verifies exact deployment and project without any mutation", async () => {
  const { calls, options } = harness({ dryRun: true });
  await promoteInspectorDeployment(options);
  assert.deepEqual(
    calls.map((c) => c.method),
    ["GET", "GET"],
  );
});
test("promotion posts only scoped verified IDs and waits for exact production target", async () => {
  const { calls, options } = harness({
    polls: [
      project,
      {
        ...project,
        targets: { production: { id: "dpl_test" } },
        lastAliasRequest: {
          toDeploymentId: "dpl_test",
          jobStatus: "succeeded",
        },
      },
    ],
  });
  await promoteInspectorDeployment(options);
  assert.deepEqual(
    calls.map((c) => c.method),
    ["GET", "GET", "POST", "GET", "GET"],
  );
  assert.equal(new URL(calls[2].url).pathname, "/v10/projects/prj_test/promote/dpl_test");
});
test("already current target is an idempotent no-op", async () => {
  const { calls, options } = harness({
    initial: {
      ...project,
      targets: { production: { id: "dpl_test" } },
      lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "succeeded" },
    },
  });
  await promoteInspectorDeployment(options);
  assert.equal(calls.length, 2);
});
for (const changes of [
  { ownerId: "team_other" },
  { projectId: "prj_other" },
  { target: "preview" },
  { readyState: "ERROR" },
  { readySubstate: "OTHER" },
  { meta: { githubCommitSha: "other" } },
  { url: "other.vercel.app" },
]) {
  test(`rejects mismatched deployment ${Object.keys(changes)[0]} before mutation`, async () => {
    const { calls, options } = harness({
      deploy: { ...deployment, ...changes },
    });
    await assert.rejects(promoteInspectorDeployment(options), /mismatch/);
    assert.ok(calls.every((c) => c.method === "GET"));
  });
}
test("fails when promotion target does not converge", async () => {
  const { options } = harness({ polls: [project, project] });
  await assert.rejects(promoteInspectorDeployment(options), /Timed out/);
});
test("fails on terminal alias failure", async () => {
  const { options } = harness({
    polls: [
      {
        ...project,
        lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "failed" },
      },
    ],
  });
  await assert.rejects(promoteInspectorDeployment(options), /promotion failed/);
});
test("API errors omit response bodies", async () => {
  const { options } = harness({ status: 403 });
  await assert.rejects(promoteInspectorDeployment(options), /failed \(403\)/);
});

test("target assignment alone does not count as alias completion", async () => {
  const { options } = harness({
    polls: [
      {
        ...project,
        targets: { production: { id: "dpl_test" } },
        lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "pending" },
      },
    ],
  });
  await assert.rejects(promoteInspectorDeployment(options), /Timed out/);
});
test("rolling release configuration fails before mutation", async () => {
  const { calls, options } = harness({
    initial: { ...project, rollingRelease: {} },
  });
  await assert.rejects(promoteInspectorDeployment(options), /rolling releases/);
  assert.ok(calls.every((c) => c.method === "GET"));
});

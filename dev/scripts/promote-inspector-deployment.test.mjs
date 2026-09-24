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
const promotedTarget = {
  id: "dpl_test",
  readyState: "READY",
  readySubstate: "PROMOTED",
  aliasAssigned: 123,
  aliasError: null,
};
const project = { id: "prj_test", accountId: "team_test" };
function harness({
  dryRun = false,
  deploy = deployment,
  initial = project,
  polls = [
    {
      ...project,
      targets: { production: promotedTarget },
      lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "succeeded" },
    },
  ],
  status = 200,
  domains = [{ name: "inspector.example.test", projectId: "prj_test", verified: true }],
  aliasDeployment = "dpl_test",
  pagination,
} = {}) {
  const calls = [];
  let projectReads = 0;
  let lastPoll = initial;
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
          : url.includes("/domains?")
            ? { domains, pagination }
            : url.includes("/v4/aliases/")
              ? {
                  alias: "inspector.example.test",
                  projectId: "prj_test",
                  deploymentId: aliasDeployment,
                }
              : url.includes("/v13/deployments/")
                ? deploy
                : projectReads++ === 0
                  ? initial
                  : (lastPoll = polls.shift() ?? lastPoll);
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
        targets: { production: promotedTarget },
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
    ["GET", "GET", "POST", "GET", "GET", "GET", "GET", "GET"],
  );
  assert.equal(new URL(calls[2].url).pathname, "/v10/projects/prj_test/promote/dpl_test");
});
test("already current target is an idempotent no-op", async () => {
  const { calls, options } = harness({
    initial: {
      ...project,
      targets: { production: promotedTarget },
      lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus: "succeeded" },
    },
  });
  await promoteInspectorDeployment(options);
  assert.equal(calls.length, 5);
  assert.ok(calls.every((c) => c.method === "GET"));
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
        targets: { production: promotedTarget },
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

test("malformed JSON errors omit response body details", async () => {
  const marker = "synthetic-private-response-marker";
  const { options } = harness();
  options.fetchImpl = async () => ({
    ok: true,
    status: 200,
    json: async () => {
      throw new SyntaxError(`Unexpected token in ${marker}`);
    },
  });
  await assert.rejects(promoteInspectorDeployment(options), (error) => {
    assert.equal(error.message, "Vercel GET request returned invalid JSON.");
    assert.ok(!String(error).includes(marker));
    assert.equal(error.cause, undefined);
    return true;
  });
});

test("null alias request succeeds only with promoted target and matching production domain", async () => {
  const { options } = harness({
    polls: [{ ...project, targets: { production: promotedTarget }, lastAliasRequest: null }],
  });
  await promoteInspectorDeployment(options);
});
for (const target of [
  { ...promotedTarget, readySubstate: "STAGED" },
  { ...promotedTarget, aliasAssigned: false },
  { ...promotedTarget, aliasAssigned: 0 },
  { ...promotedTarget, aliasError: { code: "failed" } },
  { ...promotedTarget, id: "dpl_other" },
])
  test(`rejects incomplete target receipt ${JSON.stringify(target)}`, async () => {
    const { options } = harness({
      polls: [{ ...project, targets: { production: target }, lastAliasRequest: null }],
    });
    await assert.rejects(promoteInspectorDeployment(options), /Timed out/);
  });
test("matching target with stale domain alias never succeeds", async () => {
  const { options } = harness({ aliasDeployment: "dpl_old" });
  await assert.rejects(promoteInspectorDeployment(options), /Timed out/);
});
test("partial domain list fails closed", async () => {
  const { options } = harness({ pagination: { next: 123 } });
  await assert.rejects(promoteInspectorDeployment(options), /incomplete/);
});
test("empty domain list is not vacuous success", async () => {
  const { options } = harness({ domains: [] });
  await assert.rejects(promoteInspectorDeployment(options), /no verified/);
});

test("target changing during domain verification is not accepted", async () => {
  const { options } = harness({
    polls: [
      { ...project, targets: { production: promotedTarget }, lastAliasRequest: null },
      { ...project, targets: { production: { ...promotedTarget, id: "dpl_other" } } },
    ],
  });
  await assert.rejects(promoteInspectorDeployment(options), /Timed out/);
});

for (const jobStatus of ["failed", "skipped"]) {
  test(`a previous ${jobStatus} attempt permits a fresh promotion request`, async () => {
    const { calls, options } = harness({
      initial: { ...project, lastAliasRequest: { toDeploymentId: "dpl_test", jobStatus } },
    });
    await promoteInspectorDeployment(options);
    assert.equal(calls.filter((call) => call.method === "POST").length, 1);
  });
}

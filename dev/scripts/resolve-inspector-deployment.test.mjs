import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { resolveInspectorDeployment } from "./resolve-inspector-deployment.mjs";

const requiredEnv = {
  VERCEL_ORG_ID: "team_alice",
  VERCEL_PROJECT_ID: "project_inspector",
  VERCEL_TOKEN: "vercel_token",
};

function makeOutputFile() {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-inspector-deployment-"));
  return path.join(tempRoot, "github-output");
}

function jsonResponse(body, init = {}) {
  return {
    ok: init.ok ?? true,
    status: init.status ?? 200,
    async json() {
      return body;
    },
    async text() {
      return JSON.stringify(body);
    },
  };
}

test("resolveInspectorDeployment writes GitHub output for the latest staged deployment from main", async () => {
  const outputFile = makeOutputFile();
  const requests = [];
  const logs = [];

  const result = await resolveInspectorDeployment({
    env: { ...requiredEnv, GITHUB_OUTPUT: outputFile },
    fetchImpl: async (url, init) => {
      requests.push({ url: String(url), authorization: init.headers.Authorization });
      return jsonResponse({
        deployments: [
          {
            url: "jazz-inspector-git-main-alice.vercel.app",
            readyState: "READY",
            target: "production",
            readySubstate: "STAGED",
          },
        ],
      });
    },
    log: (message) => logs.push(message),
    sleep: async () => {
      throw new Error("resolver should not wait after finding a staged deployment");
    },
  });

  assert.deepEqual(result, {
    deploymentUrl: "https://jazz-inspector-git-main-alice.vercel.app",
    alreadyPromoted: false,
  });
  assert.equal(
    fs.readFileSync(outputFile, "utf8"),
    [
      "deployment_url=https://jazz-inspector-git-main-alice.vercel.app",
      "already_promoted=false",
      "",
    ].join("\n"),
  );
  assert.equal(requests.length, 1);
  assert.equal(requests[0].authorization, "Bearer vercel_token");
  const requestUrl = new URL(requests[0].url);
  assert.equal(requestUrl.origin + requestUrl.pathname, "https://api.vercel.com/v6/deployments");
  assert.equal(requestUrl.searchParams.get("projectId"), "project_inspector");
  assert.equal(requestUrl.searchParams.get("target"), "production");
  assert.equal(requestUrl.searchParams.get("state"), "READY");
  assert.equal(requestUrl.searchParams.get("branch"), "main");
  assert.equal(requestUrl.searchParams.has("sha"), false);
  assert.equal(requestUrl.searchParams.get("teamId"), "team_alice");
  assert.deepEqual(logs, [
    "Resolved inspector deployment: jazz-inspector-git-main-alice.vercel.app state=READY target=production substate=STAGED",
  ]);
});

test("resolveInspectorDeployment marks an already promoted deployment", async () => {
  const outputFile = makeOutputFile();

  const result = await resolveInspectorDeployment({
    env: { ...requiredEnv, GITHUB_OUTPUT: outputFile },
    fetchImpl: async () =>
      jsonResponse({
        deployments: [
          {
            url: "jazz-inspector.vercel.app",
            readyState: "READY",
            target: "production",
            readySubstate: "PROMOTED",
          },
        ],
      }),
    log: () => {},
  });

  assert.deepEqual(result, {
    deploymentUrl: "https://jazz-inspector.vercel.app",
    alreadyPromoted: true,
  });
  assert.equal(
    fs.readFileSync(outputFile, "utf8"),
    ["deployment_url=https://jazz-inspector.vercel.app", "already_promoted=true", ""].join("\n"),
  );
});

test("resolveInspectorDeployment uses the latest main deployment before older staged deployments", async () => {
  const outputFile = makeOutputFile();

  const result = await resolveInspectorDeployment({
    env: { ...requiredEnv, GITHUB_OUTPUT: outputFile },
    fetchImpl: async () =>
      jsonResponse({
        deployments: [
          {
            url: "jazz-inspector-latest-main.vercel.app",
            readyState: "READY",
            target: "production",
            readySubstate: "PROMOTED",
          },
          {
            url: "jazz-inspector-older-main.vercel.app",
            readyState: "READY",
            target: "production",
            readySubstate: "STAGED",
          },
        ],
      }),
    log: () => {},
  });

  assert.deepEqual(result, {
    deploymentUrl: "https://jazz-inspector-latest-main.vercel.app",
    alreadyPromoted: true,
  });
  assert.equal(
    fs.readFileSync(outputFile, "utf8"),
    [
      "deployment_url=https://jazz-inspector-latest-main.vercel.app",
      "already_promoted=true",
      "",
    ].join("\n"),
  );
});

test("resolveInspectorDeployment reports the last matching deployments when none is staged", async () => {
  const outputFile = makeOutputFile();
  const sleeps = [];

  await assert.rejects(
    resolveInspectorDeployment({
      env: { ...requiredEnv, GITHUB_OUTPUT: outputFile },
      fetchImpl: async () =>
        jsonResponse({
          deployments: [
            {
              url: "jazz-inspector-main.vercel.app",
              readyState: "READY",
              target: "production",
              readySubstate: "QUEUED",
            },
          ],
        }),
      attempts: 2,
      delayMs: 5,
      log: () => {},
      sleep: async (delayMs) => {
        sleeps.push(delayMs);
      },
    }),
    /No staged inspector production deployment found on main\.[\s\S]*jazz-inspector-main\.vercel\.app state=READY target=production substate=QUEUED/,
  );
  assert.deepEqual(sleeps, [5]);
  assert.equal(fs.existsSync(outputFile), false);
});

test("resolveInspectorDeployment requires all CI environment variables", async () => {
  const env = { ...requiredEnv };
  delete env.VERCEL_TOKEN;

  await assert.rejects(
    resolveInspectorDeployment({
      env,
      fetchImpl: async () => {
        throw new Error("fetch should not run without credentials");
      },
      log: () => {},
    }),
    /Missing required environment variable VERCEL_TOKEN/,
  );
});

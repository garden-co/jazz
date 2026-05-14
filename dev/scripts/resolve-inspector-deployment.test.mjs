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
  RELEASE_SHA: "abc123",
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

test("resolveInspectorDeployment writes GitHub output for a staged deployment", async () => {
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
  assert.match(requests[0].url, /^https:\/\/api\.vercel\.com\/v6\/deployments\?/);
  assert.match(requests[0].url, /projectId=project_inspector/);
  assert.match(requests[0].url, /target=production/);
  assert.match(requests[0].url, /state=READY/);
  assert.match(requests[0].url, /branch=main/);
  assert.match(requests[0].url, /sha=abc123/);
  assert.match(requests[0].url, /teamId=team_alice/);
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
    /No staged inspector production deployment found for abc123\.[\s\S]*jazz-inspector-main\.vercel\.app state=READY target=production substate=QUEUED/,
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

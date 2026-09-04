import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import { shouldSkipDocsDeploy } from "../../scripts/skip-docs-vercel-deploy.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const vercelConfig = JSON.parse(fs.readFileSync(path.join(root, "docs/vercel.json"), "utf8"));
const requiredEnv = {
  VERCEL_ENV: "preview",
  VERCEL_GIT_REPO_OWNER: "garden-co",
  VERCEL_GIT_REPO_SLUG: "jazz",
  VERCEL_GIT_PULL_REQUEST_ID: "2350",
};

function jsonResponse(body, init = {}) {
  return {
    ok: init.ok ?? true,
    status: init.status ?? 200,
    async json() {
      return body;
    },
  };
}

function pullRequest(overrides = {}) {
  return {
    state: "open",
    author_association: "MEMBER",
    labels: [{ name: "docs" }],
    head: { repo: { full_name: "garden-co/jazz" } },
    base: { repo: { full_name: "garden-co/jazz" } },
    ...overrides,
  };
}

test("docs Vercel config delegates to the fail-closed preview filter", () => {
  assert.equal(vercelConfig.buildCommand, "pnpm build");
  assert.equal(vercelConfig.ignoreCommand, "node ../dev/scripts/skip-docs-vercel-deploy.mjs");
  assert.equal(fs.existsSync(path.join(root, ".github/workflows/docs-vercel-preview.yml")), false);
});

test("builds only an open trusted same-repository PR carrying docs", async () => {
  const requests = [];
  const skip = await shouldSkipDocsDeploy({
    env: requiredEnv,
    fetchImpl: async (url, init) => {
      requests.push({ url: String(url), headers: init.headers });
      return jsonResponse(pullRequest());
    },
  });
  assert.equal(skip, false);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "https://api.github.com/repos/garden-co/jazz/pulls/2350");
  assert.equal(requests[0].headers.Accept, "application/vnd.github+json");
  assert.equal("Authorization" in requests[0].headers, false);
});

test("skips production, non-PR previews, and incomplete or malformed metadata", async () => {
  for (const env of [
    { ...requiredEnv, VERCEL_ENV: "production" },
    { ...requiredEnv, VERCEL_GIT_PULL_REQUEST_ID: undefined },
    { ...requiredEnv, VERCEL_GIT_PULL_REQUEST_ID: "2350/../../secrets" },
    { ...requiredEnv, VERCEL_GIT_REPO_OWNER: undefined },
    { ...requiredEnv, VERCEL_GIT_REPO_SLUG: undefined },
  ]) {
    let fetched = false;
    const skip = await shouldSkipDocsDeploy({
      env,
      fetchImpl: async () => {
        fetched = true;
        throw new Error("must not fetch without complete preview PR metadata");
      },
    });
    assert.equal(skip, true);
    assert.equal(fetched, false);
  }
});

test("skips unlabeled, closed, fork, and untrusted pull requests", async () => {
  const cases = [
    pullRequest({ labels: [] }),
    pullRequest({ labels: [{ name: "documentation" }] }),
    pullRequest({ state: "closed" }),
    pullRequest({ head: { repo: { full_name: "some-fork/jazz" } } }),
    pullRequest({ author_association: "NONE" }),
  ];
  for (const body of cases) {
    assert.equal(
      await shouldSkipDocsDeploy({ env: requiredEnv, fetchImpl: async () => jsonResponse(body) }),
      true,
    );
  }
});

test("fails closed on denial, malformed response, network error, and timeout", async () => {
  const fetches = [
    async () => jsonResponse({}, { ok: false, status: 403 }),
    async () => jsonResponse({ state: "open" }),
    async () => {
      throw new Error("network unavailable");
    },
  ];
  for (const fetchImpl of fetches) {
    assert.equal(await shouldSkipDocsDeploy({ env: requiredEnv, fetchImpl }), true);
  }

  assert.equal(
    await shouldSkipDocsDeploy({
      env: requiredEnv,
      createAbortSignal: () => AbortSignal.timeout(1),
      fetchImpl: async (_url, init) =>
        new Promise((_, reject) => {
          init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true });
        }),
    }),
    true,
  );
});

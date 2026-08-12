import assert from "node:assert/strict";
import test from "node:test";

import { shouldSkipNewCorePreview } from "./skip-new-core-vercel-preview.mjs";

const requiredEnv = {
  VERCEL_ENV: "preview",
  VERCEL_GIT_REPO_OWNER: "garden-co",
  VERCEL_GIT_REPO_SLUG: "jazz",
  VERCEL_GIT_PULL_REQUEST_ID: "1398",
  VERCEL_GITHUB_READ_TOKEN: "read_only_token",
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

test("skips only a PR targeting the new-core integration branch", async () => {
  const requests = [];

  const skip = await shouldSkipNewCorePreview({
    env: requiredEnv,
    fetchImpl: async (url, init) => {
      requests.push({ url: String(url), headers: init.headers });
      return jsonResponse({ base: { ref: "codex/jazz-core-engine-swap" } });
    },
  });

  assert.equal(skip, true);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "https://api.github.com/repos/garden-co/jazz/pulls/1398");
  assert.equal(requests[0].headers.Accept, "application/vnd.github+json");
  assert.equal(requests[0].headers.Authorization, "Bearer read_only_token");
  assert.equal(requests[0].headers["X-GitHub-Api-Version"], "2022-11-28");
});

test("fails open when the GitHub lookup exceeds its short abort timeout", async () => {
  const skip = await shouldSkipNewCorePreview({
    env: requiredEnv,
    createAbortSignal: () => AbortSignal.timeout(1),
    fetchImpl: async (_url, init) => {
      await new Promise((_, reject) => {
        if (init.signal.aborted) {
          reject(init.signal.reason);
          return;
        }
        init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true });
      });
    },
  });

  assert.equal(skip, false);
});

test("builds PRs targeting every other branch", async () => {
  const skip = await shouldSkipNewCorePreview({
    env: requiredEnv,
    fetchImpl: async () => jsonResponse({ base: { ref: "main" } }),
  });

  assert.equal(skip, false);
});

test("fails open outside preview or without a Vercel PR id, repository coordinates, or token", async () => {
  for (const missing of [
    "VERCEL_ENV",
    "VERCEL_GIT_PULL_REQUEST_ID",
    "VERCEL_GIT_REPO_OWNER",
    "VERCEL_GIT_REPO_SLUG",
    "VERCEL_GITHUB_READ_TOKEN",
  ]) {
    const env = { ...requiredEnv };
    delete env[missing];
    let fetched = false;

    const skip = await shouldSkipNewCorePreview({
      env,
      fetchImpl: async () => {
        fetched = true;
        throw new Error("must not fetch without complete inputs");
      },
    });

    assert.equal(skip, false, missing);
    assert.equal(fetched, false, missing);
  }
});

test("fails open for a malformed PR id, unsuccessful response, malformed response, and network error", async () => {
  const malformedId = await shouldSkipNewCorePreview({
    env: { ...requiredEnv, VERCEL_GIT_PULL_REQUEST_ID: "1398/../../secrets" },
    fetchImpl: async () => {
      throw new Error("must not fetch a malformed id");
    },
  });
  assert.equal(malformedId, false);

  for (const fetchImpl of [
    async () => jsonResponse({}, { ok: false, status: 403 }),
    async () => jsonResponse({ base: {} }),
    async () => {
      throw new Error("network unavailable");
    },
  ]) {
    assert.equal(await shouldSkipNewCorePreview({ env: requiredEnv, fetchImpl }), false);
  }
});

import assert from "node:assert/strict";
import test from "node:test";

import { createResettablePromise, loadSecret, SecretLoadError } from "./secret-promise-cache.ts";

test("render retries reuse one pending secret load", async () => {
  let loadCount = 0;
  let resolveSecret;
  const pendingSecret = new Promise((resolve) => {
    resolveSecret = resolve;
  });
  const secret = createResettablePromise(() => {
    loadCount += 1;
    return pendingSecret;
  });

  const firstRender = secret.get();
  const retryRender = secret.get();
  const rerender = secret.get();

  assert.strictEqual(retryRender, firstRender);
  assert.strictEqual(rerender, firstRender);
  assert.equal(loadCount, 1);

  resolveSecret("stable-secret");
  assert.equal(await firstRender, "stable-secret");
  assert.strictEqual(secret.get(), firstRender);
  assert.equal(loadCount, 1);
});

test("an explicit reset starts one new load after an error", async () => {
  const failure = new Error("secure storage unavailable");
  let loadCount = 0;
  const secret = createResettablePromise(() => {
    loadCount += 1;
    return loadCount === 1 ? Promise.reject(failure) : Promise.resolve("recovered-secret");
  });

  const failedLoad = secret.get();
  await assert.rejects(failedLoad, failure);
  assert.strictEqual(secret.get(), failedLoad);
  assert.equal(loadCount, 1);

  secret.reset();

  assert.equal(await secret.get(), "recovered-secret");
  assert.equal(loadCount, 2);
});

test("secret-load failures are distinguishable from descendant errors", async () => {
  const failure = new Error("secure storage unavailable");

  await assert.rejects(
    loadSecret(() => Promise.reject(failure)),
    (error) => error instanceof SecretLoadError && error.cause === failure,
  );
});

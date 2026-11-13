import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { previewUrl, resolves, summary } from "./check-pkg-pr-new-urls.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const commit = "71facd9e972a5d031ed38fca4f26ae38a8914a07";

test("preview URLs use the 7-character short SHA pkg.pr.new prints", () => {
  assert.equal(
    previewUrl("jazz-wasm", commit),
    "https://pkg.pr.new/garden-co/jazz/jazz-wasm@71facd9",
  );
  assert.equal(
    previewUrl("@garden-co/jazz-napi-linux-x64-gnu", commit),
    "https://pkg.pr.new/garden-co/jazz/@garden-co/jazz-napi-linux-x64-gnu@71facd9",
  );
  for (const bad of ["71facd9", commit.toUpperCase(), "", undefined])
    assert.throws(() => previewUrl("jazz-tools", bad));
});

test("the summary lists one install command per package", () => {
  const text = summary([previewUrl("jazz-tools", commit), previewUrl("jazz-wasm", commit)]);
  assert.match(text, /^pnpm add https:\/\/pkg\.pr\.new\/garden-co\/jazz\/jazz-tools@71facd9$/m);
  assert.match(text, /^pnpm add https:\/\/pkg\.pr\.new\/garden-co\/jazz\/jazz-wasm@71facd9$/m);
});

test("resolves retries until pkg.pr.new serves the URL, and gives up after its attempts", async () => {
  let calls = 0;
  const flaky = async () => ({ ok: ++calls >= 3 });
  assert.equal(await resolves("u", { attempts: 5, delayMs: 0, fetchImpl: flaky }), true);
  assert.equal(calls, 3);
  calls = 0;
  const missing = async () => {
    calls++;
    throw new Error("network");
  };
  assert.equal(await resolves("u", { attempts: 3, delayMs: 0, fetchImpl: missing }), false);
  assert.equal(calls, 3);
});

test("every attempt is bounded by a timeout signal", async () => {
  const stalled = (_url, { signal }) =>
    new Promise((_resolve, reject) =>
      signal.addEventListener("abort", () => reject(signal.reason)),
    );
  const started = Date.now();
  assert.equal(
    await resolves("u", { attempts: 2, delayMs: 0, timeoutMs: 50, fetchImpl: stalled }),
    false,
  );
  assert.ok(Date.now() - started < 2_000, "a stalled connection must not hold the check");
});

test("the preview workflow checks every package it publishes, right after publishing", () => {
  const workflow = readFileSync(path.join(root, ".github/workflows/preview-build.yml"), "utf8");
  const publish = workflow.indexOf("pnpm exec pkg-pr-new publish");
  const check = workflow.indexOf(
    'node dev/scripts/check-pkg-pr-new-urls.mjs "${PREVIEW_COMMIT}" "${PACKAGES[@]}"',
  );
  assert.ok(publish > 0, "the preview publish step exists");
  assert.ok(
    check > publish,
    "the short-SHA check runs after the publish, on the same package list",
  );
  assert.equal(workflow.slice(publish, check).includes("- name:"), false, "and in the same step");
});

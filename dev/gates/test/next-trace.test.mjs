import test from "node:test";
import assert from "node:assert/strict";
import { shouldRun, touchesGuardedPaths } from "../next-trace.mjs";

test("a ready PR runs the Next.js trace test only when it touches what the test guards", () => {
  for (const file of [
    "packages/jazz-tools/src/backend/create-jazz.ts",
    "packages/jazz-tools/src/backend/create-jazz-session.ts",
    "packages/jazz-tools/src/dev/codegen-napi.ts",
    "crates/jazz-napi/src/lib.rs",
    "packages/jazz-tools/scripts/next-trace-napi.test.mjs",
    "dev/gates/next-trace.mjs",
  ])
    assert.equal(touchesGuardedPaths(["README.md", file]), true, file);
  for (const file of [
    "packages/jazz-tools/src/runtime/db.ts",
    "packages/jazz-tools/src/dev/codegen-napi.test.ts",
    "crates/jazz/src/lib.rs",
    "docs/content/docs/index.mdx",
  ])
    assert.equal(touchesGuardedPaths([file]), false, file);
});

test("it always runs outside a pull request, and in one whose diff can't be read", () => {
  const unread = () => {
    throw new Error("git must not be asked outside a pull request");
  };
  assert.equal(shouldRun({ env: { GITHUB_EVENT_NAME: "push" }, git: unread }).run, true);
  assert.equal(shouldRun({ env: {}, git: unread }).run, true);
  const pr = { GITHUB_EVENT_NAME: "pull_request" };
  assert.equal(shouldRun({ env: pr, git: () => null }).run, true);
  assert.equal(shouldRun({ env: pr, git: () => "docs/a.mdx\n" }).run, false);
  assert.equal(
    shouldRun({ env: pr, git: () => "docs/a.mdx\ncrates/jazz-napi/src/lib.rs\n" }).run,
    true,
  );
});

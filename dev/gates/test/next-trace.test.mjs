import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { shouldRun, touchesGuardedPaths } from "../next-trace.mjs";

const script = fileURLToPath(new URL("../next-trace.mjs", import.meta.url));

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

test("--decide writes the decision to $GITHUB_OUTPUT and runs nothing", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "next-trace-decide-"));
  try {
    const output = path.join(dir, "output");
    const result = spawnSync(process.execPath, [script, "--decide"], {
      encoding: "utf8",
      env: { ...process.env, GITHUB_EVENT_NAME: "push", GITHUB_OUTPUT: output },
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(fs.readFileSync(output, "utf8"), "run=true\n");
    assert.match(result.stdout, /next-trace: runs, not a pull request/);
    assert.doesNotMatch(result.stdout, /ensure-correctness-artifacts/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

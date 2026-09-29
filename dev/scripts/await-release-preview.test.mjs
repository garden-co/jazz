import assert from "node:assert/strict";
import test from "node:test";

import { PREVIEW_WORKFLOW, previewDecision, previewRunMismatch } from "./await-release-preview.mjs";

const run = (id, status, conclusion, created_at = "2026-09-29T00:00:00Z") => ({
  id,
  status,
  conclusion,
  created_at,
});

test("a successful preview of the commit is used, newest first", () => {
  assert.deepEqual(
    previewDecision([
      run(1, "completed", "success", "2026-09-29T00:00:00Z"),
      run(2, "completed", "success", "2026-09-29T01:00:00Z"),
      run(3, "completed", "failure", "2026-09-29T02:00:00Z"),
    ]),
    { state: "ready", runId: 2 },
  );
});

test("a preview still running is waited for, even beside a failed one", () => {
  assert.deepEqual(
    previewDecision([run(1, "completed", "cancelled"), run(2, "in_progress", null)]),
    { state: "waiting" },
  );
  assert.deepEqual(previewDecision([run(1, "queued", null)]), { state: "waiting" });
});

test("a commit whose previews all finished without success fails", () => {
  const decision = previewDecision([
    run(1, "completed", "failure"),
    run(2, "completed", "cancelled"),
  ]);
  assert.equal(decision.state, "failed");
  assert.match(decision.reason, /1 \(failure\), 2 \(cancelled\)/);
});

test("no preview run yet is reported as missing", () => {
  assert.deepEqual(previewDecision([]), { state: "missing" });
});

const sha = "a".repeat(40);
const preview = (overrides) => ({
  id: 7,
  path: `.github/workflows/${PREVIEW_WORKFLOW}`,
  head_sha: sha,
  head_branch: "changeset-release/release",
  ...overrides,
});
const head = { headSha: sha, headBranch: "changeset-release/release" };

test("a preview of this commit and branch stands for it", () => {
  assert.equal(previewRunMismatch(preview(), head), undefined);
});

test("a pinned preview of an older commit cannot satisfy this commit's gate", () => {
  assert.match(previewRunMismatch(preview({ head_sha: "b".repeat(40) }), head), /not this commit/);
});

test("a preview from another branch or workflow is not used", () => {
  assert.match(previewRunMismatch(preview({ head_branch: "main" }), head), /ran on main/);
  assert.match(
    previewRunMismatch(preview({ path: ".github/workflows/ci.yml" }), head),
    /is not a preview-jazz-tools/,
  );
});

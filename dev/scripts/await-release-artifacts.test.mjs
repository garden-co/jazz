import assert from "node:assert/strict";
import test from "node:test";

import { artifactsDecision, awaitArtifacts } from "./await-release-artifacts.mjs";

const sha = "a".repeat(40);
const path = ".github/workflows/release-artifacts.yml";
const run = (id, extra = {}) => ({
  id,
  head_sha: sha,
  path,
  status: "completed",
  conclusion: "success",
  created_at: `2026-11-19T00:0${id}:00Z`,
  ...extra,
});

test("the newest successful run of the commit wins", () => {
  assert.deepEqual(artifactsDecision([run(1), run(3), run(2, { conclusion: "failure" })], sha), {
    state: "ready",
    runId: 3,
  });
});

test("ignores runs of other commits and other workflows", () => {
  const decision = artifactsDecision(
    [run(1, { head_sha: "b".repeat(40) }), run(2, { path: ".github/workflows/ci.yml" })],
    sha,
  );
  assert.equal(decision.state, "missing");
});

test("waits while a run is still building", () => {
  assert.equal(artifactsDecision([run(1, { status: "in_progress", conclusion: null })], sha).state, "waiting");
});

test("fails when every run failed, but a cancelled run alone means build", () => {
  assert.equal(artifactsDecision([run(1, { conclusion: "failure" })], sha).state, "failed");
  assert.equal(artifactsDecision([run(1, { conclusion: "cancelled" })], sha).state, "missing");
});

test("awaitArtifacts polls until ready, and returns empty when missing", async () => {
  const answers = [[run(1, { status: "queued", conclusion: null })], [run(1)]];
  const id = await awaitArtifacts({ headSha: sha, list: async () => answers.shift(), pollMs: 1, log: () => {} });
  assert.equal(id, "1");
  assert.equal(
    await awaitArtifacts({ headSha: sha, list: async () => [], pollMs: 1, missingGraceMs: 0, log: () => {} }),
    "",
  );
  // A run dispatched alongside the gate shows up within the grace period.
  const late = [[], [run(2)]];
  assert.equal(
    await awaitArtifacts({ headSha: sha, list: async () => late.shift(), pollMs: 1, missingGraceMs: 60_000, log: () => {} }),
    "2",
  );
});

test("a run still building is ready once the required artifacts are uploaded", async () => {
  const building = run(4, { status: "in_progress", conclusion: null });
  const uploads = [["pkg-jazz-tools"], ["pkg-jazz-tools", "release-server", "other"]];
  const id = await awaitArtifacts({
    headSha: sha,
    list: async () => [building],
    listArtifacts: async () => uploads.shift(),
    required: ["pkg-jazz-tools", "release-server"],
    pollMs: 1,
    log: () => {},
  });
  assert.equal(id, "4");
});

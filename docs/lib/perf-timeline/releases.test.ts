import { test } from "node:test";
import assert from "node:assert/strict";
import { isVersionTag, resolveReleaseAncestors } from "./releases.ts";
import type { RawRun } from "./model.ts";

// Oldest first, as the snapshot script lists them.
const tags = [
  { name: "v2.0.0-alpha.1", sha: "tag1", url: "https://example.com/1" },
  { name: "v2.0.0-alpha.2", sha: "tag2", url: "https://example.com/2" },
];
const run = (sha: string, branch = "main"): RawRun => ({
  id: sha,
  date: "2026-09-13",
  status: "COMPLETED",
  event: "Push",
  commit: { hash: sha, message: sha, branch: { name: branch, pullRequest: null } },
  results: [
    { id: sha, benchmark: { id: "bench", name: "bench" }, walltime: { min: 1, median: 1, max: 1 } },
  ],
});

test("main commits are attributed to the oldest release containing them", () => {
  const containing: Record<string, string[]> = {
    old: ["v2.0.0-alpha.2", "v2.0.0-alpha.1"],
    mid: ["v2.0.0-alpha.2"],
    new: [],
  };
  const asked: string[] = [];
  const result = resolveReleaseAncestors(
    [run("old"), run("old"), run("mid"), run("new"), run("pr", "feature"), run("tag2")],
    tags,
    (sha) => {
      asked.push(sha);
      return new Set(containing[sha]);
    },
  );
  assert.deepEqual(Object.fromEntries(result.included), {
    old: "v2.0.0-alpha.1",
    mid: "v2.0.0-alpha.2",
    tag2: "v2.0.0-alpha.2",
  });
  // Deduplicated, exact tags need no lookup, PR runs are never reclassified.
  assert.deepEqual(asked.sort(), ["mid", "new", "old"]);
  assert.deepEqual(result.warnings, []);
});

test("a commit missing from the checkout stays unreleased with a warning", () => {
  const result = resolveReleaseAncestors([run("gone")], tags, () => null);
  assert.equal(result.included.size, 0);
  assert.match(result.warnings[0], /missing from the release history/);
});

test("only semantic-version tags count as releases", () => {
  for (const name of ["v2.0.0-alpha.57", "2.0.0", "v1.2.3+build"]) assert.ok(isVersionTag(name));
  for (const name of ["jazz-sim-fixtures-v1", "perf-timeline-data", "v2"])
    assert.equal(isVersionTag(name), false);
});

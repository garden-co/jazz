import { test } from "node:test";
import assert from "node:assert/strict";
import { isAncestorComparison, resolveReleaseAncestors } from "./releases.ts";
import type { RawRun } from "./model.ts";

const tags = [{ name: "v2.0.0", sha: "tag", url: "https://example.com" }];
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

test("comparison direction proves ancestry, not date or a diverged merge base", () => {
  for (const status of ["ahead", "identical"]) assert.ok(isAncestorComparison(status));
  for (const status of ["behind", "diverged", "unknown"])
    assert.equal(isAncestorComparison(status), false);
});

test("deduplicates main SHAs and leaves newer/diverged commits unreleased", async () => {
  const calls: string[] = [];
  const result = await resolveReleaseAncestors(
    [run("old"), run("old"), run("new"), run("diverged"), run("pr", "feature"), run("tag")],
    tags,
    async (base, head) => {
      assert.equal(head, "tag");
      calls.push(base);
      return base === "old" ? "ahead" : base === "new" ? "behind" : "diverged";
    },
  );
  assert.deepEqual([...result.included.keys()].sort(), ["old", "tag"]);
  assert.deepEqual(calls.sort(), ["diverged", "new", "old"]);
  assert.deepEqual(result.warnings, []);
});

test("API failure preserves history without inventing release evidence", async () => {
  const result = await resolveReleaseAncestors([run("old")], tags, async () => {
    throw new Error("rate limited");
  });
  assert.equal(result.included.size, 0);
  assert.match(result.warnings[0], /unverified release status/);
});

test("request cap is explicit and never interpreted as non-ancestry proof", async () => {
  let calls = 0;
  const result = await resolveReleaseAncestors(
    [run("a"), run("b")],
    tags,
    async () => {
      calls++;
      return "ahead";
    },
    1,
  );
  assert.equal(calls, 1);
  assert.equal(result.included.size, 1);
  assert.match(result.warnings[0], /request budget/);
});

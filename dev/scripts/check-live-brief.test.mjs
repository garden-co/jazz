import assert from "node:assert/strict";
import test from "node:test";

import { fetchLive, firstDifferences, normalize, verdict } from "./check-live-brief.mjs";

const HOUR = 3_600_000;
const brief = "# Agent Quickstart\n\n1. Decide\n   - Default: Vercel\n";

test("the same brief passes, whatever the line endings and trailing spaces", () => {
  const live = brief.replace(/\n/g, "  \r\n") + "\n\n";
  assert.equal(normalize(live), normalize(brief));
  assert.deepEqual(verdict({ expected: brief, actual: live, changedAt: 0 }).ok, true);
});

test("a stale live brief fails, naming the first differing line", () => {
  const live = brief.replace("Vercel", "Netlify");
  const result = verdict({ expected: brief, actual: live, changedAt: 0, now: 48 * HOUR });
  assert.equal(result.ok, false);
  assert.deepEqual(result.diff, [
    "line 4:",
    "  repo:    - Default: Vercel",
    "  live:    - Default: Netlify",
  ]);
});

test("a difference passes within the grace period after the brief changed", () => {
  const live = brief + "older";
  const changedAt = 100 * HOUR;
  assert.equal(
    verdict({
      expected: brief,
      actual: live,
      changedAt,
      now: changedAt + 2 * HOUR,
      graceMs: 24 * HOUR,
    }).ok,
    true,
  );
  assert.equal(
    verdict({
      expected: brief,
      actual: live,
      changedAt,
      now: changedAt + 25 * HOUR,
      graceMs: 24 * HOUR,
    }).ok,
    false,
  );
});

test("a missing line is reported as missing", () => {
  assert.deepEqual(firstDifferences("a\nb", "a"), ["line 2:", "  repo: b", "  live: (missing)"]);
});

test("an HTTP error is an error, not a difference", async () => {
  const fetchImpl = async () => new Response("nope", { status: 503 });
  await assert.rejects(fetchLive("https://example.test/quickstart.md", fetchImpl), /HTTP 503/);
});

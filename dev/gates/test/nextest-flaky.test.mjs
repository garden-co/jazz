import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { flakyPasses, reportFlakyPasses } from "../nextest-flaky.mjs";

// Written by cargo-nextest 0.9.143 for three tests under `retries = 2`: one
// that fails its first attempt only, one that fails every attempt, and one
// that passes.
const fixture = path.join(import.meta.dirname, "fixtures/nextest-flaky-junit.xml");

test("lists a test that passed on a retry, not one that failed every attempt", () => {
  assert.deepEqual(flakyPasses(fs.readFileSync(fixture, "utf8")), [
    { binary: "flk", test: "t::flaky_once", failedAttempts: 1 },
  ]);
});

test("writes the flaky passes to the job summary and as warnings", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-flaky-"));
  const summaryPath = path.join(dir, "summary.md");
  const lines = [];
  const flaky = reportFlakyPasses(fixture, { summaryPath, log: (line) => lines.push(line) });
  assert.equal(flaky.length, 1);
  assert.match(lines[0], /^::warning title=Flaky pass::flk t::flaky_once failed 1 time/);
  const summary = fs.readFileSync(summaryPath, "utf8");
  assert.match(summary, /### Flaky passes \(Rust\)/);
  assert.match(summary, /\| `flk` \| `t::flaky_once` \| 1 \|/);
  assert.doesNotMatch(summary, /always_fails/);
});

test("reports nothing without a JUnit report", () => {
  const lines = [];
  assert.deepEqual(reportFlakyPasses("/nonexistent/junit.xml", { log: (l) => lines.push(l) }), []);
  assert.deepEqual(lines, []);
});

test("nextest writes the JUnit report the runner reads, in both profiles", () => {
  const config = fs.readFileSync(
    path.join(import.meta.dirname, "../../../.config/nextest.toml"),
    "utf8",
  );
  for (const profile of ["jazz", "jazz-ci"])
    assert.match(config, new RegExp(`\\[profile\\.${profile}\\.junit\\]\\npath = "junit\\.xml"`));
});

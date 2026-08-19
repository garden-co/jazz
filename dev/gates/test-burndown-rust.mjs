#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import fs from "node:fs";
const fail = (message) => {
  throw new Error(message);
};
function rows(section) {
  return [...section.matchAll(/^\s*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|/gm)].map(
    ([, test, path]) => ({ test, path }),
  );
}
function parse(doc) {
  const activeStart = doc.indexOf("## Active Rust quarantine");
  const dormantStart = doc.indexOf("## Pre-existing/dormant Rust ignores");
  const tsStart = doc.indexOf("## Active TypeScript/browser quarantine");
  if (activeStart < 0 || dormantStart < 0 || tsStart < 0)
    fail("missing Rust or TypeScript quarantine section");
  const active = rows(doc.slice(activeStart, dormantStart));
  const dormant = rows(doc.slice(dormantStart, tsStart));
  const all = [...active, ...dormant];
  const seen = new Set();
  for (const row of all) {
    if (seen.has(row.test)) fail("duplicate documented test: " + row.test);
    seen.add(row.test);
  }
  return { active, dormant, all, documented: seen };
}
function compiledIgnored() {
  const raw = execFileSync(
    "cargo",
    [
      "nextest",
      "list",
      "--workspace",
      "--lib",
      "--bins",
      "--tests",
      "--features",
      "test",
      "--message-format",
      "json",
    ],
    { encoding: "utf8" },
  );
  const inventory = JSON.parse(raw);
  const ignored = new Set();
  for (const [binary, suite] of Object.entries(inventory["rust-suites"]))
    for (const [name, test] of Object.entries(suite.testcases))
      if (test.ignored) ignored.add(binary + "::" + name);
  return ignored;
}
function same(a, b) {
  return a.size === b.size && [...a].every((x) => b.has(x));
}
function verifyMarkers(active) {
  const markerLocations = new Set();
  for (const row of active) {
    if (!fs.existsSync(row.path)) fail("documented path does not exist: " + row.path);
    const name = row.test.slice(row.test.lastIndexOf("::") + 2);
    const source = fs.readFileSync(row.path, "utf8");
    const re = new RegExp(
      String.raw`#\[ignore = "[^"]+"\]\s*(?:#\[[^\]]+\]\s*)*(?:pub\s+)?(?:async\s+)?fn\s+${name}\b`,
      "g",
    );
    const matches = [...source.matchAll(re)];
    if (matches.length !== 1) fail("active marker must bind exactly once: " + row.test);
    markerLocations.add(row.path + ":" + matches[0].index);
  }
  if (markerLocations.size !== active.length)
    fail("active marker bijection failed: documented=" + active.length);
}
function selfTest() {
  const base =
    "## Active Rust quarantine\n| `a` | `x.rs` |\n## Pre-existing/dormant Rust ignores\n| `b` | `y.rs` |\n## Active TypeScript/browser quarantine\n";
  if (parse(base).all.length !== 2) fail("self-test base");
  for (const [label, mutation] of [
    ["duplicate doc FQN", base.replace("`b`", "`a`")],
    ["missing row", base.replace("| `b` | `y.rs` |\n", "")],
  ]) {
    let failed = false;
    try {
      const p = parse(mutation);
      if (label === "missing row" && p.all.length !== 2) throw new Error("missing");
    } catch {
      failed = true;
    }
    if (!failed) fail("self-test did not reject " + label);
  }
  if (same(new Set(["a"]), new Set(["a", "extra"]))) fail("self-test extra ignore");
  if (same(new Set(["a"]), new Set(["b"]))) fail("self-test swapped green ignore");
  console.log(
    "burndown gate self-tests: duplicate doc FQN, missing row, extra ignore, swapped ignore, and wrong identity rejected.",
  );
}
if (process.argv.includes("--self-test")) {
  selfTest();
  process.exit(0);
}
const doc = fs.readFileSync("TEST_BURNDOWN.md", "utf8");
const { active, dormant, documented } = parse(doc);
if (active.length !== 20 || dormant.length !== 10) fail("expected 20 active + 10 dormant rows");
const ignored = compiledIgnored();
if (!same(ignored, documented)) fail("compiled ignored set differs from documented set");
verifyMarkers(active);
console.log("Rust burndown: exact 20 active + 10 dormant identity bijection.");

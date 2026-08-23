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
function issueMappings(doc) {
  const start = doc.indexOf("## Offline issue map");
  const activeStart = doc.indexOf("## Active Rust quarantine");
  if (start < 0 || activeStart < 0 || start > activeStart)
    fail("missing offline issue map before Rust quarantine");
  const section = doc.slice(start, activeStart);
  const mappings = [
    ...section.matchAll(
      /^\|\s*\[#(\d+)\]\(https:\/\/github\.com\/garden-co\/jazz\/issues\/\1\)\s*\|\s*`([^`]+)`\s*\|$/gm,
    ),
  ].map(([, issue, prefix]) => ({ issue, prefix }));
  if (mappings.length === 0) fail("offline issue map has no mappings");
  const seen = new Set();
  for (const { issue, prefix } of mappings) {
    if (!/^\d+$/.test(issue)) fail("invalid issue number: " + issue);
    if (seen.has(prefix)) fail("duplicate issue-map prefix: " + prefix);
    seen.add(prefix);
  }
  return mappings;
}
function declaredCount(section, label) {
  const heading = section.match(/^## .+?\((\d+)(?:;[^)]*)?\)$/m);
  if (!heading) fail(`missing ${label} count in section heading`);
  return Number(heading[1]);
}
function parse(doc) {
  const activeStart = doc.indexOf("## Active Rust quarantine");
  const dormantStart = doc.indexOf("## Pre-existing/dormant Rust ignores");
  const tsStart = doc.indexOf("## Active TypeScript/browser quarantine");
  if (activeStart < 0 || dormantStart < 0 || tsStart < 0)
    fail("missing Rust or TypeScript quarantine section");
  const activeSection = doc.slice(activeStart, dormantStart);
  const dormantSection = doc.slice(dormantStart, tsStart);
  const active = rows(activeSection);
  const dormant = rows(dormantSection);
  const declaredActive = declaredCount(activeSection, "active Rust quarantine");
  const declaredDormant = declaredCount(dormantSection, "dormant Rust ignores");
  if (active.length !== declaredActive)
    fail(`active heading declares ${declaredActive} rows but table contains ${active.length}`);
  if (dormant.length !== declaredDormant)
    fail(`dormant heading declares ${declaredDormant} rows but table contains ${dormant.length}`);
  const all = [...active, ...dormant];
  const seen = new Set();
  for (const row of all) {
    if (seen.has(row.test)) fail("duplicate documented test: " + row.test);
    seen.add(row.test);
  }
  const mappings = issueMappings(doc);
  for (const row of all) {
    const matching = mappings.filter(({ prefix }) => row.test.startsWith(prefix));
    if (matching.length === 0) fail("ignored test has no issue mapping: " + row.test);
    const longest = Math.max(...matching.map(({ prefix }) => prefix.length));
    if (matching.filter(({ prefix }) => prefix.length === longest).length !== 1)
      fail("ignored test has ambiguous issue mapping: " + row.test);
  }
  return { active, dormant, all, documented: seen, mappings };
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
function difference(a, b) {
  return [...a].filter((value) => !b.has(value)).sort();
}
function verifyMarkers(active) {
  const markerLocations = new Set();
  for (const row of active) {
    if (!fs.existsSync(row.path)) fail("documented path does not exist: " + row.path);
    const name = row.test.slice(row.test.lastIndexOf("::") + 2);
    const source = fs.readFileSync(row.path, "utf8");
    const re = new RegExp(
      String.raw`#\[ignore = "(?:\\.|[^"\\])*"\]\s*(?:#\[[^\]]+\]\s*)*(?:pub\s+)?(?:async\s+)?fn\s+${name}\b`,
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
    "## Offline issue map\n| Issue | Test identity prefix |\n| --- | --- |\n| [#1](https://github.com/garden-co/jazz/issues/1) | `a` |\n| [#2](https://github.com/garden-co/jazz/issues/2) | `b` |\n## Active Rust quarantine (1)\n| `a` | `x.rs` |\n## Pre-existing/dormant Rust ignores (1; separately registered)\n| `b` | `y.rs` |\n## Active TypeScript/browser quarantine\n";
  if (parse(base).all.length !== 2) fail("self-test base");
  for (const [label, mutation] of [
    ["duplicate doc FQN", base.replace("`b`", "`a`")],
    ["missing row", base.replace("| `b` | `y.rs` |\n", "")],
    ["stale heading count", base.replace("quarantine (1)", "quarantine (2)")],
    ["unmapped ignored test", base.replace("| [#2](https://github.com/garden-co/jazz/issues/2) | `b` |\n", "")],
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
    "quarantine gate self-tests: duplicate doc FQN, missing row, stale heading count, unmapped issue, extra ignore, swapped ignore, and wrong identity rejected.",
  );
}
if (process.argv.includes("--self-test")) {
  selfTest();
  process.exit(0);
}
const doc = fs.readFileSync("TEST_BURNDOWN.md", "utf8");
const { active, dormant, documented } = parse(doc);
const ignored = compiledIgnored();
if (!same(ignored, documented))
  fail(
    `compiled ignored set differs from documented set\ncompiled only:\n${difference(ignored, documented).join("\n")}\ndocumented only:\n${difference(documented, ignored).join("\n")}`,
  );
verifyMarkers(active);
console.log(
  `Rust quarantine: exact ${active.length} active + ${dormant.length} dormant identity bijection with offline issue mappings.`,
);

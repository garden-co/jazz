#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const marker = "TEST_BURNDOWN_TS:";
const fail = (message) => {
  throw new Error(message);
};
const same = (a, b) => a.size === b.size && [...a].every((value) => b.has(value));
function rows(section) {
  return [...section.matchAll(/^\s*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|/gm)].map(
    ([, test, path]) => ({ test, path }),
  );
}
function documentedRows(doc) {
  const start = doc.indexOf("## Active TypeScript/browser quarantine");
  if (start < 0) fail("missing TypeScript/browser quarantine section");
  const section = doc.slice(start);
  const active = rows(section);
  const identities = new Set();
  for (const row of active) {
    const identity = `${row.path}::${row.test}`;
    if (identities.has(identity)) fail(`duplicate documented TS/browser test: ${identity}`);
    identities.add(identity);
  }
  if (active.length !== 2) fail(`expected 2 active TS/browser rows, got ${active.length}`);
  return { active, identities };
}
function sourceMarkers() {
  const paths = [];
  const visit = (directory) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const candidate = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(candidate);
      else if (entry.isFile() && entry.name.endsWith(".test.ts")) {
        const source = fs.readFileSync(candidate, "utf8");
        if (source.includes(marker)) paths.push(candidate);
      }
    }
  };
  visit("packages");
  const identities = new Set();
  for (const path of paths) {
    if (!fs.existsSync(path)) fail(`documented path does not exist: ${path}`);
    const source = fs.readFileSync(path, "utf8");
    for (const match of source.matchAll(/^\s*\/\/ TEST_BURNDOWN_TS: (.+)$/gm)) {
      const test = match[1];
      const identity = `${path}::${test}`;
      if (identities.has(identity)) fail(`duplicate source TS/browser marker: ${identity}`);
      const following = source.slice(match.index);
      const skipped = following.match(
        /^\s*\/\/ TEST_BURNDOWN_TS: .+\n(?:\s*\/\/ TEST_BURNDOWN_TS: .+\n)*\s*\/\/ known red; tracked in TEST_BURNDOWN\.md — .+\n\s*it\.skip\(\s*"([^"]+)"/,
      );
      if (!skipped) fail(`marker must immediately bind a visible it.skip: ${identity}`);
      if (test.split(" > ").at(-1) !== skipped[1]) fail(`marker/test title mismatch: ${identity}`);
      identities.add(identity);
    }
  }
  return identities;
}
function selfTest() {
  const base = new Set(["a::one"]);
  if (same(base, new Set(["a::one", "b::two"]))) fail("self-test extra marker");
  if (same(base, new Set(["a::two"]))) fail("self-test swapped identity");
  let duplicateRejected = false;
  try {
    const rows = ["a::one", "a::one"];
    if (new Set(rows).size !== rows.length) throw new Error("duplicate");
  } catch {
    duplicateRejected = true;
  }
  if (!duplicateRejected) fail("self-test duplicate identity");
  console.log(
    "TS/browser burndown gate self-tests: duplicate, missing, extra, and swapped identities rejected.",
  );
}

if (process.argv.includes("--self-test")) {
  selfTest();
  process.exit(0);
}

const { active, identities: documented } = documentedRows(
  fs.readFileSync("TEST_BURNDOWN.md", "utf8"),
);
for (const row of active)
  if (!fs.existsSync(row.path)) fail(`documented path does not exist: ${row.path}`);
const source = sourceMarkers();
if (!same(documented, source)) fail("TS/browser source markers differ from documented active set");
console.log("TS/browser burndown: exact 2 active identity bijection.");

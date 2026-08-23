#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "../..");
const specs = ["crates/jazz/SPEC", "crates/groove/SPEC"];
const issueLink = /https:\/\/github\.com\/garden-co\/jazz\/issues\/\d+/;

function fail(message) {
  throw new Error(`spec-open-questions: ${message}`);
}

function files(directory) {
  return fs
    .readdirSync(path.join(root, directory), { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
    .map((entry) => path.join(directory, entry.name));
}

function check(text, file) {
  const start = text.indexOf("## Open Questions");
  if (start < 0) return;
  const next = text.indexOf("\n## ", start + 1);
  const section = text.slice(start, next < 0 ? text.length : next);
  for (const [offset, line] of section.split("\n").entries()) {
    if (/^[-*] .*🔶/.test(line) && !issueLink.test(line))
      fail(`${file}:${offset + 1} open question lacks a GitHub issue link`);
  }
}

function selfTest() {
  check("## Open Questions\n\n- 🔶 [#1](https://github.com/garden-co/jazz/issues/1) — linked.\n", "good.md");
  let rejected = false;
  try {
    check("## Open Questions\n\n- 🔶 missing link.\n", "bad.md");
  } catch {
    rejected = true;
  }
  if (!rejected) fail("self-test did not reject an unlinked question");
  console.log("spec-open-questions: self-test passed");
}

if (process.argv.includes("--self-test")) selfTest();
else {
  for (const directory of specs)
    for (const file of files(directory)) check(fs.readFileSync(path.join(root, file), "utf8"), file);
  console.log("spec-open-questions: every unresolved question has an offline issue link");
}

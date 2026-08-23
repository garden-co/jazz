#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "../..");
const specs = ["crates/jazz/SPEC", "crates/groove/SPEC"];
const issueLink = /\[#(\d+)\]\(https:\/\/github\.com\/garden-co\/jazz\/issues\/(\d+)\)/g;

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
  for (const [offset, line] of text.split("\n").entries()) {
    if (!line.includes("🔶")) continue;
    const links = [...line.matchAll(issueLink)];
    if (links.length === 0)
      fail(`${file}:${offset + 1} unresolved question lacks a GitHub issue link`);
    for (const match of links)
      if (match[1] !== match[2])
        fail(
          `${file}:${offset + 1} issue label #${match[1]} does not match issue URL #${match[2]}`,
        );
  }
}

function selfTest() {
  check("🔶 [#1](https://github.com/garden-co/jazz/issues/1) — linked.\n", "good.md");
  for (const [name, text] of [
    ["unbulleted.md", "🔶 missing link.\n"],
    ["mismatched.md", "- 🔶 [#99999](https://github.com/garden-co/jazz/issues/1) — mismatched.\n"],
  ]) {
    let rejected = false;
    try {
      check(text, name);
    } catch {
      rejected = true;
    }
    if (!rejected) fail(`self-test did not reject ${name}`);
  }
  console.log("spec-open-questions: self-test passed");
}

if (process.argv.includes("--self-test")) selfTest();
else {
  for (const directory of specs)
    for (const file of files(directory))
      check(fs.readFileSync(path.join(root, file), "utf8"), file);
  console.log("spec-open-questions: every unresolved question has an offline issue link");
}

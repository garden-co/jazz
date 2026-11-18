#!/usr/bin/env node
// Every GitHub Action this repo runs must be pinned to a full commit SHA (or,
// for Docker actions, an image digest), so a moved tag or branch upstream
// can't change what runs in CI. Local actions (`./…`) are exempt.
//
// Scans every YAML file under a `.github/` directory and every `action.yml`,
// without a YAML dependency: `uses:` values are single-line scalars.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const usesLine = /^\s*(?:-\s+)?uses\s*:\s*(.*)$/;
const commitSha = /^[^@\s]+@[0-9a-f]{40}$/;
const imageDigest = /^docker:\/\/[^@\s]+@sha256:[0-9a-f]{64}$/;
const skipDirectories = new Set([".git", "node_modules", "target"]);

/** The `uses:` value of a line, without quotes or a trailing comment. */
function usesValue(line) {
  const match = usesLine.exec(line);
  if (!match) return undefined;
  let value = match[1].trim();
  const quote = value[0];
  if (quote === '"' || quote === "'") {
    const end = value.indexOf(quote, 1);
    return end > 0 ? value.slice(1, end) : value;
  }
  return value.replace(/\s+#.*$/, "").trim();
}

/** Whether a `uses:` value is local or pinned to an immutable reference. */
export function isPinned(value) {
  if (value.startsWith("./")) return true;
  if (value.startsWith("docker://")) return imageDigest.test(value);
  return commitSha.test(value);
}

/** `{ line, value }` for every `uses:` in `text` that isn't pinned. */
export function findUnpinned(text) {
  const unpinned = [];
  for (const [index, line] of text.split("\n").entries()) {
    const value = usesValue(line);
    if (value !== undefined && !isPinned(value)) unpinned.push({ line: index + 1, value });
  }
  return unpinned;
}

/** Workflow and action files under `root`, repository-relative. */
export function actionFiles(root, directory = "", underGithub = false) {
  const files = [];
  for (const entry of fs.readdirSync(path.join(root, directory), { withFileTypes: true })) {
    const relative = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      if (skipDirectories.has(entry.name)) continue;
      files.push(...actionFiles(root, relative, underGithub || entry.name === ".github"));
    } else if (
      entry.isFile() &&
      /\.ya?ml$/.test(entry.name) &&
      (underGithub || /^action\.ya?ml$/.test(entry.name))
    ) {
      files.push(relative);
    }
  }
  return files.sort();
}

function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const problems = actionFiles(root).flatMap((file) =>
    findUnpinned(fs.readFileSync(path.join(root, file), "utf8")).map(
      ({ line, value }) => `${file}:${line}: ${value}`,
    ),
  );
  if (problems.length > 0) {
    console.error(
      "actions-pinned: pin these to a full commit SHA (keep the tag as a `# vX` comment):",
    );
    for (const problem of problems) console.error(`  ${problem}`);
    process.exit(1);
  }
  console.log("actions-pinned: every action is pinned");
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();

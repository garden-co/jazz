#!/usr/bin/env node
/**
 * Refuse tracked paths Windows can't check out, which break every job that
 * clones the repository there (the windows-x86_64 release artifacts):
 * - the characters < > : " | ? * and control characters;
 * - a name ending in a dot or a space;
 * - reserved device names (CON, PRN, AUX, NUL, COM1–9, LPT1–9), with or
 *   without an extension;
 * - two paths that differ only in case (one overwrites the other).
 *
 * Usage: node dev/gates/windows-paths.mjs (checks `git ls-files`).
 */
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const INVALID_CHARS = /[<>:"|?*]/;
const RESERVED = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(\..*)?$/i;

/** Why `path` can't exist on Windows, or undefined. */
export function windowsProblem(path) {
  for (const name of path.split("/")) {
    const bad = name.match(INVALID_CHARS)?.[0] ?? [...name].find((c) => c.charCodeAt(0) < 32);
    if (bad) return `\`${JSON.stringify(bad).slice(1, -1)}\` in \`${name}\``;
    if (/[. ]$/.test(name)) return `\`${name}\` ends in a dot or a space`;
    if (RESERVED.test(name)) return `\`${name}\` is a reserved device name`;
  }
  return undefined;
}

/** Every problem among `paths`, as `path: why` lines. */
export function windowsPathProblems(paths) {
  const problems = [];
  const folded = new Map();
  for (const path of paths) {
    const why = windowsProblem(path);
    if (why) problems.push(`${path}: ${why}`);
    const key = path.toLowerCase();
    const other = folded.get(key);
    if (other !== undefined) problems.push(`${path}: differs from ${other} only in case`);
    else folded.set(key, path);
  }
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const paths = execFileSync("git", ["ls-files", "-z"], { encoding: "utf8", maxBuffer: 1 << 28 })
    .split("\0")
    .filter(Boolean);
  const problems = windowsPathProblems(paths);
  if (problems.length > 0) {
    console.error(
      `windows-paths: ${problems.length} tracked path${problems.length === 1 ? "" : "s"} Windows can't check out:\n` +
        problems.map((p) => `  ${p}`).join("\n"),
    );
    process.exit(1);
  }
  console.log(`windows-paths: ${paths.length} tracked paths are valid on Windows`);
}

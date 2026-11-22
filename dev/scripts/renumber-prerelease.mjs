#!/usr/bin/env node
// Starts a new prerelease channel at .0. `changeset version` numbers a
// prerelease from the package's current number whatever its tag, so the first
// version after `changeset pre enter beta` on 2.0.0-alpha.70 comes out as
// 2.0.0-beta.71. `pnpm release:version` runs this right after it: every
// version that changesets moved onto the pre.json tag from another tag (or
// from no prerelease) is renumbered to <tag>.0, in every file `changeset
// version` changed (package.json versions and dependency ranges, CHANGELOG
// headings). Later versions on the same tag are left alone (beta.0 -> beta.1).
//
//   node dev/scripts/renumber-prerelease.mjs [--root <repo>] [--base <git rev, default HEAD>]

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const prereleaseTag = (version) => /^\d+\.\d+\.\d+-([0-9A-Za-z-]+)\.\d+$/.exec(version)?.[1];

// before/after: Map name -> version. Returns Map oldNew -> renumbered.
export function renumbering(tag, before, after) {
  const out = new Map();
  for (const [name, next] of after) {
    const prev = before.get(name);
    if (prev === undefined || prev === next) continue;
    const m = new RegExp(
      `^(\\d+\\.\\d+\\.\\d+)-${tag.replace(/[^0-9A-Za-z-]/g, "")}\\.(\\d+)$`,
    ).exec(next);
    if (!m || m[2] === "0") continue;
    if (prereleaseTag(prev) === tag) continue;
    out.set(next, `${m[1]}-${tag}.0`);
  }
  return out;
}

export function rewrite(text, renames) {
  let result = text;
  for (const [from, to] of renames) {
    const escaped = from.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    // Not inside a longer version (2.0.0-beta.71 in 2.0.0-beta.710 or -beta.71.1).
    result = result.replace(new RegExp(`${escaped}(?![0-9]|\\.[0-9]|-[0-9A-Za-z])`, "g"), to);
  }
  return result;
}

function main() {
  const argv = process.argv;
  const flag = (name, fallback) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : fallback);
  const root = flag("--root", process.cwd());
  const base = flag("--base", "HEAD");
  const git = (...args) => execFileSync("git", ["-C", root, ...args], { encoding: "utf8" });
  const prePath = join(root, ".changeset/pre.json");
  if (!existsSync(prePath)) return console.log("Not in prerelease mode; nothing to renumber.");
  const pre = JSON.parse(readFileSync(prePath, "utf8"));
  if (pre.mode !== "pre") return console.log(`pre.json mode is ${pre.mode}; nothing to renumber.`);

  const changed = git("diff", "--name-only", base)
    .split("\n")
    .filter(Boolean)
    .filter((file) => existsSync(join(root, file)));
  const before = new Map();
  const after = new Map();
  for (const file of changed.filter((f) => f.endsWith("package.json"))) {
    let old;
    try {
      old = JSON.parse(git("show", `${base}:${file}`));
    } catch {
      continue;
    }
    const now = JSON.parse(readFileSync(join(root, file), "utf8"));
    if (!now.name) continue;
    before.set(now.name, old.version);
    after.set(now.name, now.version);
  }
  const renames = renumbering(pre.tag, before, after);
  if (renames.size === 0)
    return console.log(`No version enters ${pre.tag} here; nothing to renumber.`);
  for (const file of changed.filter((f) => /(package\.json|CHANGELOG\.md)$/.test(f))) {
    const path = join(root, file);
    const text = readFileSync(path, "utf8");
    const next = rewrite(text, renames);
    if (next !== text) writeFileSync(path, next);
  }
  for (const [from, to] of renames) console.log(`${from} -> ${to} (first ${pre.tag})`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();

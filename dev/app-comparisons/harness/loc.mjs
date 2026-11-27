#!/usr/bin/env node
// Usage: node harness/loc.mjs [<hero>/<stack> ...]   (default: every app with a loc.json)
// loc.json: { "<category>": ["glob", "!not-this", ...], ..., "exclude": ["glob", ...] }
// A "!glob" inside a category drops files from that category only.
// Counts non-blank, non-comment lines (//, /* */, #, -- comments; JSX {/* */}).
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import fg from "fast-glob";
import { allTargets, resolveTarget } from "./apps.mjs";

const HEADLINE = ["schema", "permissions", "server", "data"];

export function countLines(text, ext) {
  let n = 0;
  let inBlock = false;
  const hash = /\.(py|sh|toml|ya?ml)$/.test(ext);
  const sql = /\.sql$/.test(ext);
  for (let line of text.split("\n")) {
    line = line.trim();
    if (inBlock) {
      const end = line.indexOf("*/");
      if (end < 0) continue;
      inBlock = false;
      line = line.slice(end + 2).trim();
    }
    if (line.startsWith("{/*") && line.endsWith("*/}")) continue;
    if (line.startsWith("/*")) {
      const end = line.indexOf("*/");
      if (end < 0) {
        inBlock = true;
        continue;
      }
      line = line.slice(end + 2).trim();
    }
    if (!line) continue;
    if (line.startsWith("//")) continue;
    if (hash && line.startsWith("#")) continue;
    if (sql && line.startsWith("--")) continue;
    n++;
  }
  return n;
}

/** LOC of a target ("stage-plan/zero"); its loc.json globs are relative to the app. */
export function locFor(target) {
  const { appDir, hooksDir } = resolveTarget(target);
  const spec = JSON.parse(readFileSync(join(hooksDir, "loc.json"), "utf8"));
  const ignore = ["**/node_modules/**", "**/dist/**", "**/.next/**", "**/_generated/**", ...(spec.exclude ?? [])];
  const out = {};
  const files = {};
  for (const [category, globs] of Object.entries(spec)) {
    if (category === "exclude" || category.startsWith("_")) continue;
    let lines = 0;
    const matched = fg.sync(globs, { cwd: appDir, ignore, dot: false });
    for (const f of matched) lines += countLines(readFileSync(join(appDir, f), "utf8"), f);
    out[category] = lines;
    files[category] = matched.length;
  }
  out.data_layer = HEADLINE.reduce((sum, c) => sum + (out[c] ?? 0), 0);
  out.total = Object.entries(out).filter(([c]) => c !== "data_layer").reduce((sum, [, n]) => sum + n, 0);
  return { loc: out, files };
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
const targets = !isMain ? [] : process.argv.slice(2).length ? process.argv.slice(2) : allTargets();
for (const t of targets) console.log(t.padEnd(28), JSON.stringify(locFor(t).loc));

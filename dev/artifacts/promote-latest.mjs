#!/usr/bin/env node
// Points npm's `latest` dist-tag at the v2 alpha that "Publish jazz-tools
// alpha" just published, for every v2 package. Since alpha.60, a plain
// `npm install jazz-tools` gets v2; Classic stays reachable as `jazz-tools@0`.
//
// Dependencies are promoted before the packages that need them, and jazz-tools
// and create-jazz go last. `latest` never moves back to an older 2.0.0-alpha
// (say, when an old version is re-run to recover a publish).
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const alpha = /^2\.0\.0-alpha\.([0-9]+)$/;

export function v2Packages(base = root) {
  const napiPlatforms = readdirSync(join(base, "crates/jazz-napi/npm"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => readJson(join(base, "crates/jazz-napi/npm", entry.name, "package.json")).name)
    .sort();
  if (napiPlatforms.length === 0) throw new Error("no jazz-napi platform packages found");
  return [
    "jazz-wasm",
    ...napiPlatforms,
    "jazz-napi",
    "jazz-rn-android",
    "jazz-rn-ios",
    "jazz-rn",
    "jazz-tools",
    "create-jazz",
  ];
}

// `current` is the package's `latest` today (undefined if it has none).
export function shouldPromote(current, version) {
  const target = alpha.exec(version);
  if (!target)
    throw new Error(`only a 2.0.0-alpha.N becomes latest, got ${JSON.stringify(version)}`);
  if (current === version) return false;
  const existing = current && alpha.exec(current);
  if (existing && Number(existing[1]) > Number(target[1])) return false;
  return true;
}

function npm(args) {
  return execFileSync("npm", args, { encoding: "utf8" }).trim();
}

export function promote(version, base = root, run = npm) {
  for (const name of v2Packages(base)) {
    run(["view", `${name}@${version}`, "version"]); // throws if it was never published
    const tags = JSON.parse(run(["view", name, "dist-tags", "--json"]) || "{}");
    if (!shouldPromote(tags.latest, version)) {
      console.log(`${name}: latest stays ${tags.latest}`);
      continue;
    }
    run(["dist-tag", "add", `${name}@${version}`, "latest"]);
    console.log(`${name}: latest ${tags.latest ?? "(none)"} -> ${version}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const version = readJson(join(root, "packages/jazz-tools/package.json")).version;
  promote(version);
}

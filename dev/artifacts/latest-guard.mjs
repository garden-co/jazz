#!/usr/bin/env node
// Since alpha.61, "Publish jazz-tools alpha" publishes every v2 package with
// npm trusted publishing (OIDC) straight to the `latest` dist-tag, so a plain
// `npm install jazz-tools` gets v2 (Classic stays reachable as `jazz-tools@0`).
// npm's OIDC credentials can publish but cannot change dist-tags, so `latest`
// is never moved after the fact. This guard runs before the first upload and
// refuses a publish that would move `latest` back to an older 2.0.0-alpha (say,
// a re-run of an old version to recover a publish).
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const alpha = /^2\.0\.0-alpha\.([0-9]+)$/;

// The packages whose `latest` jazz-tools users see first. The napi platform
// and RN packages are published in lock-step with these.
export const GUARDED_PACKAGES = ["jazz-wasm", "jazz-napi", "jazz-rn", "jazz-tools", "create-jazz"];

// `current` is the package's `latest` today (undefined if it has none).
export function wouldMoveLatestBack(current, version) {
  const target = alpha.exec(version);
  if (!target)
    throw new Error(`only a 2.0.0-alpha.N is published as latest, got ${JSON.stringify(version)}`);
  const existing = current && alpha.exec(current);
  return Boolean(existing && Number(existing[1]) > Number(target[1]));
}

function npm(args) {
  return execFileSync("npm", args, { encoding: "utf8" }).trim();
}

export function checkLatest(version, run = npm) {
  for (const name of GUARDED_PACKAGES) {
    const tags = JSON.parse(run(["view", name, "dist-tags", "--json"]) || "{}");
    if (wouldMoveLatestBack(tags.latest, version)) {
      throw new Error(
        `${name}@${tags.latest} is already latest; publishing ${version} would move latest back`,
      );
    }
    console.log(`${name}: latest ${tags.latest ?? "(none)"} -> ${version} is allowed`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const version = JSON.parse(
    readFileSync(join(root, "packages/jazz-tools/package.json"), "utf8"),
  ).version;
  checkLatest(version);
}

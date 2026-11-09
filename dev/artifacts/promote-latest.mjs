#!/usr/bin/env node
// Points npm's `latest` dist-tag at the v2 alpha that "Publish jazz-tools
// alpha" just published, for every v2 package. Since alpha.60, a plain
// `npm install jazz-tools` gets v2; Classic stays reachable as `jazz-tools@0`.
//
// Dependencies are promoted before the packages that need them, and jazz-tools
// and create-jazz go last. `latest` never moves back to an older 2.0.0-alpha
// (say, when an old version is re-run to recover a publish).
//
// npm can take a long while to serve a version it has accepted (jazz-rn-android,
// ~100 MB, took 85 minutes on alpha.60), so each package waits until its version
// is visible. The React Native packages are their own group (`rn`), promoted by
// a separate job, so a slow RN upload never holds up the rest of the release.
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const alpha = /^2\.0\.0-alpha\.([0-9]+)$/;

const RN_PACKAGES = ["jazz-rn-android", "jazz-rn-ios", "jazz-rn"];

// `group` is "core" (everything but React Native), "rn", or "all".
export function v2Packages(base = root, group = "all") {
  // The jazz-napi platform packages exist only after the publish job stages
  // them, so the rn group (run by its own job) doesn't read them.
  if (group === "rn") return RN_PACKAGES;
  const napiPlatforms = readdirSync(join(base, "crates/jazz-napi/npm"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => readJson(join(base, "crates/jazz-napi/npm", entry.name, "package.json")).name)
    .sort();
  if (napiPlatforms.length === 0) throw new Error("no jazz-napi platform packages found");
  const all = [
    "jazz-wasm",
    ...napiPlatforms,
    "jazz-napi",
    ...RN_PACKAGES,
    "jazz-tools",
    "create-jazz",
  ];
  if (group === "all") return all;
  if (group === "core") return all.filter((name) => !RN_PACKAGES.includes(name));
  throw new Error(`unknown package group ${JSON.stringify(group)} (core, rn or all)`);
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

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

// Returns once npm serves `name@version`, retrying every `intervalMs` until
// `timeoutMs` has passed, then rethrows npm's last error.
export function waitUntilVisible(
  name,
  version,
  {
    run = npm,
    sleep = sleepSync,
    timeoutMs = 3 * 60 * 60_000,
    intervalMs = 60_000,
    now = Date.now,
  } = {},
) {
  const deadline = now() + timeoutMs;
  for (;;) {
    try {
      run(["view", `${name}@${version}`, "version"]);
      return;
    } catch (error) {
      if (now() >= deadline) throw error;
      console.log(
        `${name}@${version} is not visible on npm yet; retrying in ${intervalMs / 1000}s`,
      );
      sleep(intervalMs);
    }
  }
}

export function promote(version, base = root, run = npm, { group = "all", wait = {} } = {}) {
  for (const name of v2Packages(base, group)) {
    waitUntilVisible(name, version, { run, ...wait });
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
  promote(version, root, npm, { group: process.argv[2] ?? "all" });
}

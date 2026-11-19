#!/usr/bin/env node
// Publishes the prebuilt npm tarballs of a release (the `release-npm`
// artifact: every *.tgz plus NPM-SHA256SUMS) with `npm publish <tarball>`.
// Used by .github/workflows/release-publish.yml; nothing is packed here.
//
//   node dev/release-publish/npm-publish.mjs <dir> <version> [--dry-run | --check]
//
// Before the first upload it checks every tarball against NPM-SHA256SUMS,
// that each one's package.json is <version> with no `workspace:` ranges, and
// that the set is complete. It then publishes in levels of the packages'
// dependencies on each other (platform packages and jazz-wasm first, then
// jazz-napi and jazz-rn, then jazz-tools, then create-jazz), each level in
// parallel, so `latest` never names a package whose dependencies aren't
// there yet. A version already on npm is skipped, so a failed run re-runs.
// --dry-run runs `npm publish --dry-run` instead and publishes nothing;
// --check stops after the checks.
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { verifyRnPackSize } from "../artifacts/verify-rn-pack-size.mjs";

export const REQUIRED = [
  "jazz-wasm",
  "jazz-napi",
  "jazz-rn",
  "jazz-rn-android",
  "jazz-rn-ios",
  "jazz-tools",
  "create-jazz",
];
export const NAPI_PLATFORMS = 5;
export const LAST = ["create-jazz"];

/** Levels of `packages` ({ name, deps }) such that each depends only on earlier levels. */
export function publishLevels(packages) {
  const names = new Set(packages.map((p) => p.name));
  const level = new Map();
  const visiting = new Set();
  const byName = new Map(packages.map((p) => [p.name, p]));
  const place = (name) => {
    if (level.has(name)) return level.get(name);
    if (visiting.has(name)) throw new Error(`dependency cycle through ${name}`);
    visiting.add(name);
    const deps = byName.get(name).deps.filter((d) => names.has(d) && d !== name);
    const value = deps.length === 0 ? 0 : Math.max(...deps.map(place)) + 1;
    visiting.delete(name);
    level.set(name, value);
    return value;
  };
  // create-jazz scaffolds apps on the jazz-tools it names without depending
  // on it: it goes last, so it never offers a version that isn't out yet.
  const last = packages.filter((p) => LAST.includes(p.name));
  const levels = [];
  for (const p of packages) if (!LAST.includes(p.name)) (levels[place(p.name)] ??= []).push(p);
  if (last.length > 0) levels.push(last);
  return levels.map((l) => l.sort((a, b) => a.name.localeCompare(b.name)));
}

function readPackage(tarball) {
  const json = execFileSync("tar", ["-xOzf", tarball, "package/package.json"], {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  return JSON.parse(json);
}

export function checkManifest(pkg, version) {
  if (pkg.version !== version) throw new Error(`${pkg.name} is ${pkg.version}, not ${version}`);
  for (const field of ["dependencies", "optionalDependencies", "peerDependencies"])
    for (const [dep, range] of Object.entries(pkg[field] ?? {}))
      if (typeof range === "string" && range.startsWith("workspace:"))
        throw new Error(`${pkg.name}'s ${field}.${dep} is ${range}`);
}

function run(cmd, args) {
  return new Promise((resolvePromise) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (out += d));
    child.on("close", (status) => resolvePromise({ status, out }));
  });
}

async function publishOne(pkg, dryRun) {
  const started = Date.now();
  const spec = `${pkg.name}@${pkg.version}`;
  const existing = await run("npm", ["view", spec, "version"]);
  if (existing.status === 0 && existing.out.trim() === pkg.version)
    return { spec, result: "already on npm", seconds: 0 };
  const args = ["publish", pkg.tarball, "--tag", "latest", "--access", "public"];
  if (dryRun) args.push("--dry-run");
  const result = await run("npm", args);
  const seconds = Math.round((Date.now() - started) / 1000);
  if (result.status === 0) return { spec, result: dryRun ? "dry-run ok" : "published", seconds };
  // npm accepts a large upload before it serves it: publishing again answers
  // E409 "Cannot publish over previously staged version". It is ours already.
  if (/E409/.test(result.out) && /previously staged version/.test(result.out))
    return { spec, result: "already staged on npm", seconds };
  process.stderr.write(result.out);
  throw new Error(`npm ${args.join(" ")} failed (exit ${result.status})`);
}

async function main() {
  const [dirArg, version, ...flags] = process.argv.slice(2);
  if (!dirArg || !version)
    throw new Error("usage: npm-publish.mjs <dir> <version> [--dry-run | --check]");
  const dryRun = flags.includes("--dry-run");
  const dir = resolve(dirArg);

  // NPM-SHA256SUMS covers exactly the tarballs.
  const sums = new Map(
    readFileSync(join(dir, "NPM-SHA256SUMS"), "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        const [hash, file] = line.trim().split(/\s+\*?/);
        return [file, hash];
      }),
  );
  const tarballs = readdirSync(dir).filter((f) => f.endsWith(".tgz"));
  for (const file of tarballs) if (!sums.has(file)) throw new Error(`NPM-SHA256SUMS lacks ${file}`);
  for (const [file, hash] of sums) {
    const actual = createHash("sha256")
      .update(readFileSync(join(dir, file)))
      .digest("hex");
    if (actual !== hash) throw new Error(`${file}: sha256 ${actual}, NPM-SHA256SUMS says ${hash}`);
  }

  const packages = tarballs.map((file) => {
    const tarball = join(dir, file);
    const manifest = readPackage(tarball);
    checkManifest(manifest, version);
    if (manifest.name.startsWith("jazz-rn")) verifyRnPackSize(statSync(tarball).size);
    const deps = Object.keys({
      ...manifest.dependencies,
      ...manifest.optionalDependencies,
      ...manifest.peerDependencies,
    });
    return { name: manifest.name, version: manifest.version, tarball, deps };
  });
  const names = new Set(packages.map((p) => p.name));
  for (const name of REQUIRED) if (!names.has(name)) throw new Error(`no tarball for ${name}`);
  const napiPlatforms = packages.filter((p) => /jazz-napi-/.test(p.name)).length;
  if (napiPlatforms !== NAPI_PLATFORMS)
    throw new Error(`${napiPlatforms} jazz-napi platform packages, expected ${NAPI_PLATFORMS}`);

  const levels = publishLevels(packages);
  if (flags.includes("--check")) {
    for (const [index, level] of levels.entries())
      console.log(`level ${index}: ${level.map((p) => p.name).join(", ")}`);
    console.log(`${packages.length} tarballs at ${version} check out`);
    return;
  }
  const rows = [];
  const started = Date.now();
  for (const [index, level] of levels.entries()) {
    console.log(`==> level ${index}: ${level.map((p) => p.name).join(", ")}`);
    const results = await Promise.all(level.map((p) => publishOne(p, dryRun)));
    for (const r of results) {
      console.log(`    ${r.spec}: ${r.result} (${r.seconds}s)`);
      rows.push({ level: index, ...r });
    }
  }
  const total = Math.round((Date.now() - started) / 1000);
  console.log(`npm: ${rows.length} packages in ${total}s${dryRun ? " (dry run)" : ""}`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    const lines = [
      `### npm${dryRun ? " (dry run)" : ""}: ${total}s`,
      "",
      "| level | package | result | s |",
      "|---|---|---|---|",
      ...rows.map((r) => `| ${r.level} | ${r.spec} | ${r.result} | ${r.seconds} |`),
      "",
    ];
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${lines.join("\n")}\n`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

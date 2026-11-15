#!/usr/bin/env node
// Checks that Jazz's npm packages, its published Rust crates and the Go
// module are versioned in lockstep with jazz-tools on npm. Run it on main
// and on the Version Packages PR head:
//
//   node dev/scripts/check-lockstep-versions.mjs [--root <repo>]
//
// Exits 1 with a list of mismatches.

import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import {
  INDEPENDENTLY_VERSIONED,
  cargoMetadata,
  lockstepCrates,
  publishPlan,
} from "./rust-crates.mjs";

export const GO_VERSION_FILE = "packages/jazz-go/version.go";
const GO_VERSION_PATTERN = /\bVersion\s*=\s*"([^"]+)"/;

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

/** Versions of the workspace's npm packages, by name. */
function npmVersions(root) {
  const versions = new Map();
  const dirs = ["packages", "crates", "crates/jazz-rn/npm"];
  for (const dir of dirs) {
    if (!existsSync(join(root, dir))) continue;
    for (const entry of readdirSync(join(root, dir), { withFileTypes: true })) {
      const manifest = join(root, dir, entry.name, "package.json");
      if (entry.isDirectory() && existsSync(manifest)) {
        const pkg = readJson(manifest);
        if (pkg.name && pkg.version) versions.set(pkg.name, pkg.version);
      }
    }
  }
  const rn = join(root, "crates/jazz-rn/package.json");
  if (existsSync(rn)) {
    const pkg = readJson(rn);
    versions.set(pkg.name, pkg.version);
  }
  return versions;
}

export function checkLockstep({ root, metadata }) {
  const problems = [];
  const npm = npmVersions(root);
  const expected = npm.get("jazz-tools");
  if (!expected) return { expected, problems: ["packages/jazz-tools/package.json not found"] };

  // npm: the Changesets fixed group moves together.
  const config = readJson(join(root, ".changeset/config.json"));
  for (const group of config.fixed ?? [])
    if (group.includes("jazz-tools"))
      for (const name of group) {
        const version = npm.get(name);
        if (version === undefined) problems.push(`npm package ${name} (fixed group) not found`);
        else if (version !== expected)
          problems.push(`npm ${name} is ${version}, expected ${expected}`);
      }

  // crates.io: every published Jazz crate, and the exact requirements
  // between them.
  const plan = publishPlan(metadata);
  problems.push(...plan.problems);
  const lockstep = new Set(lockstepCrates(plan));
  for (const name of plan.order) {
    const pkg = plan.byName.get(name);
    if (lockstep.has(name) && pkg.version !== expected)
      problems.push(`crate ${name} is ${pkg.version}, expected ${expected}`);
    for (const dep of pkg.dependencies) {
      if (!dep.path || !plan.byName.has(dep.name)) continue;
      if (dep.kind === "dev" && (!dep.req || dep.req === "*")) continue;
      const want = lockstep.has(dep.name)
        ? `=${expected}`
        : `=${plan.byName.get(dep.name).version}`;
      if (dep.req !== want)
        problems.push(`crate ${name} requires ${dep.name} ${dep.req}, expected ${want}`);
    }
  }

  // Go: the module's version constant, once the module has one.
  const goFile = join(root, GO_VERSION_FILE);
  let go = "absent";
  if (existsSync(goFile)) {
    const match = readFileSync(goFile, "utf8").match(GO_VERSION_PATTERN);
    if (!match) problems.push(`${GO_VERSION_FILE} has no Version = "..." constant`);
    else if (match[1] !== expected)
      problems.push(`Go ${GO_VERSION_FILE} Version is ${match[1]}, expected ${expected}`);
    else go = match[1];
  }

  return { expected, crates: plan.order, go, problems };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const rootFlag = process.argv.indexOf("--root");
  const root = rootFlag === -1 ? process.cwd() : process.argv[rootFlag + 1];
  const { expected, crates, go, problems } = checkLockstep({
    root,
    metadata: cargoMetadata({ cwd: root }),
  });
  if (problems.length > 0) {
    console.error(`Lockstep version check failed (jazz-tools on npm: ${expected}):`);
    for (const problem of problems) console.error(`  - ${problem}`);
    process.exit(1);
  }
  const independent = Object.keys(INDEPENDENTLY_VERSIONED).filter((name) => crates.includes(name));
  console.log(
    `Lockstep at ${expected}: npm fixed group, ${crates.length - independent.length} crates`,
  );
  console.log(`  crates.io order: ${crates.join(" -> ")}`);
  if (independent.length > 0) console.log(`  versioned apart: ${independent.join(", ")}`);
  console.log(`  Go: ${go === "absent" ? `${GO_VERSION_FILE} absent, skipped` : go}`);
}

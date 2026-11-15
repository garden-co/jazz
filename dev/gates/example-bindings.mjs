#!/usr/bin/env node
/**
 * Every example app's committed bindings are what `jazz-tools generate`
 * writes from its jazz.app.json: `generate --check` in each directory that
 * commits a jazz.app.json (apps, and the docs' recipe apps beside them).
 * A stale or missing jazz.generated.ts otherwise only shows when a backend
 * starts. Runs against the jazz-tools CLI the TypeScript partition built.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const cli = join(root, "packages", "jazz-tools", "dist", "cli.js");

/** Directories under examples/ and dev/stress-tests/ that commit a jazz.app.json. */
export function exampleAppDirs(cwd = root) {
  const listed = spawnSync(
    "git",
    ["ls-files", "--", "examples/**/jazz.app.json", "dev/stress-tests/**/jazz.app.json"],
    { cwd, encoding: "utf8" },
  );
  if (listed.status !== 0) throw new Error(`git ls-files failed: ${listed.stderr}`);
  return listed.stdout
    .split("\n")
    .filter(Boolean)
    .map((file) => dirname(file))
    .sort();
}

/**
 * A Rust crate beside its app files (a Cargo.toml with `[package]`):
 * `generate --check` doesn't check a Rust-only crate's bindings yet, so these
 * are listed as skipped rather than failing.
 */
export function isRustCrate(dir) {
  const manifest = join(dir, "Cargo.toml");
  return existsSync(manifest) && /^\s*\[package\]/m.test(readFileSync(manifest, "utf8"));
}

/** `generate --check` in each app; the failures, as `dir: last line of output`. */
export function checkBindings(dirs, { cwd = root, run = defaultRun } = {}) {
  const failures = [];
  const skipped = [];
  for (const dir of dirs) {
    const absolute = resolve(cwd, dir);
    if (isRustCrate(absolute)) {
      skipped.push(dir);
      continue;
    }
    const result = run(absolute);
    if (result.status !== 0) {
      const output = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim().split("\n");
      failures.push(`${dir}: ${output.at(-1) ?? `exit ${result.status}`}`);
    }
  }
  return { failures, skipped, checked: dirs.length - skipped.length };
}

function defaultRun(dir) {
  return spawnSync(process.execPath, [cli, "generate", "--check", "--schema-dir", dir], {
    cwd: dir,
    encoding: "utf8",
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (!existsSync(cli)) {
    console.error(`example bindings: ${cli} is missing; build jazz-tools first`);
    process.exit(1);
  }
  const { failures, skipped, checked } = checkBindings(exampleAppDirs());
  for (const dir of skipped) console.log(`skipped (Rust crate): ${dir}`);
  if (failures.length > 0) {
    console.error(
      `example bindings: ${failures.length} of ${checked} apps fail \`jazz-tools generate --check\`:\n` +
        failures.map((f) => `  ${f}`).join("\n") +
        "\nRun `npx jazz-tools generate` in each and commit what it writes.",
    );
    process.exit(1);
  }
  console.log(`example bindings: ${checked} apps pass \`jazz-tools generate --check\`.`);
}

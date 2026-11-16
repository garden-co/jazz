#!/usr/bin/env node
/**
 * Every example app's committed jazz.app.json, jazz.permissions.json and
 * bindings are what `jazz-tools generate` writes from its TS (schema.ts):
 * `generate --check` in each directory that commits a jazz.app.json (apps,
 * the docs' recipe apps beside them, Rust crates (src/jazz_app.rs), Go
 * modules, Swift packages (JazzApp.swift) and Gradle projects (JazzApp.kt),
 * whose language the directory names). StagePlan's go-cli, rust-cli,
 * swift-cli and kotlin-cli re-export react-localfirst's TS, so this also keeps
 * their JSON StagePlan's (and examples/docs/todo-client-kotlin's the docs
 * todo app's). A stale file otherwise only shows when a backend
 * starts. Runs against the jazz-tools CLI the TypeScript partition built.
 */
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
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

/** `generate --check` in each app; the failures, as `dir: last line of output`. */
export function checkBindings(dirs, { cwd = root, run = defaultRun } = {}) {
  const failures = [];
  for (const dir of dirs) {
    const absolute = resolve(cwd, dir);
    const result = run(absolute);
    if (result.status !== 0) {
      const output = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim().split("\n");
      failures.push(`${dir}: ${output.at(-1) ?? `exit ${result.status}`}`);
    }
  }
  return { failures, checked: dirs.length };
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
  const { failures, checked } = checkBindings(exampleAppDirs());
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

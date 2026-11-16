#!/usr/bin/env node
/**
 * Every example app's committed bindings are what `jazz-tools generate`
 * writes from its jazz.app.json: `generate --check` in each directory that
 * commits a jazz.app.json (apps, the docs' recipe apps beside them, and
 * Rust crates, whose src/jazz_app.rs it checks too).
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
  return (
    listed.stdout
      .split("\n")
      .filter(Boolean)
      .map((file) => dirname(file))
      // Go apps: Go is switched off in `jazz-tools generate` until its
      // bindings reach parity; jazz-codegen's golden tests check that their
      // committed jazzapp/jazz_app.go is current.
      .filter((dir) => !existsSync(join(resolve(cwd, dir), "go.mod")))
      .filter((dir) => !isSwiftPackage(resolve(cwd, dir)))
      .sort()
  );
}

/**
 * A Swift package of the Jazz Swift runtime (a Package.swift using its
 * `Jazz` product): its bindings are JazzApp.swift, which jazz-codegen's golden
 * test and dev/gates/jazz-swift.sh check while the npm CLI has Swift off.
 */
function isSwiftPackage(dir) {
  const manifest = join(dir, "Package.swift");
  return existsSync(manifest) && usesJazzSwift(readFileSync(manifest, "utf8"));
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

/** As jazz-codegen's `swift::is_app_manifest`: the `Jazz` product, or the jazz-swift package. */
function usesJazzSwift(manifest) {
  return /\.product\(\s*name:\s*"Jazz"/.test(manifest) || /["/]jazz-swift(\.git)?"/.test(manifest);
}

import * as path from "node:path";
import { fileURLToPath } from "node:url";
import pc from "picocolors";

import { KNOWN_STARTERS, type StarterName } from "./starters.js";
import { runStarter, type RunStarterResult } from "./run-starter.js";

function findRepoRoot(): string {
  // src/cli.ts → ../ is src/, ../../ is the package dir, ../../../ is packages/,
  // ../../../../ is the repo root.
  const here = path.dirname(fileURLToPath(import.meta.url));
  return path.resolve(here, "../../..");
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function printResult(r: RunStarterResult): void {
  const total = r.durations.reduce((acc, p) => acc + p.durationMs, 0);
  const phaseLine = r.durations.map((p) => `${p.name}=${formatDuration(p.durationMs)}`).join(" ");
  const tag = r.success ? pc.green("✔") : pc.red("✘");
  console.log(`${tag} ${r.starter} ${pc.dim(`(${formatDuration(total)})`)}`);
  console.log(`  ${pc.dim(phaseLine)}`);
  if (!r.success && r.errorMessage) {
    console.log(pc.red(r.errorMessage));
  }
}

function usage(): never {
  console.error(
    "Usage: tsx src/cli.ts <starter> [<starter>...] [--verbose] [--skip-e2e] [--keep] [--tarball-dir <dir>]",
  );
  console.error(
    "       tsx src/cli.ts --all [--verbose] [--skip-e2e] [--keep] [--tarball-dir <dir>]",
  );
  console.error(`\nKnown starters:\n  ${KNOWN_STARTERS.join("\n  ")}`);
  process.exit(1);
}

function takeFlagValue(args: string[], flag: string): string | undefined {
  const idx = args.indexOf(flag);
  if (idx === -1) return undefined;
  const value = args[idx + 1];
  if (value === undefined || value.startsWith("-")) usage();
  // Remove both the flag and its value from the array so the positional filter
  // below doesn't pick the value up as a starter name.
  args.splice(idx, 2);
  return value;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const tarballDir = takeFlagValue(args, "--tarball-dir");
  const all = args.includes("--all");
  const verbose = args.includes("--verbose") || args.includes("-v");
  const skipE2E = args.includes("--skip-e2e");
  const keepTempDir = args.includes("--keep");
  const positional = args.filter((a) => !a.startsWith("-"));

  let targets: StarterName[];
  if (all) {
    targets = [...KNOWN_STARTERS];
  } else if (positional.length === 0) {
    usage();
  } else {
    targets = positional.map((p) => {
      if (!(KNOWN_STARTERS as readonly string[]).includes(p)) {
        console.error(pc.red(`Unknown starter: ${p}`));
        usage();
      }
      return p as StarterName;
    });
  }

  const repoRoot = findRepoRoot();
  const results: RunStarterResult[] = [];

  for (const starter of targets) {
    console.log(pc.bold(`\n▶ ${starter}`));
    const r = await runStarter({ starter, repoRoot, verbose, skipE2E, keepTempDir, tarballDir });
    results.push(r);
    printResult(r);
  }

  const failures = results.filter((r) => !r.success);
  if (failures.length > 0) {
    console.log(pc.red(`\n${failures.length}/${results.length} starter(s) failed.`));
    process.exit(1);
  }
  console.log(pc.green(`\nAll ${results.length} starter(s) passed.`));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

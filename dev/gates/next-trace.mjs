#!/usr/bin/env node
// The Next.js trace test (packages/jazz-tools/scripts/next-trace-napi.test.mjs,
// adopter gate B65-1) with what it needs: the sealed native pair and an
// admitted jazz-tools build, which it packs into a pnpm Next app.
//
// CI's fast scope (ready PRs) runs the `next-trace-decide` partition first,
// `next-trace.mjs --decide`: in a pull_request run it says whether the PR
// touches how a deployed backend reaches jazz-napi (GUARDED_PATHS), and
// writes `run=true|false` to $GITHUB_OUTPUT so the job's later steps (setup
// and the `next-trace` partition, this script without a flag) run only then.
// The full scope runs the trace in the typescript partition instead.
import { spawnSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/** What the trace test guards: changes here run it on a ready PR. */
export const GUARDED_PATHS = Object.freeze([
  /^packages\/jazz-tools\/src\/backend\//,
  /^packages\/jazz-tools\/src\/dev\/codegen-napi\.ts$/,
  /^crates\/jazz-napi\//,
  /^packages\/jazz-tools\/scripts\/next-trace-napi\.test\.mjs$/,
  /^dev\/gates\/next-trace\.mjs$/,
]);

export function touchesGuardedPaths(changedFiles) {
  return changedFiles.some((file) => GUARDED_PATHS.some((pattern) => pattern.test(file)));
}

/**
 * Whether to run here: outside a pull_request run, always. In one, CI checks
 * out the PR's merge commit with its first parent, the base; when that diff
 * can't be read, run anyway.
 */
export function shouldRun({ env = process.env, git = runGit } = {}) {
  if (env.GITHUB_EVENT_NAME !== "pull_request") return { run: true, why: "not a pull request" };
  const diff = git(["diff", "--name-only", "HEAD^1", "HEAD"]);
  if (diff === null) return { run: true, why: "the PR's diff could not be read" };
  const changed = diff.split("\n").filter(Boolean);
  return touchesGuardedPaths(changed)
    ? { run: true, why: "the PR touches how a backend reaches jazz-napi" }
    : { run: false, why: "the PR touches nothing the trace test guards" };
}

function runGit(args) {
  const result = spawnSync("git", args, { encoding: "utf8" });
  return result.status === 0 ? result.stdout : null;
}

const STEPS = [
  ["node", ["dev/gates/ensure-correctness-artifacts.mjs"]],
  [
    "node",
    [
      "dev/gates/run-correctness-consumer.mjs",
      "--",
      "pnpm",
      "exec",
      "turbo",
      "run",
      "build",
      "--filter=jazz-tools",
      "--only",
    ],
  ],
  [
    "node",
    [
      "dev/gates/run-correctness-consumer.mjs",
      "--",
      "pnpm",
      "--dir",
      "packages/jazz-tools",
      "test:next-trace",
    ],
  ],
];

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = fileURLToPath(new URL("../..", import.meta.url));
  process.chdir(root);
  if (process.argv[2] === "--decide") {
    const decision = shouldRun();
    console.log(`next-trace: ${decision.run ? "runs" : "skipped"}, ${decision.why}`);
    if (process.env.GITHUB_OUTPUT)
      appendFileSync(process.env.GITHUB_OUTPUT, `run=${decision.run}\n`);
    process.exit(0);
  }
  for (const [command, args] of STEPS) {
    const started = Date.now();
    console.log(`next-trace: ${command} ${args.join(" ")}`);
    const result = spawnSync(command, args, { stdio: "inherit" });
    console.log(`next-trace: done in ${((Date.now() - started) / 1000).toFixed(1)}s`);
    if (result.status !== 0) process.exit(result.status ?? 1);
  }
}

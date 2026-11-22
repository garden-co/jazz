#!/usr/bin/env node
/**
 * TypeScript correctness consumers. Native artifacts are produced elsewhere;
 * this entrypoint deliberately verifies their sealed hand-off before it asks
 * Turbo to build Jazz Tools or starts either test suite.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { runCorrectnessConsumer } from "./run-correctness-consumer.mjs";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

// `--shard NAME` selects one of run-ts-tests.sh's shards (CI's test-ts matrix
// runs one per job); without it every suite runs. The preparation below is the
// same for every shard.
const args = process.argv.slice(2);
if (!(args.length === 0 || (args.length === 2 && args[0] === "--shard" && args[1]))) {
  console.error("usage: run-ts-consumers.mjs [--shard NAME]");
  process.exit(2);
}
// Reject an unknown shard before the builds below, not after them.
if (
  args.length &&
  spawnSync("bash", ["dev/gates/run-ts-tests.sh", "--describe-shard", args[1]], {
    cwd: root,
    stdio: ["ignore", "ignore", "inherit"],
  }).status !== 0
)
  process.exit(2);

try {
  // Node scaffold tests intentionally consume jazz-rn's installed JS entry
  // points. Rebuild that bridge before tests: prepare may have run before a
  // rebase changed the command ABI, leaving valid-looking stale lib/ output.
  await runCorrectnessConsumer("pnpm", ["--filter", "jazz-rn", "build"], {
    cwd: root,
    rootDir: root,
  });
  await runCorrectnessConsumer(
    "pnpm",
    ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--only"],
    { cwd: root, rootDir: root },
  );
  await runCorrectnessConsumer("pnpm", ["--filter", "inspector", "run", "build:embedded"], {
    cwd: root,
    rootDir: root,
  });
  console.log("ts-consumers: build retained its admitted native artifact snapshot");
  await runCorrectnessConsumer("bash", ["dev/gates/run-ts-tests.sh", ...args], {
    cwd: root,
    rootDir: root,
  });
} catch (error) {
  console.error(`ts-consumers: ${error.message}`);
  process.exitCode = 1;
}

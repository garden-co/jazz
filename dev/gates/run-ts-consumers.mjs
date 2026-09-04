#!/usr/bin/env node
/**
 * TypeScript correctness consumers. Native artifacts are produced elsewhere;
 * this entrypoint deliberately verifies their sealed hand-off before it asks
 * Turbo to build Jazz Tools or starts either test suite.
 */
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { runCorrectnessConsumer } from "./run-correctness-consumer.mjs";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

try {
  await runCorrectnessConsumer(
    "pnpm",
    ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--only"],
    { cwd: root, rootDir: root },
  );
  console.log("ts-consumers: build retained its admitted native artifact snapshot");
  await runCorrectnessConsumer("bash", ["dev/gates/run-ts-tests.sh"], {
    cwd: root,
    rootDir: root,
  });
} catch (error) {
  console.error(`ts-consumers: ${error.message}`);
  process.exitCode = 1;
}

#!/usr/bin/env node
/**
 * TypeScript correctness consumers. Native artifacts are produced elsewhere;
 * this entrypoint deliberately verifies their sealed hand-off before it asks
 * Turbo to build Jazz Tools or starts either test suite.
 */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { verifyCorrectnessArtifactProducer } from "../artifacts/correctness-artifact-producer.mjs";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

function run(executable, args) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(executable, args, { cwd: root, stdio: "inherit", env: process.env });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`TypeScript consumers failed with ${signal ?? `exit ${code ?? 1}`}`));
    });
  });
}

try {
  verifyCorrectnessArtifactProducer(root);
  console.log("ts-consumers: verified native producer manifest");
  await run("pnpm", ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--only"]);
  await run("bash", ["dev/gates/run-ts-tests.sh"]);
} catch (error) {
  console.error(`ts-consumers: ${error.message}`);
  process.exitCode = 1;
}

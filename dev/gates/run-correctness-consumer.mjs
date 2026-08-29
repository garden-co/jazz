#!/usr/bin/env node
/**
 * Single admission point for Node/browser correctness consumers.
 *
 * It validates the producer receipt once, then gives the complete child
 * process tree immutable, content-addressed artifact paths.  Do not replace
 * this with a direct `vitest`/`pnpm test:browser` invocation: package pointers
 * are mutable producer state and are intentionally not correctness authority.
 */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { correctnessArtifactConsumerEnvironment } from "../artifacts/correctness-artifact-producer.mjs";

export const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

export function correctnessConsumerEnvironment(rootDir = root) {
  return { ...process.env, ...correctnessArtifactConsumerEnvironment(rootDir) };
}

export function runCorrectnessConsumer(
  executable,
  args,
  { cwd = process.cwd(), spawnImpl = spawn, rootDir = root } = {},
) {
  const env = correctnessConsumerEnvironment(rootDir);
  return new Promise((resolvePromise, reject) => {
    const child = spawnImpl(executable, args, { cwd, env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`correctness consumer failed with ${signal ?? `exit ${code ?? 1}`}`));
    });
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const argv = process.argv.slice(2);
  if (argv[0] === "--") argv.shift();
  const [executable, ...args] = argv;
  if (!executable) {
    console.error("Usage: node dev/gates/run-correctness-consumer.mjs -- command [args...]");
    process.exitCode = 2;
  } else {
    try {
      await runCorrectnessConsumer(executable, args);
    } catch (error) {
      console.error(`correctness consumer: ${error.message}`);
      process.exitCode = 1;
    }
  }
}

#!/usr/bin/env node
/**
 * Single admission point for Node/browser correctness consumers.
 *
 * It validates the producer receipt, gives the complete child process tree
 * exact content-addressed artifact paths, then validates the same receipt
 * again after the child exits. Do not replace this with a direct
 * `vitest`/`pnpm test:browser` invocation: package pointers are mutable
 * producer state and are intentionally not correctness authority.
 *
 * This is an accidental concurrent-build/workspace-mutation boundary. The
 * paths remain owned by the current OS user; a hostile same-UID process can
 * replace path-based WASM/NAPI inputs between checks. Preventing that would
 * require different consumer APIs based on held file descriptors or content
 * transfer rather than portable filesystem paths.
 */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import {
  correctnessArtifactConsumerEnvironment,
  verifyCorrectnessArtifactConsumerEnvironment,
} from "../artifacts/correctness-artifact-producer.mjs";

export const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

export function correctnessConsumerEnvironment(rootDir = root) {
  if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1") {
    verifyCorrectnessArtifactConsumerEnvironment(rootDir, process.env);
    return { ...process.env };
  }
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
      try {
        verifyCorrectnessArtifactConsumerEnvironment(rootDir, env);
      } catch (error) {
        reject(
          new Error(`correctness artifacts changed during consumer execution (${error.message})`),
        );
        return;
      }
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

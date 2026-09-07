#!/usr/bin/env node
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { verifyCorrectnessArtifactProducer } from "../artifacts/correctness-artifact-producer.mjs";

// Reuse only the official exact-source, sealed producer receipt. This avoids
// rebuilding the same native pair when local CI serializes storage and TS.
export async function ensureCorrectnessArtifacts({
  root = fileURLToPath(new URL("../..", import.meta.url)),
  verify = verifyCorrectnessArtifactProducer,
  build = () =>
    new Promise((resolve, reject) => {
      const child = spawn("pnpm", ["build:correctness-artifacts"], { cwd: root, stdio: "inherit" });
      child.once("error", reject);
      child.once("exit", (code, signal) =>
        code === 0
          ? resolve()
          : reject(new Error(`correctness artifact producer failed: ${signal ?? code}`)),
      );
    }),
} = {}) {
  try {
    verify(root);
    console.log("correctness artifacts: reusing verified exact-source sealed receipt");
    return;
  } catch {
    console.log("correctness artifacts: no valid current receipt; running official producer");
  }
  await build();
  verify(root);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await ensureCorrectnessArtifacts();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

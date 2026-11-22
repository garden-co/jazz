#!/usr/bin/env node
/**
 * The typescript-tools-other partition's first step: the native
 * correctness-artifact producer and the Durable Object host build, run
 * concurrently. The host (fat-LTO cargo plus wasm-opt, minutes) does not
 * depend on the producer's sealed pair; run one after the other they were
 * that shard's critical path.
 *
 * Both are the commands that would otherwise run in sequence: the producer
 * (dev/gates/ensure-correctness-artifacts.mjs) and the host the Durable
 * Object tests build for themselves when it is missing
 * (packages/jazz-tools/scripts/ensure-durable-object-host.mjs), here at the
 * merge-group wasm-opt level. The host
 * builds in its own Cargo target directory so the two do not serialize on
 * Cargo's build-directory lock. This fails if either fails, after both end.
 */
import { spawn } from "node:child_process";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

export const DURABLE_OBJECT_TARGET_DIR = "target/durable-object-host";
// Merge groups test the Durable Object suite on an -O1 host (same Rust and
// fat LTO); the -Oz host that ships gets the same suite in
// build-jazz-packages.yml before every release and in nightly-tests.yml.
export const DURABLE_OBJECT_MERGE_GROUP_WASM_OPT_LEVEL = "O1";

function run(label, args, env = {}) {
  return new Promise((resolvePromise) => {
    const child = spawn(process.execPath, args, {
      cwd: root,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const prefix = (stream, target) => {
      let pending = "";
      stream.on("data", (chunk) => {
        pending += chunk;
        const lines = pending.split("\n");
        pending = lines.pop();
        for (const line of lines) target.write(`[${label}] ${line}\n`);
      });
      stream.on("end", () => pending && target.write(`[${label}] ${pending}\n`));
    };
    prefix(child.stdout, process.stdout);
    prefix(child.stderr, process.stderr);
    child.once("error", (error) => resolvePromise(`${label}: ${error.message}`));
    child.once("close", (code, signal) =>
      resolvePromise(code === 0 ? undefined : `${label} failed with ${signal ?? `exit ${code}`}`),
    );
  });
}

export async function ensureArtifactsAndDurableObjectHost(runStep = run) {
  const failures = (
    await Promise.all([
      runStep("producer", ["dev/gates/ensure-correctness-artifacts.mjs"]),
      runStep(
        "durable-object-host",
        [
          "packages/jazz-tools/scripts/ensure-durable-object-host.mjs",
          "--wasm-opt-level",
          DURABLE_OBJECT_MERGE_GROUP_WASM_OPT_LEVEL,
        ],
        {
          CARGO_TARGET_DIR: join(root, DURABLE_OBJECT_TARGET_DIR),
        },
      ),
    ])
  ).filter(Boolean);
  if (failures.length) throw new Error(failures.join("; "));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  ensureArtifactsAndDurableObjectHost().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

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
import { execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { lstatSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";
import {
  correctnessArtifactConsumerEnvironment,
  verifyCorrectnessArtifactConsumerEnvironment,
} from "../artifacts/correctness-artifact-producer.mjs";

export const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const capabilityPathEnv = "JAZZ_CORRECTNESS_CONSUMER_CAPABILITY";
const capabilityTokenEnv = "JAZZ_CORRECTNESS_CONSUMER_TOKEN";

function processStartIdentity(pid) {
  if (process.platform !== "linux") return undefined;
  try {
    return readFileSync(`/proc/${pid}/stat`, "utf8").trim().split(" ")[21];
  } catch {
    return undefined;
  }
}

function parentPid(pid) {
  if (process.platform === "linux") {
    try {
      const stat = readFileSync(`/proc/${pid}/stat`, "utf8").trim();
      const afterCommand = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      return Number(afterCommand[1]);
    } catch {
      return undefined;
    }
  }
  if (process.platform !== "win32") {
    try {
      return Number(execFileSync("ps", ["-o", "ppid=", "-p", String(pid)], { encoding: "utf8" }));
    } catch {
      return undefined;
    }
  }
  try {
    const output = execFileSync(
      "powershell.exe",
      [
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        `(Get-CimInstance Win32_Process -Filter 'ProcessId = ${pid}').ParentProcessId`,
      ],
      { encoding: "utf8" },
    );
    return Number(output.trim());
  } catch {
    return undefined;
  }
}

function isDescendantOf(ownerPid) {
  let candidate = parentPid(process.pid);
  const visited = new Set();
  while (Number.isInteger(candidate) && candidate > 0 && !visited.has(candidate)) {
    if (candidate === ownerPid) return true;
    visited.add(candidate);
    candidate = parentPid(candidate);
  }
  return false;
}

function inheritedCapability(rootDir, env) {
  const path = env[capabilityPathEnv];
  const token = env[capabilityTokenEnv];
  if (!path || !token) return false;
  try {
    const stat = lstatSync(path);
    if (!stat.isFile() || stat.isSymbolicLink()) return false;
    const capability = JSON.parse(readFileSync(path, "utf8"));
    return (
      capability.token === token &&
      capability.root === resolve(rootDir) &&
      Number.isInteger(capability.ownerPid) &&
      capability.ownerPid > 0 &&
      capability.ownerStart === processStartIdentity(capability.ownerPid) &&
      isDescendantOf(capability.ownerPid)
    );
  } catch {
    return false;
  }
}

function withoutCapability(env) {
  const clean = { ...env };
  delete clean[capabilityPathEnv];
  delete clean[capabilityTokenEnv];
  return clean;
}

function prepareCorrectnessConsumerEnvironment(rootDir = root) {
  if (inheritedCapability(rootDir, process.env)) {
    verifyCorrectnessArtifactConsumerEnvironment(rootDir, process.env);
    return { env: { ...process.env }, release() {} };
  }

  // A fresh invocation always source-admits before it can mint a descendant
  // capability. Ambient public artifact variables alone never select the
  // inherited path.
  const sealedEnvironment = correctnessArtifactConsumerEnvironment(rootDir);
  const token = randomBytes(32).toString("hex");
  const path = join(tmpdir(), `jazz-correctness-consumer-${process.pid}-${token}`);
  const capability = {
    token,
    root: resolve(rootDir),
    ownerPid: process.pid,
    ownerStart: processStartIdentity(process.pid),
  };
  writeFileSync(path, `${JSON.stringify(capability)}\n`, { flag: "wx", mode: 0o600 });
  let released = false;
  return {
    env: {
      ...withoutCapability(process.env),
      ...sealedEnvironment,
      [capabilityPathEnv]: path,
      [capabilityTokenEnv]: token,
    },
    release() {
      if (released) return;
      released = true;
      rmSync(path, { force: true });
    },
  };
}

export function runCorrectnessConsumer(
  executable,
  args,
  { cwd = process.cwd(), spawnImpl = spawn, rootDir = root } = {},
) {
  const admission = prepareCorrectnessConsumerEnvironment(rootDir);
  const { env } = admission;
  return new Promise((resolvePromise, reject) => {
    const child = spawnImpl(executable, args, { cwd, env, stdio: "inherit" });
    child.once("error", (error) => {
      admission.release();
      reject(error);
    });
    child.once("exit", (code, signal) => {
      try {
        verifyCorrectnessArtifactConsumerEnvironment(rootDir, env);
      } catch (error) {
        admission.release();
        reject(
          new Error(`correctness artifacts changed during consumer execution (${error.message})`),
        );
        return;
      }
      admission.release();
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

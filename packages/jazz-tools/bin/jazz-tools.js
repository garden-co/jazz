#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { accessSync, chmodSync, constants, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const BINARIES = {
  "darwin-arm64": "jazz-tools-darwin-arm64",
  "darwin-x64": "jazz-tools-darwin-x64",
  "linux-arm64": "jazz-tools-linux-arm64",
  "linux-x64": "jazz-tools-linux-x64",
  "win32-x64": "jazz-tools-windows-x64.exe",
};

function fail(message) {
  console.error(message);
  process.exit(1);
}

function ensureExecutable(binaryPath, name) {
  if (process.platform === "win32") {
    return;
  }

  try {
    accessSync(binaryPath, constants.X_OK);
    return;
  } catch {}

  try {
    chmodSync(binaryPath, 0o755);
  } catch (error) {
    const details = error instanceof Error ? error.message : String(error);
    fail(`Bundled binary ${name} is not executable and chmod failed: ${details}`);
  }

  try {
    accessSync(binaryPath, constants.X_OK);
  } catch {
    fail(`Bundled binary ${name} is not executable after chmod.`);
  }
}

const key = `${process.platform}-${process.arch}`;
const binaryName = BINARIES[key];
const here = dirname(fileURLToPath(import.meta.url));
const localBinaryName = process.platform === "win32" ? "jazz-tools.exe" : "jazz-tools";
const fallbackCandidates = [
  join(here, "..", "..", "..", "target", "debug", localBinaryName),
  join(here, "..", "..", "..", "target", "release", localBinaryName),
];

const bundledBinaryPath = binaryName ? join(here, "native", binaryName) : undefined;
const binaryPath =
  (bundledBinaryPath && existsSync(bundledBinaryPath) ? bundledBinaryPath : undefined) ??
  fallbackCandidates.find((candidate) => existsSync(candidate));

if (!binaryPath) {
  const lines = [];
  if (!binaryName) {
    lines.push(
      `jazz-tools does not include a bundled binary for ${process.platform}/${process.arch}.`,
    );
  } else {
    lines.push(`Bundled binary missing: ${binaryName}`);
    lines.push("This package may be corrupted or published without target artifacts.");
  }
  lines.push("No local Cargo build was found in target/debug or target/release.");
  lines.push("Run `cargo build -p jazz-tools --bin jazz-tools --features cli` to build locally.");
  fail(lines.join("\n"));
}

ensureExecutable(binaryPath, binaryName ?? localBinaryName);

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: "inherit",
  env: process.env,
});

if (result.error) {
  fail(`Failed to execute ${binaryName ?? binaryPath}: ${result.error.message}`);
}

if (typeof result.status === "number") {
  process.exit(result.status);
}

if (result.signal) {
  process.kill(process.pid, result.signal);
}

process.exit(1);

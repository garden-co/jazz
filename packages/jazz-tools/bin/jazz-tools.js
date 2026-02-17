#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
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

const key = `${process.platform}-${process.arch}`;
const binaryName = BINARIES[key];

if (!binaryName) {
  fail(
    [
      `jazz-tools does not include a bundled binary for ${process.platform}/${process.arch}.`,
      "Install from source with Cargo for unsupported targets.",
    ].join("\n"),
  );
}

const here = dirname(fileURLToPath(import.meta.url));
const binaryPath = join(here, "native", binaryName);

if (!existsSync(binaryPath)) {
  fail(
    [
      `Bundled binary missing: ${binaryName}`,
      "This package may be corrupted or published without target artifacts.",
    ].join("\n"),
  );
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: "inherit",
  env: process.env,
});

if (result.error) {
  fail(`Failed to execute ${binaryName}: ${result.error.message}`);
}

if (typeof result.status === "number") {
  process.exit(result.status);
}

if (result.signal) {
  process.kill(process.pid, result.signal);
}

process.exit(1);

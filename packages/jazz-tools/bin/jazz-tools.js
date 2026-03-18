#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { accessSync, chmodSync, constants, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
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
    fail(`Binary ${name} is not executable and chmod failed: ${details}`);
  }

  try {
    accessSync(binaryPath, constants.X_OK);
  } catch {
    fail(`Binary ${name} is not executable after chmod.`);
  }
}

function parseSchemaDir(args) {
  let schemaDir = "./schema";

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--schema-dir" && args[i + 1]) {
      schemaDir = args[++i];
      continue;
    }

    const prefix = "--schema-dir=";
    if (arg.startsWith(prefix)) {
      schemaDir = arg.slice(prefix.length);
    }
  }

  return schemaDir;
}

function parseWrapperArgs(rawArgs) {
  let rustBinOverride;
  const args = [];

  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i];

    if (arg === "--rust-bin") {
      const value = rawArgs[i + 1];
      if (!value) {
        fail("Missing value for --rust-bin.");
      }
      rustBinOverride = value;
      i += 1;
      continue;
    }

    const prefix = "--rust-bin=";
    if (arg.startsWith(prefix)) {
      const value = arg.slice(prefix.length);
      if (!value) {
        fail("Missing value for --rust-bin.");
      }
      rustBinOverride = value;
      continue;
    }

    args.push(arg);
  }

  return { args, rustBinOverride };
}

function exitWithSpawnResult(result, name) {
  if (result.error) {
    fail(`Failed to execute ${name}: ${result.error.message}`);
  }

  if (typeof result.status === "number") {
    process.exit(result.status);
  }

  if (result.signal) {
    process.kill(process.pid, result.signal);
  }

  process.exit(1);
}

const here = dirname(fileURLToPath(import.meta.url));

const { args, rustBinOverride } = parseWrapperArgs(process.argv.slice(2));
const command = args[0];

// Handle the MCP server before any Rust binary resolution.
if (command === "mcp") {
  const mcpPath = join(here, "..", "dist", "mcp", "server.js");
  const { runServer } = await import(mcpPath);
  await runServer();
  // runServer resolves when stdin closes; process exits naturally.
} else {
  const key = `${process.platform}-${process.arch}`;
  const binaryName = BINARIES[key];
  const localBinaryName = process.platform === "win32" ? "jazz-tools.exe" : "jazz-tools";
  const fallbackCandidates = [
    join(here, "..", "..", "..", "target", "debug", localBinaryName),
    join(here, "..", "..", "..", "target", "release", localBinaryName),
  ];

  const bundledBinaryPath = binaryName ? join(here, "native", binaryName) : undefined;
  if (rustBinOverride && !existsSync(rustBinOverride)) {
    fail(`Configured Rust binary missing: ${rustBinOverride}`);
  }
  const binaryPath =
    rustBinOverride ??
    (bundledBinaryPath && existsSync(bundledBinaryPath)
      ? bundledBinaryPath
      : fallbackCandidates.find((candidate) => existsSync(candidate)));

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

  ensureExecutable(binaryPath, rustBinOverride ?? binaryName ?? localBinaryName);

  if (command === "build" || command === "migrations") {
    const schemaDirArg = parseSchemaDir(args.slice(1));
    const schemaDir = resolve(process.cwd(), schemaDirArg);
    const currentTsPath = join(schemaDir, "current.ts");
    const tsCliPath = join(here, "..", "dist", "cli.js");

    if (command === "build") {
      if (existsSync(currentTsPath) && existsSync(tsCliPath)) {
        console.log(`Detected ${schemaDirArg}/current.ts. Running TypeScript schema build.`);
        const tsBuildResult = spawnSync(
          process.execPath,
          [tsCliPath, "build", "--schema-dir", schemaDirArg, "--jazz-bin", binaryPath],
          {
            stdio: "inherit",
            env: process.env,
          },
        );
        exitWithSpawnResult(tsBuildResult, "TypeScript schema build");
      }
    } else if (existsSync(tsCliPath)) {
      const tsCommandResult = spawnSync(process.execPath, [tsCliPath, ...args], {
        stdio: "inherit",
        env: process.env,
      });
      exitWithSpawnResult(tsCommandResult, "TypeScript migrations CLI");
    }
  }

  const result = spawnSync(binaryPath, args, { stdio: "inherit", env: process.env });
  exitWithSpawnResult(result, binaryName ?? binaryPath);
}

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
    fail(`Binary ${name} is not executable and chmod failed: ${details}`);
  }

  try {
    accessSync(binaryPath, constants.X_OK);
  } catch {
    fail(`Binary ${name} is not executable after chmod.`);
  }
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

function printWrapperHelp() {
  console.log("Jazz distributed database CLI");
  console.log("");
  console.log("Usage: jazz-tools <COMMAND> [options]");
  console.log("");
  console.log("Commands:");
  console.log("  validate              Validate root schema.ts and optional permissions.ts");
  console.log(
    "  schema export         Print structural schema JSON from schema.ts or a schema hash",
  );
  console.log("  permissions status    Show the current server permissions head for this app");
  console.log("  permissions push      Publish the current permissions.ts with head-parent checks");
  console.log(
    "  migrations create     Generate a typed structural migration stub from snapshots or schema hashes",
  );
  console.log("  migrations push       Push a reviewed migration edge to the server");
  console.log("  create                Create a new resource");
  console.log("  server                Run a Jazz server");
  console.log("  mcp                   Run the Jazz MCP server");
  console.log("  help                  Print this message");
  console.log("");
  console.log("Options:");
  console.log("  -h, --help            Print help");
}

const here = dirname(fileURLToPath(import.meta.url));

const { args, rustBinOverride } = parseWrapperArgs(process.argv.slice(2));
const command = args[0];

// Handle the MCP server before any Rust binary resolution.
if (!command || command === "--help" || command === "-h") {
  printWrapperHelp();
} else if (command === "help" && args.length === 1) {
  printWrapperHelp();
} else if (command === "mcp") {
  const mcpPath = join(here, "..", "dist", "mcp", "server.js");
  const { runServer } = await import(mcpPath);
  await runServer();
  // runServer resolves when stdin closes; process exits naturally.
} else if (command === "build") {
  fail("`jazz-tools build` has been renamed to `jazz-tools validate`.");
} else if (
  command === "validate" ||
  command === "migrations" ||
  command === "permissions" ||
  command === "schema"
) {
  const tsCliPath = join(here, "..", "dist", "cli.js");
  if (!existsSync(tsCliPath)) {
    fail(`TypeScript schema CLI missing: ${tsCliPath}`);
  }

  const tsCommandResult = spawnSync(process.execPath, [tsCliPath, ...args], {
    stdio: "inherit",
    env: process.env,
  });
  exitWithSpawnResult(tsCommandResult, "TypeScript schema CLI");
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

  const result = spawnSync(binaryPath, args, { stdio: "inherit", env: process.env });
  exitWithSpawnResult(result, binaryName ?? binaryPath);
}

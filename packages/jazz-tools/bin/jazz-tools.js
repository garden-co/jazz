#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function checkWrapperArgs(rawArgs) {
  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i];
    if (arg === "--env-file") {
      const value = rawArgs[i + 1];
      if (!value || value.startsWith("-")) {
        fail("Missing value for --env-file.");
      }
    } else if (arg.startsWith("--env-file=")) {
      const value = arg.slice("--env-file=".length);
      if (!value || value.startsWith("-")) {
        fail("Missing value for --env-file.");
      }
    }
  }
}

// Look past global env-file flags only to find the command. The original argv
// is passed through unchanged so the TypeScript CLI owns loading and
// normalization.
function findCommand(args) {
  let index = 0;
  while (index < args.length) {
    const arg = args[index];
    if (arg === "--env-file") {
      index += 2;
      continue;
    }
    if (arg.startsWith("--env-file=")) {
      index += 1;
      continue;
    }
    return arg;
  }
  return undefined;
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
  console.log("  validate              Validate root schema.ts and permissions.ts");
  console.log("  schema compile        Print schema JSON from local schema.ts");
  console.log(
    "  migrations create     Generate a typed structural migration stub from snapshots or schema hashes",
  );
  console.log("  migrations graph      Visualize the full migration graph");
  console.log("  login                 Sign in to Jazz Cloud with your dashboard account");
  console.log("  logout                Forget the stored Jazz Cloud session");
  console.log("  whoami                Show the signed-in Jazz Cloud user and teams");
  console.log("  teams list            List your Jazz Cloud teams");
  console.log("  apps <command>        Manage Jazz Cloud apps (run `apps --help`)");
  console.log(
    "  inspect <command>     Read and change an app's data through its backend (run `inspect --help`)",
  );
  console.log("  help                  Print this message");
  console.log("");
  console.log("Options:");
  console.log("  -h, --help            Print help");
}
const here = dirname(fileURLToPath(import.meta.url));

const args = process.argv.slice(2);
checkWrapperArgs(args);
const command = findCommand(args);

if (!command || command === "--help" || command === "-h") {
  printWrapperHelp();
} else if (command === "help" && args.length === 1) {
  printWrapperHelp();
} else if (command === "build") {
  fail("`jazz-tools build` has been renamed to `jazz-tools validate`.");
} else {
  // Every command, including the removed ones (deploy, server, create), is
  // the TypeScript CLI's: it explains what replaced a removed command.
  const tsCliPath = join(here, "..", "dist", "cli.js");
  if (!existsSync(tsCliPath)) {
    fail(`TypeScript schema CLI missing: ${tsCliPath}`);
  }

  const tsCommandResult = spawnSync(process.execPath, ["--", tsCliPath, ...args], {
    stdio: "inherit",
    env: process.env,
  });
  exitWithSpawnResult(tsCommandResult, "TypeScript schema CLI");
}

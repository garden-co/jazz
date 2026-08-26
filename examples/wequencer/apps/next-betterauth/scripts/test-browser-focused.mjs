#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { realpathSync, statSync } from "node:fs";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const appRoot = fileURLToPath(new URL("..", import.meta.url));
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));
const browserRoot = realpathSync(resolve(appRoot, "tests/browser"));
const args = process.argv.slice(2).filter((arg, index) => !(arg === "--" && index === 0));
const fileArg = args.find((arg) => !arg.startsWith("-"));

if (!fileArg) throw new Error("expected one test file inside tests/browser");
const file = realpathSync(resolve(appRoot, fileArg));
if (!statSync(file).isFile() || relative(browserRoot, file).startsWith(`..${sep}`)) {
  throw new Error("test file must be inside tests/browser");
}

const preflight = spawnSync("node", ["dev/artifacts/verify-correctness-test-artifacts.mjs"], {
  cwd: repositoryRoot,
  stdio: "inherit",
});
if ((preflight.status ?? 1) !== 0) process.exit(preflight.status ?? 1);

const result = spawnSync(
  "pnpm",
  ["exec", "vitest", "run", "--config", "vitest.config.browser.ts", file, ...args.filter((arg) => arg !== fileArg)],
  { cwd: appRoot, stdio: "inherit" },
);
process.exitCode = result.status ?? 1;

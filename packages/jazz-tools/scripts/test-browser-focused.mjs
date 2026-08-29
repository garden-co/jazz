#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { isAbsolute, relative, resolve, sep } from "node:path";
import { realpathSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";

export function isInsideBrowserRoot(fileRelative, pathApi = { isAbsolute }) {
  return (
    !pathApi.isAbsolute(fileRelative) &&
    fileRelative !== ".." &&
    !fileRelative.startsWith(`..${sep}`)
  );
}

export function parseArgs(argv) {
  const positional = [];
  let testNamePattern;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--" && i === 0) continue;
    if (arg === "-t" || arg === "--testNamePattern") {
      testNamePattern = argv[++i];
      if (!testNamePattern) throw new Error(`${arg} requires a pattern`);
    } else if (arg.startsWith("--testNamePattern=")) {
      testNamePattern = arg.slice("--testNamePattern=".length);
    } else if (arg.startsWith("-")) {
      throw new Error(`unknown option: ${arg}`);
    } else positional.push(arg);
  }
  if (positional.length !== 1) {
    throw new Error("expected exactly one browser test file");
  }
  const packageRoot = fileURLToPath(new URL("..", import.meta.url));
  const browserRoot = realpathSync(resolve(packageRoot, "tests/browser"));
  const requestedFile = resolve(positional[0]);
  let file;
  try {
    file = realpathSync(requestedFile);
  } catch {
    throw new Error(`browser test file does not exist: ${requestedFile}`);
  }
  if (!statSync(file).isFile())
    throw new Error(`browser test path is not a file: ${requestedFile}`);
  const fileRelative = relative(browserRoot, file);
  if (!isInsideBrowserRoot(fileRelative)) {
    throw new Error("file must be inside tests/browser");
  }
  const args = ["exec", "vitest", "run", "--config", "vitest.config.browser.ts", file];
  if (testNamePattern) args.push("--testNamePattern", testNamePattern);
  return { file, args };
}

export function run(argv, spawn = spawnSync) {
  const { file, args } = parseArgs(argv);
  const result = spawn(
    "node",
    ["../../dev/gates/run-correctness-consumer.mjs", "--", "pnpm", ...args],
    {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
      stdio: "inherit",
    },
  );
  return result.status ?? 1;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = run(process.argv.slice(2));
  } catch (error) {
    console.error(`Focused browser test: ${error.message}`);
    console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.ts [-t name]");
    process.exitCode = 2;
  }
}

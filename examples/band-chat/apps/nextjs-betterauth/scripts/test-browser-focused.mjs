#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { isAbsolute, relative, resolve, sep } from "node:path";
import { realpathSync, statSync } from "node:fs";

function isInsideBrowserRoot(fileRelative) {
  return !isAbsolute(fileRelative) && fileRelative !== ".." && !fileRelative.startsWith(`..${sep}`);
}

function parseArgs(argv) {
  const positional = argv.filter((arg) => arg !== "--");
  if (positional.length !== 1) throw new Error("expected exactly one browser test file");
  const browserRoot = realpathSync(resolve("tests/browser"));
  const requestedFile = resolve(positional[0]);
  let file;
  try {
    file = realpathSync(requestedFile);
  } catch {
    throw new Error(`browser test file does not exist: ${requestedFile}`);
  }
  if (!statSync(file).isFile())
    throw new Error(`browser test path is not a file: ${requestedFile}`);
  if (!isInsideBrowserRoot(relative(browserRoot, file))) {
    throw new Error("file must be inside tests/browser");
  }
  return file;
}

try {
  const file = parseArgs(process.argv.slice(2));
  const result = spawnSync(
    "node",
    [
      "../../../../dev/gates/run-correctness-consumer.mjs",
      "--",
      "pnpm",
      "exec",
      "vitest",
      "run",
      "--config",
      "vitest.config.browser.ts",
      file,
    ],
    { stdio: "inherit" },
  );
  process.exitCode = result.status ?? 1;
} catch (error) {
  console.error(`Focused browser test: ${error.message}`);
  console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.tsx");
  process.exitCode = 2;
}

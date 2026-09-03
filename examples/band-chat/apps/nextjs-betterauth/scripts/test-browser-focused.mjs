#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { isAbsolute, relative, resolve, sep } from "node:path";
import { realpathSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";

function isInsideBrowserRoot(fileRelative) {
  return !isAbsolute(fileRelative) && fileRelative !== ".." && !fileRelative.startsWith(`..${sep}`);
}

export function parseArgs(argv) {
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

export function run(argv, spawn = spawnSync) {
  const file = parseArgs(argv);
  const result = spawn(
    "node",
    [
      "../../../../dev/gates/run-correctness-consumer.mjs",
      "--",
      "bash",
      "-c",
      'pnpm --filter jazz-tools build && pnpm exec vitest run --config vitest.config.browser.ts "$1"',
      "--",
      file,
    ],
    { stdio: "inherit" },
  );
  return result.status ?? 1;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = run(process.argv.slice(2));
  } catch (error) {
    console.error(`Focused browser test: ${error.message}`);
    console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.tsx");
    process.exitCode = 2;
  }
}

#!/usr/bin/env node
import { realpathSync, statSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { isAbsolute, relative, resolve, sep } from "node:path";

try {
  const positional = process.argv.slice(2).filter((arg) => arg !== "--");
  if (positional.length !== 1) throw new Error("expected exactly one browser test file");
  const browserRoot = realpathSync(resolve("tests/browser"));
  const requested = resolve(positional[0]);
  const file = realpathSync(requested);
  const pathFromRoot = relative(browserRoot, file);
  if (
    isAbsolute(pathFromRoot) ||
    pathFromRoot === ".." ||
    pathFromRoot.startsWith(`..${sep}`) ||
    !statSync(file).isFile()
  ) {
    throw new Error("file must be inside tests/browser");
  }
  const result = spawnSync(
    "pnpm",
    ["exec", "vitest", "run", "--config", "vitest.config.browser.ts", file],
    { stdio: "inherit" },
  );
  process.exitCode = result.status ?? 1;
} catch (error) {
  console.error(`Focused browser test: ${error instanceof Error ? error.message : String(error)}`);
  console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.tsx");
  process.exitCode = 2;
}

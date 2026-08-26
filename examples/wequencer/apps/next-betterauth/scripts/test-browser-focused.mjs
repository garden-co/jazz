#!/usr/bin/env node
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { runFocusedBrowserTest } from "../../../../../packages/jazz-tools/scripts/test-browser-focused.mjs";

const appRoot = fileURLToPath(new URL("..", import.meta.url));
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));

try {
  process.exitCode = runFocusedBrowserTest(process.argv.slice(2), undefined, {
    cwd: appRoot,
    browserRoot: resolve(appRoot, "tests/browser"),
    config: "vitest.config.browser.ts",
    artifactPreflightCwd: repositoryRoot,
  });
} catch (error) {
  console.error(`Focused browser test: ${error.message}`);
  console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.ts [-t name]");
  process.exitCode = 2;
}

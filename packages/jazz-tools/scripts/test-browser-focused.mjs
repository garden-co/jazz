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

const defaultPackageRoot = fileURLToPath(new URL("..", import.meta.url));

/**
 * Build a deliberately narrow browser-Vitest invocation.
 *
 * Jazz-tools owns the argument validation and artifact preflight, while example
 * apps supply their own test root and Vitest config. This keeps an app's
 * focused topology receipt in its real browser environment instead of trying
 * to route it through jazz-tools' browser-only test directory.
 */
export function parseFocusedBrowserArgs(argv, {
  cwd = defaultPackageRoot,
  browserRoot = resolve(cwd, "tests/browser"),
  config = "vitest.config.browser.ts",
} = {}) {
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
  const canonicalBrowserRoot = realpathSync(browserRoot);
  // Resolve the forwarded path in the Vitest project's directory, not in the
  // caller's shell directory. The latter is usually equivalent for jazz-tools,
  // but is wrong when the same runner is used by an example app from the repo
  // root or by the topology soak harness.
  const requestedFile = resolve(cwd, positional[0]);
  let file;
  try {
    file = realpathSync(requestedFile);
  } catch {
    throw new Error(`browser test file does not exist: ${requestedFile}`);
  }
  if (!statSync(file).isFile())
    throw new Error(`browser test path is not a file: ${requestedFile}`);
  const fileRelative = relative(canonicalBrowserRoot, file);
  if (!isInsideBrowserRoot(fileRelative)) {
    throw new Error("file must be inside tests/browser");
  }
  const args = ["exec", "vitest", "run", "--config", config, file];
  if (testNamePattern) args.push("--testNamePattern", testNamePattern);
  return { file, args };
}

export function runFocusedBrowserTest(argv, spawn = spawnSync, {
  cwd = defaultPackageRoot,
  browserRoot = resolve(cwd, "tests/browser"),
  config = "vitest.config.browser.ts",
  artifactPreflight = resolve(defaultPackageRoot, "../../dev/artifacts/verify-correctness-test-artifacts.mjs"),
  artifactPreflightCwd = defaultPackageRoot,
} = {}) {
  const { file, args } = parseFocusedBrowserArgs(argv, { cwd, browserRoot, config });
  const preflight = spawn("node", [artifactPreflight], {
    cwd: artifactPreflightCwd,
    stdio: "inherit",
  });
  if ((preflight.status ?? 1) !== 0) return preflight.status ?? 1;
  const result = spawn("pnpm", args, {
    cwd,
    stdio: "inherit",
  });
  return result.status ?? 1;
}

// Keep the package's public focused-test helpers stable while allowing other
// workspace apps to use the same validation and stale-artifact preflight.
export const parseArgs = (argv) => parseFocusedBrowserArgs(argv);
export const run = (argv, spawn = spawnSync) => runFocusedBrowserTest(argv, spawn);

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = run(process.argv.slice(2));
  } catch (error) {
    console.error(`Focused browser test: ${error.message}`);
    console.error("Usage: pnpm test:browser:focused -- tests/browser/file.test.ts [-t name]");
    process.exitCode = 2;
  }
}

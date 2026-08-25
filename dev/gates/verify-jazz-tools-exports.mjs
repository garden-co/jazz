#!/usr/bin/env node
/** Verify the generated files named by Jazz Tools' public package contract. */
import { lstatSync, readFileSync } from "node:fs";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");

// These files are invoked by the test command graph without being package
// exports themselves. Keep this list deliberately small: public exports stay
// derived from package.json, so adding a new SDK surface cannot silently omit
// it from the preflight.
const internalTestEntrypoints = [
  "dist/cli.js",
  "dist/runtime/client-session.js",
  "dist/backend/request-auth.js",
];

function exportedFiles(value, files) {
  if (typeof value === "string") {
    if (value.startsWith("./dist/")) files.add(value.slice(2));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const nested of Object.values(value)) exportedFiles(nested, files);
}

export function missingJazzToolsTestSurface(root = repositoryRoot) {
  const packageRoot = resolve(root, "packages/jazz-tools");
  const manifest = JSON.parse(readFileSync(resolve(packageRoot, "package.json"), "utf8"));
  const expected = new Set(internalTestEntrypoints);
  exportedFiles(manifest.exports, expected);
  return [...expected].sort().filter((relative) => {
    const path = resolve(packageRoot, relative);
    const withinPackage = !relativePathEscapes(packageRoot, path);
    if (!withinPackage) return true;
    try {
      return !lstatSync(path).isFile();
    } catch {
      return true;
    }
  });
}

function relativePathEscapes(packageRoot, candidate) {
  const pathFromRoot = relative(packageRoot, candidate);
  return pathFromRoot === "" || pathFromRoot === ".." || pathFromRoot.startsWith(`..${sep}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const missing = missingJazzToolsTestSurface();
  if (missing.length) {
    for (const path of missing) console.error(`jazz-tools public export is missing: ${path}`);
    console.error("Fix: pnpm build:test-artifacts");
    process.exitCode = 1;
  }
}

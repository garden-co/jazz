// No example's client code imports its permissions module.
//
// An app's compiled permissions belong to its backends: they read them from
// jazz.permissions.json. A client file that imports permissions.ts (even for
// one constant) pulls `definePermissions(...)` and the app's rules into
// every client bundle that reaches it. Tests, benchmarks, scripts and files
// named as server code may import it.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/** Imports of an app's permissions module: `./permissions`, `../permissions.js`, `@/permissions`, `$lib/permissions`. */
const PERMISSIONS_IMPORT =
  /\bfrom\s+["'](?:(?:\.{1,2}\/)+(?:src\/(?:lib\/)?)?|@\/|\$lib\/)permissions(?:\.[jt]s)?["']|\bimport\(\s*["'](?:\.{1,2}\/)+permissions(?:\.[jt]s)?["']\s*\)/;

/** Paths that never reach a client bundle. */
const NOT_CLIENT =
  /(?:^|\/)(?:tests?|__tests__|benchmarks?|scripts|server|api)\/|\.(?:test|spec|server)\.[cm]?[jt]sx?$|(?:^|\/)(?:permissions|backend[^/]*|server|jazz\.server|middleware|instrumentation)\.[cm]?[jt]sx?$|(?:^|\/)route\.[jt]s$/;

function trackedSources() {
  return execFileSync(
    "git",
    [
      "ls-files",
      "--",
      "examples/**/*.ts",
      "examples/**/*.tsx",
      "examples/**/*.svelte",
      "examples/**/*.vue",
      "dev/stress-tests/**/*.ts",
      "dev/stress-tests/**/*.tsx",
    ],
    { cwd: root, encoding: "utf8" },
  )
    .split("\n")
    .filter(Boolean);
}

test("no example's client code imports its permissions module", () => {
  const offenders = trackedSources()
    .filter((path) => !NOT_CLIENT.test(path))
    .filter((path) => PERMISSIONS_IMPORT.test(readFileSync(join(root, path), "utf8")));
  assert.deepEqual(
    offenders,
    [],
    `These client files import a permissions module, which puts the app's rules in every client bundle. ` +
      `Move what they need (a constant, say) to a module both sides import:\n${offenders.join("\n")}`,
  );
});

test("the check recognises a client import and a server one", () => {
  assert.ok(PERMISSIONS_IMPORT.test('import { MAX } from "@/permissions";'));
  assert.ok(PERMISSIONS_IMPORT.test('import permissions from "../permissions.js";'));
  assert.ok(!PERMISSIONS_IMPORT.test('import permissions from "./jazz.permissions.json";'));
  assert.ok(!NOT_CLIENT.test("examples/x/src/components/Cart.tsx"));
  assert.ok(NOT_CLIENT.test("examples/x/src/lib/jazz.server.ts"));
  assert.ok(NOT_CLIENT.test("examples/x/app/api/jazz/route.ts"));
});

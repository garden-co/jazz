// No example's client code imports its jazz.permissions.json.
//
// An app's permissions belong to its backends: `createJazz({ app, permissions })`
// takes the imported file. A client file that imports it puts the app's rules
// in every client bundle that reaches it. Tests, benchmarks, scripts and files
// named as server code may import it.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/** Imports of an app's permissions file: `./jazz.permissions.json`, `@/jazz.permissions.json`, `$lib/…`, `import(…)`. */
const PERMISSIONS_IMPORT =
  /\bfrom\s+["'](?:(?:\.{1,2}\/)+(?:src\/(?:lib\/)?)?|@\/|\$lib\/)jazz\.permissions\.json["']|\bimport\(\s*["'](?:\.{1,2}\/)+(?:src\/(?:lib\/)?)?jazz\.permissions\.json["']/;

/**
 * Paths that never reach a client bundle: tests, benchmarks, scripts, and
 * anything whose path names it a server (`server/`, `jazz-server.ts`,
 * `todo-server-ts/`, `*-server-snippet.ts`).
 */
const NOT_CLIENT =
  /server|(?:^|\/)(?:tests?|__tests__|benchmarks?|scripts|api)\/|\.(?:test|spec|server)\.[cm]?[jt]sx?$|(?:^|\/)(?:permissions|backend[^/]*|server|jazz\.server|middleware|instrumentation)\.[cm]?[jt]sx?$|(?:^|\/)route\.[jt]s$/;

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
    `These client files import jazz.permissions.json, which puts the app's rules in every client bundle. ` +
      `Only backends pass it to createJazz:\n${offenders.join("\n")}`,
  );
});

test("the check recognises a client import and a server one", () => {
  assert.ok(PERMISSIONS_IMPORT.test('import permissions from "@/jazz.permissions.json";'));
  assert.ok(
    PERMISSIONS_IMPORT.test(
      'import permissions from "../jazz.permissions.json" with { type: "json" };',
    ),
  );
  assert.ok(PERMISSIONS_IMPORT.test('const p = await import("./jazz.permissions.json");'));
  assert.ok(!PERMISSIONS_IMPORT.test('import { app } from "./jazz.generated";'));
  assert.ok(!NOT_CLIENT.test("examples/x/src/components/Cart.tsx"));
  assert.ok(NOT_CLIENT.test("examples/x/src/lib/jazz.server.ts"));
  assert.ok(NOT_CLIENT.test("examples/x/app/api/jazz/route.ts"));
  assert.ok(NOT_CLIENT.test("examples/x/lib/jazz-server.ts"));
  assert.ok(NOT_CLIENT.test("examples/todo-server-ts/src/main.ts"));
});

// The browser tests' app: tests/browser/app holds its jazz.app.json and
// jazz.permissions.json, and the jazz.generated.ts `jazz-tools generate
// --schema-dir tests/browser/app` writes from them. Node only (it reads the
// permissions files); the browser fixture imports ./app/jazz.generated.ts.
import { readFileSync } from "node:fs";
import type { CreateJazzOptions } from "jazz-tools/backend";
import type { TestPermissionsFile } from "jazz-tools/testing";

type AppPermissionsFile = CreateJazzOptions["permissions"] & TestPermissionsFile;

export { app } from "./app/jazz.generated.ts";

const permissionsFile = (name: string): AppPermissionsFile =>
  JSON.parse(readFileSync(new URL(`./app/${name}`, import.meta.url), "utf8")) as AppPermissionsFile;

// Embedded Inspector hosts are ordinary sessions and must be able to read.
export const permissions = permissionsFile("jazz.permissions.json");

// Standalone Inspector admin reads must use SYSTEM authority rather than
// succeeding only because the application happens to allow public reads.
export const standalonePermissions = permissionsFile("standalone.permissions.json");

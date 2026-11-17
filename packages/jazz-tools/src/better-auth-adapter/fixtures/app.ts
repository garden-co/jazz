// The Better Auth tables an app gets from `generate` (with the admin-style
// extra user fields the adapter tests write), and their deny-all
// jazz.permissions.json entries. Node only: loads jazz-napi.
import { readFileSync } from "node:fs";
import { testApp, testPermissions } from "../../testing/test-app.js";

const read = (name: string) => readFileSync(new URL(name, import.meta.url), "utf8");

export const appFile = JSON.parse(read("./jazz.app.json")) as {
  tables: Record<string, { columns: Record<string, unknown> }>;
};
export const permissionsFile = JSON.parse(read("./jazz.permissions.json"));
export const app = testApp(appFile);
export const wasmSchema = app.wasmSchema;
/** The engine's permissions, as a backend loads them from jazz.permissions.json. */
export const permissions = testPermissions(app, permissionsFile);

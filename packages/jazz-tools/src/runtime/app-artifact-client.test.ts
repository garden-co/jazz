import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { shippedArtifact, type AppPermissions } from "./app-artifact.js";

// An example app as validate wrote its files and as its clients import it.
const EXAMPLE = new URL("../../../../examples/todo-server-ts/", import.meta.url);
const { app } = (await import(new URL("schema.ts", EXAMPLE).href)) as {
  app: { wasmSchema: WasmSchema };
};
const permissionsFile = JSON.parse(
  await readFile(new URL("jazz.permissions.json", EXAMPLE), "utf8"),
) as AppPermissions;

/** Every key anywhere in `value`. */
function keysOf(value: unknown, into = new Set<string>()): Set<string> {
  if (Array.isArray(value)) for (const item of value) keysOf(item, into);
  else if (value && typeof value === "object") {
    for (const [key, entry] of Object.entries(value)) {
      into.add(key);
      keysOf(entry, into);
    }
  }
  return into;
}

describe("what a client's app object carries", () => {
  // Clients import schema.ts, so whatever `defineApp` holds ends up in every
  // client bundle: it must not hold the app's permissions.
  it("is the app's jazz.app.json, with no permissions in it", () => {
    const artifact = shippedArtifact(app.wasmSchema)!;
    expect(Object.keys(artifact).sort()).toEqual([
      "codegen",
      "current",
      "format",
      "migrations",
      "schemas",
    ]);
    const keys = keysOf(artifact);
    for (const policyKey of ["permissions", "bundle", "using", "with_check", "select"]) {
      expect(keys.has(policyKey), policyKey).toBe(false);
    }
    expect(keysOf(app.wasmSchema).has("policies")).toBe(false);
  });

  it("while the backend's jazz.permissions.json holds them, for the same version", () => {
    expect(permissionsFile.schema).toBe(shippedArtifact(app.wasmSchema)!.current);
    expect(Object.keys(permissionsFile.permissions ?? {}).sort()).toEqual(["projects", "todos"]);
  });
});

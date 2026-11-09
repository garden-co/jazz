import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { ClientMigrations } from "./client-migrations.js";

const migrations: ClientMigrations = {
  schemas: [{ hash: "a".repeat(64), schema: { tables: {} } }],
  migrations: [],
};

/** A fresh copy of the module, as a second bundle of jazz-tools loads it. */
async function freshCopy() {
  vi.resetModules();
  return import("./client-migrations.js");
}

describe("an app's shipped migrations", () => {
  it("are read by another copy of jazz-tools than the one that defined the app", async () => {
    // Next's server build can load the module defining the app and the one
    // opening its store as separate copies.
    const defining = await freshCopy();
    const opening = await freshCopy();
    expect(opening).not.toBe(defining);

    const schema = { todos: { columns: [] } } as unknown as WasmSchema;
    defining.shipMigrations(schema, migrations);

    expect(opening.shippedMigrations(schema)).toBe(migrations);
  });

  it("leave the schema's JSON, and so its hash, as it was", async () => {
    const { shipMigrations, shippedMigrations } = await freshCopy();
    const schema = { todos: { columns: [] } } as unknown as WasmSchema;
    const before = JSON.stringify(schema);
    shipMigrations(schema, migrations);
    expect(JSON.stringify(schema)).toBe(before);
    expect(Object.keys(schema)).toEqual(["todos"]);
    expect(shippedMigrations(schema)).toBe(migrations);
  });

  it("are none for an app that ships none", async () => {
    const { shipMigrations, shippedMigrations } = await freshCopy();
    const schema = { todos: { columns: [] } } as unknown as WasmSchema;
    shipMigrations(schema, "none");
    expect(shippedMigrations(schema)).toBeUndefined();
    expect(() => shipMigrations(schema, undefined)).toThrow(/defineApp needs the app's migrations/);
  });
});

import { describe, expect, it } from "vitest";
import { col, defineApp } from "./index.js";
import { getRegisteredWasmSchema } from "./typed-app.js";

// The inspector overlay reads the registered schema so it can render before any
// query has created a runtime client (e.g. a write-only page). It must be known
// statically at defineApp time, independent of any connection.
describe("getRegisteredWasmSchema", () => {
  it("exposes the WasmSchema of the most recently defined app", () => {
    defineApp({
      widgets: { name: col.string(), done: col.boolean() },
    });

    const schema = getRegisteredWasmSchema();
    expect(schema).toBeDefined();
    expect(schema?.widgets).toBeDefined();
    expect(schema?.widgets?.columns.some((c) => c.name === "name")).toBe(true);
  });
});

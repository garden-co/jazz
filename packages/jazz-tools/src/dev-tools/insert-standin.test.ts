import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import type { DbConfig } from "../runtime/db.js";
import { DevToolsDb } from "./extension-panel.js";

// The inspector's grid stages an insert and calls db.insert(table, values).
// db.insert maps the client's returned row through transformRow, which expects a
// WasmRow ({ id, values: WasmValue[] }). DevToolsJazzClient.insert used to return
// the raw input object as the stand-in, so row.values was undefined and
// transformRow crashed with "Cannot read properties of undefined (reading '0')".
const todoSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    owner_id: s.string(),
  }),
};
const app = s.defineApp(todoSchema);

describe("DevToolsDb insert stand-in", () => {
  it("produces a transformable WasmRow stand-in (does not crash mapValue)", () => {
    const db = new DevToolsDb({} as DbConfig);
    expect(() =>
      db.insert(app.todos, { title: "buy milk", done: false, owner_id: "user-1" }),
    ).not.toThrow();
  });
});

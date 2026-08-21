import { describe, expect, it } from "vitest";

import type { WasmSchema } from "../../drivers/types.js";
import { schema as s } from "../../index.js";
import { encodeSchema } from "./schema-codec.js";

describe("branch-aware schema codec", () => {
  it("preserves branchBy columns in the public schema source", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "workspace_id", column_type: { type: "Uuid" }, nullable: false }],
        branchBy: ["workspace_id"],
      },
    };

    const source = JSON.parse(new TextDecoder().decode(encodeSchema(schema)));
    expect(source).toEqual({ tables: schema });
  });

  it("exposes branchBy through the typed schema DSL", () => {
    const app = s.defineApp({
      branches: s.table({ name: s.string() }),
      todos: s
        .table({
          branch_id: s.ref("branches"),
          title: s.string(),
        })
        .branchBy(["branch_id"]),
    });

    expect(app.wasmSchema.todos?.branchBy).toEqual(["branch_id"]);
  });
});

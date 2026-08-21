import { describe, expect, it } from "vitest";

import type { WasmSchema } from "../../drivers/types.js";
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
});

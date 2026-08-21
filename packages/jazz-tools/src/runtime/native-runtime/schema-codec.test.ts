import { describe, expect, it } from "vitest";

import type { WasmSchema } from "../../drivers/types.js";
import { encodeSchema } from "./schema-codec.js";

describe("branch-aware schema codec", () => {
  it("preserves dimensions and table bindings in the public schema source", () => {
    const schema: WasmSchema = {
      todos: {
        branchDimensions: [
          {
            id: "31313131-3131-3131-3131-313131313131",
            name: "workspace",
            columnType: { type: "Uuid" },
            migrationDefault: {
              type: "Uuid",
              value: "00000000-0000-0000-0000-000000000000",
            },
          },
        ],
        columns: [{ name: "workspace_id", column_type: { type: "Uuid" }, nullable: false }],
        branchBy: [{ column: "workspace_id", dimension: "workspace" }],
      },
    };

    const source = JSON.parse(new TextDecoder().decode(encodeSchema(schema)));
    expect(source).toEqual({ tables: schema });
  });
});

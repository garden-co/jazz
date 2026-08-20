import { describe, expect, it } from "vitest";

import type { WasmSchema } from "../../drivers/types.js";
import { encodeSchema } from "./schema-codec.js";

describe("branch-aware schema codec", () => {
  it("matches the Rust postcard layout for dimensions and table bindings", () => {
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

    expect([...encodeSchema(schema)]).toEqual([
      1, 16, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 9, 119, 111, 114, 107,
      115, 112, 97, 99, 101, 10, 8, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 5, 116,
      111, 100, 111, 115, 1, 12, 119, 111, 114, 107, 115, 112, 97, 99, 101, 95, 105, 100, 10, 0, 1,
      12, 119, 111, 114, 107, 115, 112, 97, 99, 101, 95, 105, 100, 16, 49, 49, 49, 49, 49, 49, 49,
      49, 49, 49, 49, 49, 49, 49, 49, 49, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
  });
});

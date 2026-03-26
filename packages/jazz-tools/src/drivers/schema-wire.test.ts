import { describe, expect, it } from "vitest";
import type { WasmSchema } from "./types.js";
import { serializeRuntimeSchema } from "./schema-wire.js";

describe("serializeRuntimeSchema", () => {
  it("serializes Bytea defaults as JSON arrays", () => {
    const schema: WasmSchema = {
      files: {
        columns: [
          {
            name: "payload",
            column_type: { type: "Bytea" },
            nullable: false,
            default: { type: "Bytea", value: new Uint8Array([0, 1, 255]) },
          },
        ],
      },
    };

    expect(JSON.parse(serializeRuntimeSchema(schema))).toEqual({
      files: {
        columns: [
          {
            name: "payload",
            column_type: { type: "Bytea" },
            nullable: false,
            default: { type: "Bytea", value: [0, 1, 255] },
          },
        ],
      },
    });
  });
});

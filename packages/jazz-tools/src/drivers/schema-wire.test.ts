import { describe, expect, it } from "vitest";
import type { WasmSchema } from "./types.js";
import { serializeRuntimeSchema } from "./schema-wire.js";

describe("serializeRuntimeSchema", () => {
  it("wraps runtime schema payloads and serializes Bytea defaults as JSON arrays", () => {
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
      __jazzRuntimeSchema: 1,
      schema: {
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
      },
      loadedPolicyBundle: false,
    });
  });

  it("marks loaded policy bundles explicitly", () => {
    const schema: WasmSchema = {};

    expect(JSON.parse(serializeRuntimeSchema(schema, { loadedPolicyBundle: true }))).toMatchObject({
      __jazzRuntimeSchema: 1,
      schema: {},
      loadedPolicyBundle: true,
    });
  });
});

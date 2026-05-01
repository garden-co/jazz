import { afterEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "./types.js";
import { getRuntimeSchemaCacheKey, serializeRuntimeSchema } from "./schema-wire.js";

afterEach(() => {
  vi.restoreAllMocks();
});

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

  it("memoizes cache keys by runtime schema identity", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    };
    const stringify = vi.spyOn(JSON, "stringify");

    const first = getRuntimeSchemaCacheKey(schema);
    const second = getRuntimeSchemaCacheKey(schema);

    expect(second).toBe(first);
    expect(stringify).toHaveBeenCalledTimes(1);
  });

  it("keeps loaded policy bundle cache keys distinct for the same schema", () => {
    const schema: WasmSchema = {};
    const stringify = vi.spyOn(JSON, "stringify");

    const unloaded = getRuntimeSchemaCacheKey(schema);
    const loaded = getRuntimeSchemaCacheKey(schema, { loadedPolicyBundle: true });

    expect(loaded).not.toBe(unloaded);
    expect(JSON.parse(unloaded)).toMatchObject({ loadedPolicyBundle: false });
    expect(JSON.parse(loaded)).toMatchObject({ loadedPolicyBundle: true });
    expect(stringify).toHaveBeenCalledTimes(2);
  });
});

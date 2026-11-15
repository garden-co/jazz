import { readFileSync } from "node:fs";
import { validateSchema } from "jazz-napi";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { testApp } from "./test-app.js";

const fixture = (path: string) =>
  readFileSync(new URL(`./fixtures/${path}`, import.meta.url), "utf8");
const engineSchema = (name: string) => JSON.parse(fixture(`${name}/schema.json`)) as WasmSchema;

describe("schema fixtures", () => {
  it("compiles column defaults and merge strategies to the engine's values", () => {
    const { wasmSchema } = testApp(fixture("with-defaults/jazz.app.json"));
    const column = (table: string, name: string) =>
      wasmSchema[table]!.columns.find((column) => column.name === name);

    expect(column("todos", "done")?.default).toEqual({ type: "Boolean", value: false });
    expect(column("todos", "tags")?.default).toEqual({
      type: "Array",
      value: [
        { type: "Text", value: "work" },
        { type: "Text", value: "home" },
      ],
    });
    expect(column("todos", "metadata")?.default).toEqual({
      type: "Text",
      value: JSON.stringify({ createdBy: "alice" }),
    });
    // The engine's JSON form; inserts write it as the bytes 00 01 ff.
    expect(column("todos", "avatar")?.default).toEqual({ type: "Bytea", value: [0, 1, 255] });
    expect(column("counters", "count")?.merge_strategy).toBe("Counter");
  });

  it("accepts a payload enum default with null in an optional field", () => {
    expect(() =>
      validateSchema(encodeSchema(engineSchema("with-enum-payload-defaults"))),
    ).not.toThrow();
  });

  it("rejects payload enum defaults with a nested value tag mismatch", () => {
    expect(() => validateSchema(encodeSchema(engineSchema("with-enum-payload-wrong-tag")))).toThrow(
      "default Integer(7) does not match column type Text",
    );
  });

  it("rejects null defaults for required payload enum fields", () => {
    expect(() =>
      validateSchema(encodeSchema(engineSchema("with-enum-payload-required-null"))),
    ).toThrow("payload enum default cannot use null for a required field");
  });
});

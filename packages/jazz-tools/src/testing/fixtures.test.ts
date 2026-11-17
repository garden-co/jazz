import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { testApp } from "./test-app.js";

const fixture = (path: string) =>
  readFileSync(new URL(`./fixtures/${path}`, import.meta.url), "utf8");

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
});

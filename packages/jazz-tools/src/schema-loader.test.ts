import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { loadCompiledSchema } from "./schema-loader.js";

const WITH_DEFAULTS_DIR = fileURLToPath(
  new URL("./testing/fixtures/with-defaults", import.meta.url),
);

describe("loadCompiledSchema", () => {
  it("keeps typed-app schema and wasm schema losslessly aligned", async () => {
    const { app } = (await import("./testing/fixtures/with-defaults/schema.js")) as {
      app: { wasmSchema: unknown };
    };
    const loaded = await loadCompiledSchema(WITH_DEFAULTS_DIR);

    expect(schemaToWasm(loaded.schema)).toEqual(loaded.wasmSchema);
    expect(loaded.wasmSchema).toEqual(app.wasmSchema);

    const todos = loaded.schema.tables.find((table) => table.name === "todos");
    const doneColumn = todos?.columns.find((column) => column.name === "done");
    const tagsColumn = todos?.columns.find((column) => column.name === "tags");
    const metadataColumn = todos?.columns.find((column) => column.name === "metadata");
    const avatarColumn = todos?.columns.find((column) => column.name === "avatar");
    const counter = loaded.schema.tables.find((table) => table.name === "counters");
    const countColumn = counter?.columns.find((column) => column.name === "count");

    expect(doneColumn?.default).toBe(false);
    expect(tagsColumn?.default).toEqual(["work", "home"]);
    expect(metadataColumn?.default).toEqual({ createdBy: "alice" });
    expect(avatarColumn?.default).toEqual(new Uint8Array([0, 1, 255]));
    expect(countColumn?.mergeStrategy).toBe("counter");
    expect(loaded.wasmSchema).toEqual(
      expect.objectContaining({
        todos: expect.objectContaining({
          columns: expect.arrayContaining([
            expect.objectContaining({
              name: "done",
              default: { type: "Boolean", value: false },
            }),
          ]),
        }),
      }),
    );
  });
});

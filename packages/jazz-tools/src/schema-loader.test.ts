import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { loadCompiledSchema } from "./schema-loader.js";

const WITH_DEFAULTS_DIR = fileURLToPath(
  new URL("./testing/fixtures/with-defaults", import.meta.url),
);
const WITH_BIGINT_DIR = fileURLToPath(new URL("./testing/fixtures/with-bigint", import.meta.url));

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

  it("loads typed-app BIGINT columns from wasm schema exports", async () => {
    const loaded = await loadCompiledSchema(WITH_BIGINT_DIR);

    expect(schemaToWasm(loaded.schema)).toEqual(loaded.wasmSchema);

    const counters = loaded.schema.tables.find((table) => table.name === "counters");
    const largeCount = counters?.columns.find((column) => column.name === "largeCount");
    expect(largeCount?.sqlType).toBe("BIGINT");
    expect(largeCount?.default).toBe(9007199254740993n);
  });
});
const FIXTURES_DIR = fileURLToPath(new URL("../tests/ts-dsl/fixtures", import.meta.url));

const fixtureDir = (name: string) => `${FIXTURES_DIR}/${name}`;

describe("bundled DSL schema loading", () => {
  it("collects tables from a public bare jazz-tools side-effect import", async () => {
    const loaded = await loadCompiledSchema(fixtureDir("side-effect-only"));

    expect(loaded.schema.tables.map((table) => table.name)).toEqual(["side_effect_tasks"]);
  });

  it("preserves explicit schema precedence while consuming side-effect collection", async () => {
    const explicit = await loadCompiledSchema(fixtureDir("explicit-precedence"));
    expect(explicit.schema.tables.map((table) => table.name)).toEqual(["explicit_tasks"]);

    const sideEffect = await loadCompiledSchema(fixtureDir("side-effect-only"));
    expect(sideEffect.schema.tables.map((table) => table.name)).toEqual(["side_effect_tasks"]);
  });

  it("cleans failed bundle state so a retry can load the schema", async () => {
    process.env.JAZZ_SCHEMA_LOADER_FAIL_RETRY = "1";
    await expect(loadCompiledSchema(fixtureDir("retry-after-failure"))).rejects.toThrow(
      "intentional schema fixture failure",
    );

    delete process.env.JAZZ_SCHEMA_LOADER_FAIL_RETRY;
    const loaded = await loadCompiledSchema(fixtureDir("retry-after-failure"));
    expect(loaded.schema.tables.map((table) => table.name)).toEqual(["retry_tasks"]);
  });

  it("isolates parallel top-level-await bundles deterministically", async () => {
    const [a, b] = await Promise.all([
      loadCompiledSchema(fixtureDir("parallel-tla-a")),
      loadCompiledSchema(fixtureDir("parallel-tla-b")),
    ]);

    expect(a.schema.tables.map((table) => table.name)).toEqual(["parallel_a"]);
    expect(b.schema.tables.map((table) => table.name)).toEqual(["parallel_b"]);
  });
});

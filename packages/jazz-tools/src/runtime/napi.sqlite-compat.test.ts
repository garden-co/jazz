import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const ALPHA46_TODOS_FIXTURE = fileURLToPath(
  new URL("./fixtures/alpha46-todos.sqlite", import.meta.url),
);
const ALPHA46_TODOS_SCHEMA_HASH =
  "bfd77d25b0696da75df2ca82ab129c6289432decaaad8b86adcb31a366bdd217";
const ALPHA46_TODO_ROW = {
  id: "00000000-0000-4000-8000-000000000046",
  values: [
    { type: "Text", value: "alpha.46 sqlite fixture" },
    { type: "Boolean", value: false },
  ],
};

const ALPHA46_TODOS_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

type CurrentNapiRuntime = {
  getSchemaHash(): string;
  query(queryJson: string): Promise<Array<{ id: string; values: unknown[] }>>;
  close(): void;
};

async function openCurrentRuntimeWithAlpha46Db(): Promise<{
  runtime: CurrentNapiRuntime;
  cleanup: () => Promise<void>;
}> {
  const { NapiRuntime } = await loadNapiModule();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-alpha46-sqlite-"));
  const dataPath = join(dataRoot, "runtime.sqlite");

  await copyFile(ALPHA46_TODOS_FIXTURE, dataPath);

  const runtime = new NapiRuntime(
    serializeRuntimeSchema(ALPHA46_TODOS_SCHEMA),
    "00000000-0000-0000-0000-000000000046",
    "prod",
    "main",
    dataPath,
  ) as unknown as CurrentNapiRuntime;

  return {
    runtime,
    cleanup: async () => {
      runtime.close();
      await rm(dataRoot, { recursive: true, force: true });
    },
  };
}

describe("NAPI SQLite compatibility", () => {
  it("uses the alpha.46 schema hash when the current runtime opens the fixture", async () => {
    const { runtime, cleanup } = await openCurrentRuntimeWithAlpha46Db();

    try {
      expect(runtime.getSchemaHash()).toBe(ALPHA46_TODOS_SCHEMA_HASH);
    } finally {
      await cleanup();
    }
  });

  it("reads row data when the current runtime opens a SQLite database written by alpha.46", async () => {
    const { runtime, cleanup } = await openCurrentRuntimeWithAlpha46Db();

    try {
      const rows = await runtime.query(
        JSON.stringify({ table: "todos", relation_ir: { TableScan: { table: "todos" } } }),
      );

      expect(rows).toEqual([ALPHA46_TODO_ROW]);
    } finally {
      await cleanup();
    }
  });
});

/**
 * SPEC 20 in the TS DSL: `s.vector`, `s.point`, the table's `textIndex`,
 * `vectorIndex` and `geoIndex`, and their migrations, from `schema.ts` to
 * `jazz.app.json` and back through the printer, with the same schema hash.
 */
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";
import { readableSchema } from "./define-schema.js";
import { printSchemaTs } from "../printer/index.js";
import { printTable, printDefault } from "../printer/schema.js";
import { printMigrations } from "../printer/migrations.js";
import type { AppJson, TableJson } from "../printer/app-json.js";
import { createMigration, loadMigrationHistory, type ModuleLoader } from "../dev/migrations.js";
import { loadAppFiles } from "../dev/codegen-napi.js";

// The printed and generated files import "jazz-tools"; the DSL is all they need.
vi.mock("jazz-tools", async () => ({
  ...(await import("../schema-namespace.js")),
  ...(await import("../migration-namespace.js")),
}));

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function scratch(): Promise<string> {
  // Inside the package, so the files resolve "jazz-tools" as an app's would.
  const base = join(import.meta.dirname, "..", "..", "node_modules", ".cache");
  await mkdir(base, { recursive: true });
  const dir = await mkdtemp(join(base, "jazz-specialised-"));
  dirs.push(dir);
  return dir;
}

const loadModule: ModuleLoader = (file) => import(/* @vite-ignore */ file);
const tablesOf = (schema: unknown) => readableSchema(schema)!.tables;

async function schemaHash(app: object): Promise<string> {
  const napi = (await import("jazz-napi")) as unknown as {
    loadAppArtifact(app: string, permissions: string): { current: string };
  };
  return napi.loadAppArtifact(JSON.stringify(app), '{"format":1,"tables":{}}').current;
}

const schema = s.defineSchema({
  places: s
    .table({
      title: s.string(),
      body: s.string().optional(),
      owner: s.string(),
      embedding: s.vector(3).default([0.5, 0.1, 2]),
      guess: s.vector(2).optional(),
      exact: s.vector(2).default(new Float32Array([0.1, 1])),
      location: s.point().default([13.405, 52.52]),
      entrance: s.point().optional(),
    })
    .indexOnly(["owner"])
    .compositeIndex(["owner", "$createdAt"])
    .textIndex(
      "search",
      { title: 4, body: { weight: 1, length: 256 } },
      { analyzer: "jazz-text/1:en", prefix: true },
    )
    .textIndex("words", { title: 1 }, { analyzer: "jazz-text/1:simple" })
    .vectorIndex("similar", "embedding", { metric: "cosine" })
    .vectorIndex("closest", "guess", { metric: "l2" })
    .geoIndex("near", "location"),
});

/** The same table written as `jazz.app.json`, by hand. */
const json: TableJson = {
  columns: {
    title: "string",
    body: "string?",
    owner: "string",
    embedding: { vector: 3, default: [0.5, 0.1, 2] },
    guess: { vector: 2, optional: true },
    exact: { vector: 2, default: [0.1, 1] },
    location: { type: "point", default: [13.405, 52.52] },
    entrance: "point?",
  },
  indexes: [
    "owner",
    ["owner", "$createdAt"],
    {
      name: "search",
      text: { title: 4, body: { weight: 1, length: 256 } },
      analyzer: "jazz-text/1:en",
      prefix: true,
    },
    { name: "words", text: { title: 1 }, analyzer: "jazz-text/1:simple" },
    { name: "similar", vector: "embedding", metric: "cosine" },
    { name: "closest", vector: "guess", metric: "l2" },
    { name: "near", geo: "location" },
  ],
};
const app: AppJson = { format: 1, tables: { places: json } };

describe("specialised indexes in the TS DSL", () => {
  it("generates what jazz.app.json lists", () => {
    const generated = tablesOf(schema).places!;
    expect(generated.indexes).toEqual(json.indexes);
    const { exact, ...columns } = generated.columns as Record<string, unknown>;
    const { exact: handWritten, ...others } = json.columns;
    expect(columns).toEqual(others);
    // A Float32Array default is its elements as the 32-bit floats they are.
    expect(exact).toEqual({ vector: 2, default: [Math.fround(0.1), 1] });
    expect(handWritten).toBeDefined();
  });

  it("hashes as the hand-written file does", async () => {
    expect(await schemaHash({ format: 1, tables: tablesOf(schema) })).toBe(await schemaHash(app));
  });

  it("prints back to TS that generates the same schema", async () => {
    const printed = printSchemaTs(app, { format: 1, tables: { places: {} } });
    expect(printed.unsupported).toEqual([]);
    const text = printed.files["schema.ts"]!;
    expect(text).toContain("s.vector(3).default([0.5, 0.1, 2])");
    expect(text).toContain("s.point().optional()");
    expect(text).toContain('.vectorIndex("similar", "embedding", { metric: "cosine" })');
    expect(text).toContain('.geoIndex("near", "location")');

    const dir = await scratch();
    await writeFile(join(dir, "schema.ts"), text);
    const module = (await loadModule(join(dir, "schema.ts"))) as { schema: unknown };
    const regenerated = tablesOf(module.schema);
    expect(regenerated).toEqual(app.tables);
    expect(await schemaHash({ format: 1, tables: regenerated })).toBe(await schemaHash(app));
  });

  it("reports what the DSL can't say instead of dropping it", () => {
    for (const index of [
      { name: "near", geo: "location", serverOnly: true },
      { name: "near", geo: "location", $comment: "the map" },
    ]) {
      const unsupported: { path: string; text: string; reason: string }[] = [];
      printTable(
        { columns: { location: "point" }, indexes: [index] },
        { path: "tables.places", unsupported },
      );
      expect(unsupported).toHaveLength(1);
      expect(unsupported[0]!.path).toBe("tables.places.indexes.near");
    }
  });

  it("keeps a default's sign of zero, or refuses it", () => {
    expect(printDefault("point", [-0, 1])).toBe("[-0, 1]");
    expect(printDefault({ vector: 2 }, [1, -0])).toBe("[1, -0]");
    expect(() =>
      tablesOf(s.defineSchema({ places: s.table({ at: s.point().default([-0, 1]) }) })),
    ).toThrow(/-0 is not supported yet/);
  });

  it("refuses an unknown column and a second index of a name", () => {
    const table = s.table({
      title: s.string(),
      embedding: s.vector(2),
      location: s.point(),
      shout: s.computed(s.string(), ["title"]),
    });
    const en = { analyzer: "jazz-text/1:en" } as const;
    expect(() => table.geoIndex("near", "nowhere" as never)).toThrow(/unknown column "nowhere"/);
    expect(() => table.textIndex("search", { shout: 1 } as never, en)).toThrow(/unknown column/);
    expect(() => table.geoIndex("near", "location").geoIndex("near", "location")).toThrow(
      /second index named "near"/,
    );
  });

  it("types an index's columns by their kind", () => {
    const table = s.table({ title: s.string(), embedding: s.vector(2), location: s.point() });
    const _typesOnly = () => {
      // @ts-expect-error a geo index takes a point column
      table.geoIndex("near", "title");
      // @ts-expect-error a vector index takes a vector column
      table.vectorIndex("similar", "location", { metric: "l2" });
      // @ts-expect-error a text index takes string columns
      table.textIndex("search", { embedding: 1 }, { analyzer: "jazz-text/1:en" });
      // @ts-expect-error a text index names its analyzer
      table.textIndex("search", { title: 1 }, {});
      // @ts-expect-error a vector index names its metric
      table.vectorIndex("similar", "embedding", {});
      // @ts-expect-error a point is two numbers
      s.point().default([1]);
    };
    expect(_typesOnly).toBeTypeOf("function");
  });
});

describe("migrations over vector and point columns", () => {
  it("builds their steps", () => {
    expect(m.add.vector(2, { default: [1, 2] })).toMatchObject({
      _type: "add",
      sqlType: { kind: "VECTOR", dimensions: 2 },
    });
    expect(m.drop.point({ backwardsDefault: [13.4, 52.5] })).toMatchObject({
      _type: "drop",
      sqlType: "POINT",
    });
  });

  it("writes stubs that load back into the history", async () => {
    const v1 = s.defineSchema({
      places: s.table({ title: s.string(), old: s.vector(4).default([1, 2, 3, 4]) }),
    });
    const v2 = s.defineSchema({
      places: s
        .table({
          title: s.string(),
          embedding: s.vector(3).optional(),
          fixed: s.vector(2).default([0.5, 0.25]),
          location: s.point().default([13.405, 52.52]),
        })
        .geoIndex("near", "location"),
    });
    const migrationsDir = join(await scratch(), "migrations");
    const now = new Date(Date.UTC(2026, 10, 22, 12, 0, 0));
    await createMigration({ migrationsDir, currentTables: tablesOf(v1), loadModule, now });
    const made = await createMigration({
      migrationsDir,
      currentTables: tablesOf(v2),
      loadModule,
      now,
    });
    const stub = await readFile(join(migrationsDir, made.file!), "utf8");
    expect(stub).not.toContain("jazz.app.json");
    expect(stub).toContain("s.vector(3).optional()");
    expect(stub).toContain("s.point().default([13.405, 52.52])");
    expect(stub).toContain("m.add.vector(3, { default: null })");
    expect(stub).toContain("m.add.point({ default: [13.405, 52.52] })");
    expect(stub).toContain("m.drop.vector(4, { backwardsDefault: [1, 2, 3, 4] })");

    const history = await loadMigrationHistory({
      migrationsDir,
      currentTables: tablesOf(v2),
      loadModule,
    });
    expect(history.migrations[0]!.migrate).toEqual({
      places: {
        old: { drop: { vector: 4 }, backwardsDefault: [1, 2, 3, 4] },
        embedding: { add: { vector: 3 }, default: null },
        fixed: { add: { vector: 2 }, default: [0.5, 0.25] },
        location: { add: "point", default: [13.405, 52.52] },
      },
    });
    const files = {
      format: 1,
      tables: tablesOf(v2),
      history: history.history,
      migrations: history.migrations,
    };
    await loadAppFiles(JSON.stringify(files), '{"format":1,"tables":{}}');

    const printed = await printMigrations(files as unknown as AppJson, {
      snapshot: (tables) => ({
        hash: `${Object.keys(tables.places!.columns).length}`.padEnd(64, "0"),
        schema: { tables },
      }),
      start: now,
    });
    expect(printed.unsupported).toEqual([]);
    const reprinted = Object.entries(printed.files).find(([path]) => path.endsWith(".ts"))![1];
    expect(reprinted).toContain("m.add.vector(3, { default: null })");
    expect(reprinted).toContain("m.drop.vector(4, { backwardsDefault: [1, 2, 3, 4] })");
  });
});

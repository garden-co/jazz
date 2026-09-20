import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { loadCompiledSchema } from "./schema-loader.js";
import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "./schema-namespace.js";
import { analyzeRelations } from "./codegen/relation-analyzer.js";
import { schemaDefinitionToAst } from "./migrations.js";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { structuralSchemaHash } from "./dev/schema-utils.js";

const definition = () => ({
  posts: s.table(
    { authorId: s.uuid(), editorId: s.uuid().optional(), reviewerIds: s.array(s.uuid()) },
    {
      author: s.rel("users", "authorId"),
      editor: s.rel("users", "editorId"),
      reviewers: s.rel("users", "reviewerIds"),
    },
  ),
  users: s.table(
    { name: s.string() },
    {
      authoredPosts: s.reverse("posts", "author"),
      editedPosts: s.reverse("posts", "editor"),
      reviewAssignments: s.reverse("posts", "reviewers"),
    },
  ),
  settings: s.table({ theme: s.string() }, {}),
});

describe("explicit relationships", () => {
  it("resolves declared aliases and reverses with column-derived cardinality", () => {
    const app = s.defineApp(s.defineSchema(definition()));
    const relations = analyzeRelations(app.wasmSchema);
    expect(
      relations.get("posts")?.map((r) => [r.name, r.fromColumn, r.toTable, r.isArray, r.nullable]),
    ).toEqual([
      ["author", "authorId", "users", false, false],
      ["editor", "editorId", "users", false, true],
      ["reviewers", "reviewerIds", "users", true, false],
    ]);
    expect(relations.get("users")?.map((r) => [r.name, r.toColumn, r.isArray])).toEqual([
      ["authoredPosts", "authorId", true],
      ["editedPosts", "editorId", true],
      ["reviewAssignments", "reviewerIds", true],
    ]);
    expect(relations.get("settings")).toEqual([]);
    expect(app.wasmSchema.posts!.columns.map((c) => [c.name, c.references])).toEqual([
      ["authorId", "users"],
      ["editorId", "users"],
      ["reviewerIds", "users"],
    ]);
  });
  it("rejects nested reference arrays used as relations", () => {
    const defineTableUnchecked = s.table as unknown as (
      columns: unknown,
      relations: unknown,
    ) => unknown;
    expect(() =>
      defineTableUnchecked(
        { itemIds: s.array(s.array(s.uuid())) },
        { items: s.rel("bundle_items", "itemIds") },
      ),
    ).toThrow(/nested reference array/i);
  });
  it("preserves reference storage identity and excludes aliases from the hash", async () => {
    const app = s.defineApp(definition());
    const legacy = schemaToWasm({
      tables: [
        {
          name: "posts",
          columns: [
            { name: "authorId", sqlType: "UUID", nullable: false, references: "users" },
            { name: "editorId", sqlType: "UUID", nullable: true, references: "users" },
            {
              name: "reviewerIds",
              sqlType: { kind: "ARRAY", element: "UUID" },
              nullable: false,
              references: "users",
            },
          ],
        },
        { name: "users", columns: [{ name: "name", sqlType: "TEXT", nullable: false }] },
        { name: "settings", columns: [{ name: "theme", sqlType: "TEXT", nullable: false }] },
      ],
    });
    expect(app.wasmSchema.posts!.columns).toEqual(legacy.posts!.columns);
    expect(await structuralSchemaHash(app.wasmSchema)).toBe(await structuralSchemaHash(legacy));
    const renamed = structuredClone(app.wasmSchema);
    renamed.posts!.relations!.writer = renamed.posts!.relations!.author!;
    delete renamed.posts!.relations!.author;
    renamed.users!.relations!.authoredPosts = s.reverse("posts", "writer");
    expect(
      analyzeRelations(renamed)
        .get("posts")
        ?.map((r) => r.name),
    ).toContain("writer");
    expect(await structuralSchemaHash(renamed)).toBe(await structuralSchemaHash(legacy));
  });
  it("does not infer relationships from reference metadata or UUID spelling", () => {
    const schema = {
      posts: {
        columns: [
          {
            name: "userId",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
        ],
      },
      users: { columns: [] },
    };
    expect([...analyzeRelations(schema).values()]).toEqual([[], []]);
  });
  it("retains metadata through structural table instances and migration schema conversion", () => {
    const input = definition();
    const ordinary = s.defineApp(input);
    const structural = Object.fromEntries(
      Object.entries(input).map(([name, table]) => [name, { ...table }]),
    );
    const duplicatePackage = (s.defineApp as any)(structural);
    expect(duplicatePackage.wasmSchema).toEqual(ordinary.wasmSchema);
    const ast = schemaDefinitionToAst(input);
    expect(ast.tables.find((t) => t.name === "posts")?.columns.map((c) => c.references)).toEqual([
      "users",
      "users",
      "users",
    ]);
    expect(schemaToWasm(ast)).toEqual(ordinary.wasmSchema);
  });
  it("rejects reserved and prototype aliases before projected fields can be overwritten", () => {
    for (const name of ["$createdAt", "$createdBy", "__proto__", "constructor", "prototype", ""]) {
      expect(() =>
        (s.table as any)({ authorId: s.uuid() }, { [name]: s.rel("users", "authorId") }),
      ).toThrow(/collides/);
      const schema = s.defineApp(definition()).wasmSchema;
      schema.posts!.relations = { [name]: s.rel("users", "authorId") };
      expect(() => analyzeRelations(schema)).toThrow(/collides/);
    }
  });
  it("rejects reference-target migrations while permitting alias-only changes", () => {
    const from = {
      posts: s.table({ owner: s.uuid() }, { author: s.rel("users", "owner") }),
      users: s.table({}, {}),
      teams: s.table({}, {}),
    };
    const to = {
      ...from,
      posts: s.table({ owner: s.uuid() }, { author: s.rel("teams", "owner") }),
    };
    expect(() => (s.defineMigration as any)({ from, to })).toThrow(/match|type|reference/);
    const renamed = {
      ...from,
      posts: s.table({ owner: s.uuid() }, { writer: s.rel("users", "owner") }),
    };
    expect(s.defineMigration({ from, to: renamed }).forward).toEqual([]);
  });
  it("loads definition-only and Wasm-only schema exports without losing relations", async () => {
    const directory = await mkdtemp(join(tmpdir(), "jazz-explicit-relations-"));
    try {
      const source = fileURLToPath(new URL("./schema-namespace.ts", import.meta.url));
      await writeFile(
        join(directory, "schema.ts"),
        `import { schema as s } from ${JSON.stringify(source)}; export const schema = s.defineSchema({ posts: s.table({ owner: s.uuid() }, { author: s.rel("users", "owner") }), users: s.table({ name: s.string() }, { authored: s.reverse("posts", "author") }) });`,
      );
      const loaded = await loadCompiledSchema(directory);
      expect(loaded.wasmSchema.posts!.columns[0]!.references).toBe("users");
      expect(
        analyzeRelations(loaded.wasmSchema)
          .get("users")
          ?.map((r) => r.name),
      ).toEqual(["authored"]);
      expect(schemaToWasm(loaded.schema)).toEqual(loaded.wasmSchema);
      await writeFile(
        join(directory, "schema.ts"),
        `export const app = { wasmSchema: ${JSON.stringify(loaded.wasmSchema)} };`,
      );
      const fallback = await loadCompiledSchema(directory);
      expect(fallback.schema.tables.find((t) => t.name === "posts")?.relations).toEqual({
        author: s.rel("users", "owner"),
      });
      expect(schemaToWasm(fallback.schema)).toEqual(loaded.wasmSchema);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("rejects non-string declaration names without coercion", () => {
    for (const value of [1, true, [], {}, null, ""]) {
      expect(() => (s.rel as any)(value, "owner")).toThrow(/requires/);
      expect(() => (s.rel as any)("users", value)).toThrow(/requires/);
      expect(() => (s.reverse as any)(value, "author")).toThrow(/requires/);
      expect(() => (s.reverse as any)("posts", value)).toThrow(/requires/);
      expect(() =>
        (s.table as any)(
          { owner: s.uuid() },
          { author: { kind: "forward", table: value, column: "owner" } },
        ),
      ).toThrow(/Invalid relationship/);
      const schema = s.defineApp(definition()).wasmSchema;
      schema.posts!.relations = {
        author: { kind: "forward", table: value as any, column: "authorId" },
      };
      expect(() => analyzeRelations(schema)).toThrow(/Invalid relationship/);
    }
  });
  it("rejects invalid local declarations", () => {
    expect(() => (s.table as any)({ value: s.string() })).toThrow(/requires a relationship map/);
    expect(() =>
      (s.table as any)({ userId: s.uuid() }, { userId: s.rel("users", "userId") }),
    ).toThrow(/collides/);
    expect(() =>
      (s.table as any)({ value: s.string() }, { user: s.rel("users", "value") }),
    ).toThrow(/UUID/);
    expect(() => (s.table as any)({}, { user: s.rel("users", "missing") })).toThrow(
      /unknown column/,
    );
  });
  it("rejects invalid global declarations eagerly", () => {
    const define = s.defineSchema as any;
    expect(() =>
      define({ posts: s.table({ owner: s.uuid() }, { author: s.rel("missing", "owner") }) }),
    ).toThrow(/unknown table/);
    expect(() =>
      define({
        posts: s.table({ owner: s.uuid() }, { author: s.rel("users", "owner") }),
        users: s.table({}, { posts: s.reverse("posts", "owner") }),
      }),
    ).toThrow(/named forward/);
    expect(() =>
      define({
        posts: s.table({ owner: s.uuid() }, { author: s.rel("users", "owner") }),
        users: s.table({}, {}),
        others: s.table({}, { posts: s.reverse("posts", "author") }),
      }),
    ).toThrow(/targeting "users"/);
    expect(() =>
      define({
        posts: (s.table as any)(
          { owner: s.uuid() },
          { author: s.rel("users", "owner"), other: s.rel("others", "owner") },
        ),
        users: s.table({}, {}),
        others: s.table({}, {}),
      }),
    ).toThrow(/Conflicting/);
  });
  it("preserves raw column types and infers includes", () => {
    const app = s.defineApp(s.defineSchema(definition()));
    const query = app.posts.include({ author: true, editor: true, reviewers: true });
    type Row = s.RowOf<typeof query>;
    expectTypeOf<Row["authorId"]>().toEqualTypeOf<string>();
    expectTypeOf<Row["editorId"]>().toEqualTypeOf<string | null>();
    expectTypeOf<Row["reviewerIds"]>().toEqualTypeOf<string[]>();
    expectTypeOf<Row["author"]>().toMatchTypeOf<{ name: string } | null>();
    expectTypeOf<Row["editor"]>().toMatchTypeOf<{ name: string } | null>();
    expectTypeOf<Row["reviewers"]>().toMatchTypeOf<{ name: string }[]>();
    app.users.include({ authoredPosts: true });
  });
});

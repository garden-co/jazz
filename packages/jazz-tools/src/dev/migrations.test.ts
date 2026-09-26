import { assertMigrationMatchesCanonicalBundle } from "./catalogue.js";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { wasmSchemasEqual, structuralSchemaHash } from "./schema-utils.js";
import { renderMigrationStub } from "./migrations.js";

describe("migration stub generation", () => {
  it("generates additive table witnesses with bare, nullable, and referenced UUIDs", () => {
    const from = {
      users: s.table({ name: s.string() }, {}),
    };
    const to = {
      ...from,
      records: s.table(
        {
          externalId: s.uuid(),
          previousId: s.uuid().optional(),
          ownerId: s.uuid(),
          reviewerId: s.uuid().optional(),
        },
        { owner: s.rel("users", "ownerId"), reviewer: s.rel("users", "reviewerId") },
      ),
    };
    const source = renderMigrationStub({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: s.defineApp(from).wasmSchema,
      toSchema: s.defineApp(to).wasmSchema,
    });

    expect(source).toContain('"externalId": s.uuid(),');
    expect(source).toContain('"previousId": s.uuid().optional(),');
    expect(source).toContain('"ownerId": s.uuid(),');
    expect(source).toContain('"reviewerId": s.uuid().optional(),');

    expect(source).toContain('"owner": s.rel("users", "ownerId")');
    expect(source).toContain('"reviewer": s.rel("users", "reviewerId")');

    // The generated stub is executable JavaScript: run it through the public
    // migration builder to verify the additive-table migration is usable.
    const migration = new Function(
      "s",
      source
        .replace('import { schema as s } from "jazz-tools";', "")
        .replace("export default", "return"),
    )(s);
    expect(migration.forward).toEqual([{ table: "records", added: true, operations: [] }]);
  });
  it("executes a generated UUID reference addition as an explicit identity lens", () => {
    const users = s.table({ name: s.string() }, {});
    const columns = {
      ownerId: s.uuid(),
      reviewerId: s.uuid().optional(),
      memberIds: s.array(s.uuid()),
    };
    const from = { users, records: s.table(columns, {}) };
    const to = {
      users,
      records: s.table(columns, {
        owner: s.rel("users", "ownerId"),
        reviewer: s.rel("users", "reviewerId"),
        members: s.rel("users", "memberIds"),
      }),
    };
    const source = renderMigrationStub({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: s.defineApp(from).wasmSchema,
      toSchema: s.defineApp(to).wasmSchema,
    });
    expect(source).not.toContain("TODO");
    const migration = new Function(
      "s",
      source
        .replace('import { schema as s } from "jazz-tools";', "")
        .replace("export default", "return"),
    )(s);
    expect(migration.forward).toEqual([{ table: "records", operations: [] }]);
    expect(Object.keys(migration.from)).toEqual(["records", "users"]);
    expect(Object.keys(migration.to)).toEqual(["records", "users"]);
    expect(s.defineMigration({ from, to }).forward).toEqual([{ table: "records", operations: [] }]);
    expect(
      s.defineMigration({
        from: to,
        to: {
          users,
          records: s.table(columns, {
            author: s.rel("users", "ownerId"),
            reviewer: s.rel("users", "reviewerId"),
            members: s.rel("users", "memberIds"),
          }),
        },
      }).forward,
    ).toEqual([]);
    expect(() => (s.defineMigration as (config: any) => unknown)({ from: to, to: from })).toThrow(
      "same reference target",
    );
  });
  it("composes reference additions with explicit column operations and rejects unsupported changes", () => {
    const users = s.table({ name: s.string() }, {});
    const from = { users, records: s.table({ ownerId: s.uuid(), title: s.string() }, {}) };
    const to = {
      users,
      records: s.table(
        { ownerId: s.uuid(), title: s.string(), note: s.string().optional() },
        { owner: s.rel("users", "ownerId") },
      ),
    };
    const migration = s.defineMigration({
      from,
      to,
      migrate: { records: { note: s.add.string({ default: null }) } },
    });
    expect(migration.forward).toEqual([
      {
        table: "records",
        operations: [{ type: "introduce", column: "note", sqlType: "TEXT", value: null }],
      },
    ]);
    // Exercise runtime validation too: generated modules do not run the type checker.
    const define = s.defineMigration as (config: any) => unknown;
    expect(() => define({ from, to })).toThrow("unchanged column shapes");
    expect(() =>
      define({
        from,
        to: {
          users,
          records: s.table(
            { ownerId: s.uuid().optional(), title: s.string() },
            { owner: s.rel("users", "ownerId") },
          ),
        },
      }),
    ).toThrow("same reference target");
    expect(() =>
      define({
        from,
        to: {
          users,
          records: s.table(
            { ownerId: s.uuid(), title: s.int() },
            { owner: s.rel("users", "ownerId") },
          ),
        },
      }),
    ).toThrow("unchanged column shapes");
    expect(() =>
      define({
        from,
        to: {
          records: s.table(
            { ownerId: s.uuid(), title: s.string() },
            { owner: s.rel("missing", "ownerId") },
          ),
        },
      }),
    ).toThrow("requires target table");
    expect(() =>
      define({
        from: to,
        to: {
          users,
          others: users,
          records: s.table(
            { ownerId: s.uuid(), title: s.string(), note: s.string().optional() },
            { owner: s.rel("others", "ownerId") },
          ),
        },
      }),
    ).toThrow("same reference target");
    expect(() =>
      define({
        from,
        to: {
          users,
          records: s.table(
            { authorId: s.uuid(), title: s.string() },
            { owner: s.rel("users", "authorId") },
          ),
        },
        migrate: { records: { authorId: s.renameFrom("ownerId") } },
      }),
    ).toThrow("same reference target");
  });

  it("compares unchanged nested bigint defaults and rejects concurrent default or merge changes", () => {
    const users = s.table({ name: s.string() }, {});
    const before = {
      users,
      records: s.table(
        { ownerId: s.uuid(), counters: s.array(s.bigint()).default([1n]), score: s.int() },
        {},
      ),
    };
    const after = {
      users,
      records: s.table(
        { ownerId: s.uuid(), counters: s.array(s.bigint()).default([1n]), score: s.int() },
        { owner: s.rel("users", "ownerId") },
      ),
    };
    expect(s.defineMigration({ from: before, to: after }).forward).toEqual([
      { table: "records", operations: [] },
    ]);
    const define = s.defineMigration as (config: any) => unknown;
    expect(() =>
      define({
        from: before,
        to: {
          users,
          records: s.table(
            { ownerId: s.uuid(), counters: s.array(s.bigint()).default([2n]), score: s.int() },
            { owner: s.rel("users", "ownerId") },
          ),
        },
      }),
    ).toThrow("unchanged column shapes");
    expect(() =>
      define({
        from: before,
        to: {
          users,
          records: s.table(
            {
              ownerId: s.uuid(),
              counters: s.array(s.bigint()).default([1n]),
              score: s.int().merge("counter"),
            },
            { owner: s.rel("users", "ownerId") },
          ),
        },
      }),
    ).toThrow("unchanged column shapes");
  });

  it("roundtrips every supported structural default in generated reference witnesses", async () => {
    const id = "11111111-1111-4111-8111-111111111111";
    const columns = {
      ownerId: s.uuid().default(id),
      title: s.string().default('quote"\\\n${globalThis.injected = true}'),
      enabled: s.boolean().default(false),
      count: s.int().default(-2),
      score: s.float().default(-0),
      big: s.bigint().default(9223372036854775807n),
      time: s.timestamp().default(new Date("2026-01-01T00:00:00Z")),
      bytes: s.bytes().default(new Uint8Array([0, 128, 255])),
      nested: s.array(s.array(s.bigint())).default([[1n, -2n], []]),
      times: s.array(s.array(s.timestamp())).default([[new Date(1234)]]),
      json: s
        .json({
          type: "object",
          properties: Object.fromEntries([
            ["__proto__", { type: "object" as const }],
            ["\uE000", { type: "string" as const }],
            ["\u{10000}", { type: "string" as const }],
          ]),
        })
        .default('{ "__proto__": {"safe":true}, "quote": "x" }'),
      status: s.enum("draft", "done").default("draft"),
      missing: s.string().optional().default(null),
      refs: s.array(s.uuid()).default([id]),
    };
    const users = s.table({ name: s.string().default("anonymous") }, {});
    const from = s.defineApp({ users, records: s.table(columns, {}) }).wasmSchema;
    const to = s.defineApp({
      users,
      records: s.table(columns, { owner: s.rel("users", "ownerId") }),
    }).wasmSchema;
    const source = renderMigrationStub({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: from,
      toSchema: to,
    });
    const migration = new Function(
      "s",
      source
        .replace('import { schema as s } from "jazz-tools";', "")
        .replace("export default", "return"),
    )(s);
    for (const [actual, expected] of [
      [s.defineApp(migration.from).wasmSchema, from],
      [s.defineApp(migration.to).wasmSchema, to],
    ]) {
      expect(wasmSchemasEqual(actual!, expected!)).toBe(true);
      expect(structuralSchemaHash(actual!)).toBe(structuralSchemaHash(expected!));
      expect(actual).toEqual(expected);
    }
    expect(migration.forward).toEqual([{ table: "records", operations: [] }]);
    const root = await mkdtemp(join(tmpdir(), "jazz-default-witness-types-"));
    try {
      await writeFile(join(root, "package.json"), '{"type":"module"}');
      await writeFile(join(root, "migration.ts"), source);
      await writeFile(
        join(root, "tsconfig.json"),
        JSON.stringify({
          extends: new URL("../../tsconfig.tests.json", import.meta.url).pathname,
          compilerOptions: {
            rootDir: "/",
            typeRoots: [new URL("../../node_modules/@types", import.meta.url).pathname],
          },
          include: [join(root, "migration.ts")],
          exclude: [],
        }),
      );
      await promisify(execFile)(process.execPath, [
        new URL("../../node_modules/typescript/bin/tsc", import.meta.url).pathname,
        "--project",
        join(root, "tsconfig.json"),
      ]).catch((error: { stdout?: string; stderr?: string }) => {
        throw new Error(
          `Generated witness typecheck failed:\n${error.stdout ?? ""}${error.stderr ?? ""}`,
        );
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 30000);

  it("rejects timestamp defaults that Date cannot represent exactly", () => {
    for (const value of [0.5, 8640000000000001]) {
      const fromSchema = s.defineApp({ users: s.table({ name: s.string() }, {}) }).wasmSchema;
      const toSchema = s.defineApp({
        users: s.table({ name: s.string() }, {}),
        records: s.table({ time: s.timestamp().default(value) }, {}),
      }).wasmSchema;
      expect(() =>
        renderMigrationStub({
          fromHash: "aaaaaaaaaaaa",
          toHash: "bbbbbbbbbbbb",
          fromSchema,
          toSchema,
        }),
      ).toThrow("Cannot render migration timestamp default exactly");
    }
  });

  it("preserves default-only schema changes without transforming existing rows", () => {
    const definitions = [s.string(), s.string().default("before"), s.string().default("after")];
    for (const before of definitions)
      for (const after of definitions) {
        const from = { records: s.table({ title: before }, {}) };
        const to = { records: s.table({ title: after }, {}) };
        const migration = s.defineMigration({ from, to });
        expect(migration.forward).toEqual([]);
        expect(s.defineMigration({ from, to, migrate: {} }).forward).toEqual([]);
        expect(() =>
          assertMigrationMatchesCanonicalBundle(migration, {
            fromHash: "aaaaaaaaaaaa",
            toHash: "bbbbbbbbbbbb",
            fromSchema: s.defineApp(from).wasmSchema,
            toSchema: s.defineApp(to).wasmSchema,
          }),
        ).not.toThrow();
        if (before !== after)
          expect(wasmSchemasEqual(s.defineApp(from).wasmSchema, s.defineApp(to).wasmSchema)).toBe(
            false,
          );
      }
  });

  it("compares canonical defaults across equivalent builder input representations", () => {
    const from = {
      records: s.table(
        {
          time: s.timestamp().default(new Date(1234)),
          big: s.bigint().default(42n),
          bytes: s.bytes().default(new Uint8Array([1, 2])),
          json: s.json().default({ key: "value" }),
          nested: s.array(s.timestamp()).default([new Date(1234)]),
        },
        {},
      ),
    };
    const to = {
      records: s.table(
        {
          time: s.timestamp().default(1234),
          // Exercise JS inputs accepted by the shared runtime converters.
          big: s.bigint().default(42 as unknown as bigint),
          bytes: s.bytes().default([1, 2] as unknown as Uint8Array),
          json: s.json().default('{"key":"value"}'),
          nested: s.array(s.timestamp()).default([1234 as unknown as Date]),
        },
        {},
      ),
    };
    expect(s.defineMigration({ from, to }).forward).toEqual([]);
    expect(wasmSchemasEqual(s.defineApp(from).wasmSchema, s.defineApp(to).wasmSchema)).toBe(true);
  });

  it("accepts server JSON metadata object ordering but preserves array and default-text identity", () => {
    const users = s.table({ name: s.string() }, {});
    const columns = (serverOrder: boolean) => ({
      ownerId: s.uuid(),
      data: s
        .json(
          serverOrder
            ? {
                additionalProperties: false,
                properties: { a: { type: "number" }, z: { type: "string" } },
                type: "object",
              }
            : {
                type: "object",
                properties: { z: { type: "string" }, a: { type: "number" } },
                additionalProperties: false,
              },
        )
        .default({ z: "value", a: 1 }),
      nested: s
        .array(
          s.json(
            serverOrder
              ? { additionalProperties: false, type: "object" }
              : { type: "object", additionalProperties: false },
          ),
        )
        .default([{}]),
    });
    const from = { users, records: s.table(columns(false), {}) };
    const to = { users, records: s.table(columns(true), { owner: s.rel("users", "ownerId") }) };
    const migration = s.defineMigration({ from, to });
    const canonicalFrom = s.defineApp({ users, records: s.table(columns(true), {}) }).wasmSchema;
    const canonicalTo = s.defineApp(to).wasmSchema;
    expect(() =>
      assertMigrationMatchesCanonicalBundle(migration, {
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        fromSchema: canonicalFrom,
        toSchema: canonicalTo,
      }),
    ).not.toThrow();
    expect(migration.forward).toEqual([{ table: "records", operations: [] }]);
    const first = s.defineApp({
      records: s.table({ data: s.json({ enum: ["a", "b"] }).default('"a"') }, {}),
    }).wasmSchema;
    const reordered = s.defineApp({
      records: s.table({ data: s.json({ enum: ["b", "a"] }).default('"a"') }, {}),
    }).wasmSchema;
    expect(wasmSchemasEqual(first, reordered)).toBe(false);
    const changedText = s.defineApp({
      records: s.table({ data: s.json({ enum: ["a", "b"] }).default(' "a" ') }, {}),
    }).wasmSchema;
    expect(wasmSchemasEqual(first, changedText)).toBe(false);
  });

  it("exports snapshots and creates relation migrations with lossless bigint and bytes defaults", async () => {
    const { exportSchema, createMigration } = await import("./catalogue-project.js");
    const root = await mkdtemp(join(tmpdir(), "jazz-default-snapshots-"));
    const migrationsDir = join(root, "migrations");
    const schemaPath = join(root, "schema.ts");
    const source = (
      relation: boolean,
    ) => `import { schema as s } from ${JSON.stringify(new URL("../index.ts", import.meta.url).pathname)};
export const app = s.defineApp({
  users: s.table({ name: s.string() }, {}),
  records: s.table({ ownerId: s.uuid(), big: s.bigint().default(9223372036854775807n), bytes: s.bytes().default(new Uint8Array([0,128,255])), nested: s.array(s.bigint()).default([-9223372036854775808n]) }, ${relation ? '{ owner: s.rel("users", "ownerId") }' : "{}"})
});`;
    try {
      await writeFile(join(root, "package.json"), '{"type":"module"}');
      await writeFile(schemaPath, source(false));
      const before = await exportSchema({ schemaDir: root, migrationsDir: join(root, "exports") });
      const exported = JSON.parse(await readFile(before.snapshotPath!, "utf8"));
      expect(wasmSchemasEqual(exported, before.schema)).toBe(true);
      const initial = await createMigration({ schemaDir: root, migrationsDir });
      expect(initial.status).toBe("initial-snapshot");
      if (initial.status !== "initial-snapshot") throw new Error("Expected initial snapshot");
      expect(
        wasmSchemasEqual(JSON.parse(await readFile(initial.snapshotPath, "utf8")), before.schema),
      ).toBe(true);
      await writeFile(schemaPath, source(true));
      const generated = await createMigration({ schemaDir: root, migrationsDir });
      expect(generated.status).toBe("generated");
      if (generated.status !== "generated") throw new Error("Expected migration file");
      const code = await readFile(generated.filePath, "utf8");
      const migration = new Function(
        "s",
        code
          .replace('import { schema as s } from "jazz-tools";', "")
          .replace("export default", "return"),
      )(s);
      expect(wasmSchemasEqual(s.defineApp(migration.from).wasmSchema, before.schema)).toBe(true);
      const after = await exportSchema({ schemaDir: root, migrationsDir });
      expect(wasmSchemasEqual(s.defineApp(migration.to).wasmSchema, after.schema)).toBe(true);
      expect(migration.forward).toEqual([{ table: "records", operations: [] }]);
      expect(await createMigration({ schemaDir: root, migrationsDir })).toEqual({
        status: "unchanged",
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 20_000);

  it("loads and pushes the generated relation migration through the project API", async () => {
    const { computeSchemaHash } = await import("./catalogue.js");
    const { pushMigration } = await import("./catalogue-project.js");
    const users = s
      .table({ name: s.string(), peerId: s.uuid() }, { peer: s.rel("peers", "peerId") })
      .indexOnly(["name"])
      .branchBy("name");
    const peers = s.table({ userId: s.uuid() }, { user: s.rel("users", "userId") });
    const columns = {
      ownerId: s.uuid(),
      memberIds: s.array(s.uuid()).optional(),
      title: s.string().default("draft"),
      large: s.bigint().default(9007199254740993n),
      nested: s.array(s.bigint()).default([9223372036854775807n]),
    };
    const fromSchema = s.defineApp({ users, peers, records: s.table(columns, {}) }).wasmSchema;
    const toSchema = s.defineApp({
      users,
      peers,
      records: s.table(columns, {
        owner: s.rel("users", "ownerId"),
        members: s.rel("users", "memberIds"),
      }),
    }).wasmSchema;
    const fromHash = await computeSchemaHash(fromSchema),
      toHash = await computeSchemaHash(toSchema);
    const root = await mkdtemp(join(tmpdir(), "jazz-reference-migration-"));
    let body: any;
    try {
      await writeFile(join(root, "package.json"), '{"type":"module"}');
      const source = renderMigrationStub({ fromHash, toHash, fromSchema, toSchema });
      await writeFile(
        join(root, `references-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`),
        source.replace(
          '"jazz-tools"',
          JSON.stringify(new URL("../index.ts", import.meta.url).pathname),
        ),
      );
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: string, init?: RequestInit) => {
          if (input.endsWith("/schemas")) return Response.json({ hashes: [fromHash, toHash] });
          if (input.endsWith(`/schema/${fromHash}`))
            return new Response(
              JSON.stringify({ schema: { tables: fromSchema }, publishedAt: 0 }, (_, value) =>
                typeof value === "bigint" ? value.toString() : value,
              ),
            );
          if (input.endsWith(`/schema/${toHash}`))
            return new Response(
              JSON.stringify({ schema: { tables: toSchema }, publishedAt: 0 }, (_, value) =>
                typeof value === "bigint" ? value.toString() : value,
              ),
            );
          if (input.endsWith("/admin/migrations")) {
            body = JSON.parse(String(init?.body));
            return Response.json(
              { objectId: "44444444-4444-4444-4444-444444444444", fromHash, toHash },
              { status: 201 },
            );
          }
          throw new Error(`Unexpected fetch: ${input}`);
        }),
      );
      const path = join(root, `references-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`);
      for (const replacement of ["", '.default("altered")']) {
        await writeFile(
          path,
          source
            .replaceAll('.default("draft")', replacement)
            .replace(
              '"jazz-tools"',
              JSON.stringify(new URL("../index.ts", import.meta.url).pathname),
            ),
        );
        await expect(
          pushMigration({
            appId: "test-app",
            serverUrl: "http://localhost:1625",
            adminSecret: "test-secret",
            migrationsDir: root,
            fromHash,
            toHash,
          }),
        ).rejects.toThrow("does not match");
        expect(body).toBeUndefined();
      }
      await writeFile(
        path,
        source.replace(
          '"jazz-tools"',
          JSON.stringify(new URL("../index.ts", import.meta.url).pathname),
        ),
      );
      const result = await pushMigration({
        appId: "test-app",
        serverUrl: "http://localhost:1625",
        adminSecret: "test-secret",
        migrationsDir: root,
        fromHash,
        toHash,
      });
      expect(result.status).toBe("published");
      expect(body.forward).toEqual([{ table: "records", operations: [] }]);
      expect(body.fromHash).toBe(fromHash);
      expect(body.toHash).toBe(toHash);
    } finally {
      vi.unstubAllGlobals();
      await rm(root, { recursive: true, force: true });
    }
  });
});

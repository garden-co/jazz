import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
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

  it("loads and pushes the generated relation migration through the project API", async () => {
    const { computeSchemaHash } = await import("./catalogue.js");
    const { pushMigration } = await import("./catalogue-project.js");
    const users = s.table({ name: s.string() }, {});
    const columns = { ownerId: s.uuid(), memberIds: s.array(s.uuid()).optional() };
    const fromSchema = s.defineApp({ users, records: s.table(columns, {}) }).wasmSchema;
    const toSchema = s.defineApp({
      users,
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
            return Response.json({ schema: { tables: fromSchema }, publishedAt: 0 });
          if (input.endsWith(`/schema/${toHash}`))
            return Response.json({ schema: { tables: toSchema }, publishedAt: 0 });
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

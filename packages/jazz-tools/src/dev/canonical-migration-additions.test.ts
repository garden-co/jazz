import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";
import { assertMigrationMatchesCanonicalBundle } from "./catalogue.js";

it("refuses an added table hidden by a partial canonical source witness", () => {
  const projects = s.table({ title: s.string() }, {});
  const notes = s.table({ body: s.string() }, {});
  const partial = m.defineMigration({
    from: { projects },
    to: { projects, notes },
    createTables: { notes: true },
  });
  const published = s.defineApp({ projects, notes }, { migrations: "none" }).wasmSchema;
  expect(() =>
    assertMigrationMatchesCanonicalBundle(partial, {
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: published,
      toSchema: published,
    }),
  ).toThrow(/added table notes.*canonical source/i);
});

it("accepts a genuinely new table while a partial witness omits untouched tables", () => {
  const projects = s.table({ title: s.string() }, {});
  const notes = s.table({ body: s.string() }, {});
  const fresh = s.table({ value: s.string() }, {});
  const partial = m.defineMigration({
    from: { projects },
    to: { projects, fresh },
    createTables: { fresh: true },
  });
  const published = s.defineApp({ projects, notes }, { migrations: "none" }).wasmSchema;
  const next = s.defineApp({ projects, notes, fresh }, { migrations: "none" }).wasmSchema;
  expect(() =>
    assertMigrationMatchesCanonicalBundle(partial, {
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: published,
      toSchema: next,
    }),
  ).not.toThrow();
});

it.each([false, true])(
  "resolves partial witnesses against the sole published tip (history: %s)",
  async (history) => {
    const { deploy, computeSchemaHash } = await import("./catalogue.js");
    const { vi } = await import("vitest");
    const projects = s.table({ title: s.string() }, {});
    const notes = s.table({ body: s.string() }, {});
    const fresh = s.table({ value: s.string() }, {});
    const source = s.defineApp({ projects, notes }, { migrations: "none" });
    const target = s.defineApp({ projects, notes, fresh }, { migrations: "none" });
    const sourceHash = await computeSchemaHash(source.wasmSchema);
    const targetHash = await computeSchemaHash(target.wasmSchema);
    const olderHash = "0".repeat(64);
    const partial = m.defineMigration({
      from: { projects },
      to: { projects, fresh },
      createTables: { fresh: true },
    });
    let published = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith("/migrations/graph"))
          return new Response(
            JSON.stringify({
              activeSchemaHash: null,
              schemas: history ? [olderHash, sourceHash] : [sourceHash],
              migrations: history ? [{ fromHash: olderHash, toHash: sourceHash }] : [],
            }),
          );
        if (input.endsWith(`/schema/${sourceHash}`))
          return new Response(JSON.stringify({ schema: { tables: source.wasmSchema } }));
        expect(input.endsWith("/admin/bundles")).toBe(true);
        const body = JSON.parse(String(init?.body));
        expect(body.migrations).toEqual([
          {
            fromHash: sourceHash,
            toHash: targetHash,
            forward: [{ table: "fresh", added: true, operations: [] }],
          },
        ]);
        published++;
        return new Response(
          JSON.stringify({
            changed: true,
            published: { schemas: [targetHash], migrations: body.migrations },
          }),
        );
      }),
    );
    try {
      await deploy({
        appId: "canonical-tip",
        serverUrl: "http://localhost:1625",
        adminSecret: "test",
        schema: target,
        migration: partial,
        permissions: {},
      });
      expect(published).toBe(1);
    } finally {
      vi.unstubAllGlobals();
    }
  },
);

it("refuses an ambiguous partial source without publishing", async () => {
  const { deploy } = await import("./catalogue.js");
  const { vi } = await import("vitest");
  const projects = s.table({ title: s.string() }, {});
  const fresh = s.table({ value: s.string() }, {});
  const target = s.defineApp({ projects, fresh }, { migrations: "none" });
  const migration = m.defineMigration({
    from: { projects },
    to: { projects, fresh },
    createTables: { fresh: true },
  });
  let published = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string) => {
      if (input.endsWith("/migrations/graph"))
        return new Response(
          JSON.stringify({
            activeSchemaHash: null,
            schemas: ["0".repeat(64), "1".repeat(64)],
            migrations: [],
          }),
        );
      published++;
      throw new Error("Unexpected request before resolving the partial source");
    }),
  );
  try {
    await expect(
      deploy({
        appId: "canonical-tip",
        serverUrl: "http://localhost:1625",
        adminSecret: "test",
        schema: target,
        migration,
        permissions: {},
      }),
    ).rejects.toThrow("Partial migration source is ambiguous; supply fromHash.");
    expect(published).toBe(0);
  } finally {
    vi.unstubAllGlobals();
  }
});

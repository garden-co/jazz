import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { computeSchemaHash, deploy } from "./catalogue.js";
import type { DeploymentRequest } from "./catalogue-api.js";

const server = { appId: "deploy-test", serverUrl: "http://localhost:1625", adminSecret: "test" };
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
const reply = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
afterEach(() => vi.unstubAllGlobals());

it("deploys once per invocation and re-fetches the graph on the next invocation", async () => {
  const hash = await computeSchemaHash(app.wasmSchema);
  const calls: string[] = [];
  let stored = false;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      calls.push(input.split("/admin/")[1]!);
      if (input.endsWith("/migrations/graph"))
        return reply({
          activeSchemaHash: stored ? hash : null,
          schemas: stored ? [hash] : [],
          migrations: [],
        });
      expect(input).toBe(`${server.serverUrl}/apps/${server.appId}/admin/deploy`);
      expect(init?.headers).toMatchObject({ "X-Jazz-Admin-Secret": "test" });
      const body = JSON.parse(String(init?.body)) as DeploymentRequest;
      expect(body).toEqual({
        targetSchemaHash: hash,
        schemas: stored ? [] : [{ hash, schema: { tables: app.wasmSchema } }],
        migrations: [],
        permissions: {},
      });
      const result = {
        changed: !stored,
        published: { schemas: stored ? [] : [hash], migrations: [] },
      };
      stored = true;
      return reply(result);
    }),
  );
  expect((await deploy({ ...server, schema: app, permissions: {} })).changed).toBe(true);
  expect((await deploy({ ...server, schema: app, permissions: {} })).changed).toBe(false);
  expect(calls).toEqual(["migrations/graph", "deploy", "migrations/graph", "deploy"]);
});

it("surfaces server validation failures without retrying or publishing individual artifacts", async () => {
  const hash = await computeSchemaHash(app.wasmSchema);
  const fetchMock = vi.fn(async (input: string) => {
    if (input.endsWith("/migrations/graph"))
      return reply({ activeSchemaHash: hash, schemas: [hash], migrations: [] });
    expect(input.endsWith("/admin/deploy")).toBe(true);
    return reply(
      {
        code: "unreachable_deployment_target",
        error: "a concurrent deployment added another branch",
      },
      422,
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  await expect(deploy({ ...server, schema: app, permissions: {} })).rejects.toThrow(
    "a concurrent deployment added another branch",
  );
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("includes historical schemas and multiple migrations in a single request", async () => {
  const middle = s.defineApp({ notes: s.table({ title: s.string(), a: s.string() }, {}) });
  const target = s.defineApp({
    notes: s.table({ title: s.string(), a: s.string(), b: s.string() }, {}),
  });
  const hashes = await Promise.all(
    [app, middle, target].map((app) => computeSchemaHash(app.wasmSchema)),
  );
  const migrations = [
    s.defineMigration({
      from: { notes: s.table({ title: s.string() }, {}) },
      to: { notes: s.table({ title: s.string(), a: s.string() }, {}) },
      migrate: { notes: { a: s.add.string({ default: "" }) } },
    }),
    s.defineMigration({
      from: { notes: s.table({ title: s.string(), a: s.string() }, {}) },
      to: { notes: s.table({ title: s.string(), a: s.string(), b: s.string() }, {}) },
      migrate: { notes: { b: s.add.string({ default: "" }) } },
    }),
  ];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return reply({ activeSchemaHash: hashes[0], schemas: [hashes[0]], migrations: [] });
      expect(input.endsWith("/admin/deploy")).toBe(true);
      const body = JSON.parse(String(init?.body)) as DeploymentRequest;
      expect(body.schemas.map((schema) => schema.hash).sort()).toEqual(hashes.slice(1).sort());
      expect(body.migrations.map(({ fromHash, toHash }) => [fromHash, toHash])).toEqual([
        [hashes[0], hashes[1]],
        [hashes[1], hashes[2]],
      ]);
      return reply({
        changed: true,
        published: {
          schemas: hashes.slice(1),
          migrations: body.migrations.map(({ fromHash, toHash }) => ({ fromHash, toHash })),
        },
      });
    }),
  );
  const result = await deploy({
    ...server,
    schema: target,
    schemas: [app, middle],
    migrations,
    permissions: {},
  });
  expect(result.published.migrations).toHaveLength(2);
});

it("preserves bigint and byte defaults in the deployment JSON", async () => {
  const to = { notes: s.table({ title: s.string(), count: s.bigint(), data: s.bytes() }, {}) };
  const target = s.defineApp(to);
  const fromHash = await computeSchemaHash(app.wasmSchema);
  const migration = s.defineMigration({
    from: { notes: s.table({ title: s.string() }, {}) },
    to,
    migrate: {
      notes: {
        count: s.add.bigint({ default: 9223372036854775807n }),
        data: s.add.bytes({ default: new Uint8Array([0, 128, 255]) }),
      },
    },
  });
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return reply({ activeSchemaHash: fromHash, schemas: [fromHash], migrations: [] });
      expect(input.endsWith("/admin/deploy")).toBe(true);
      const body = JSON.parse(String(init?.body));
      expect(body.migrations[0].forward[0].operations).toEqual([
        {
          type: "introduce",
          column: "count",
          column_type: { type: "BigInt" },
          value: { type: "BigInt", value: "9223372036854775807" },
        },
        {
          type: "introduce",
          column: "data",
          column_type: { type: "Bytea" },
          value: { type: "Bytea", value: [0, 128, 255] },
        },
      ]);
      return reply({
        changed: true,
        published: {
          schemas: [body.targetSchemaHash],
          migrations: [{ fromHash, toHash: body.targetSchemaHash }],
        },
      });
    }),
  );
  await deploy({ ...server, schema: target, migration, permissions: {} });
});

import { afterEach, expect, it, vi } from "vitest";
import { testApp } from "../testing/index.js";
import { computeSchemaHash, deploy } from "./catalogue.js";
import type { DeploymentRequest } from "./catalogue-api.js";

const server = { appId: "deploy-test", serverUrl: "http://localhost:1625", adminSecret: "test" };
const app = testApp({ tables: { notes: { columns: { title: "string" } } } });
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
          latestBundleSchemaHash: stored ? hash : null,
          schemas: stored ? [hash] : [],
          migrations: [],
        });
      expect(input).toBe(`${server.serverUrl}/apps/${server.appId}/admin/bundles`);
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
  expect((await deploy({ ...server, app, permissions: {} })).changed).toBe(true);
  expect((await deploy({ ...server, app, permissions: {} })).changed).toBe(false);
  expect(calls).toEqual(["migrations/graph", "bundles", "migrations/graph", "bundles"]);
});

it("surfaces server validation failures without retrying or publishing individual artifacts", async () => {
  const hash = await computeSchemaHash(app.wasmSchema);
  const fetchMock = vi.fn(async (input: string) => {
    if (input.endsWith("/migrations/graph"))
      return reply({ latestBundleSchemaHash: hash, schemas: [hash], migrations: [] });
    expect(input.endsWith("/admin/bundles")).toBe(true);
    return reply(
      {
        code: "disconnected_migration_graph",
        error: "a concurrent deployment added another branch",
      },
      422,
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  await expect(deploy({ ...server, app, permissions: {} })).rejects.toThrow(
    "a concurrent deployment added another branch",
  );
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("includes historical schemas and multiple migrations in a single request", async () => {
  const v1 = { notes: { columns: { title: "string" } } };
  const v2 = { notes: { columns: { title: "string", a: "string" } } };
  const v3 = { notes: { columns: { title: "string", a: "string", b: "string" } } };
  const middle = testApp({ tables: v2 });
  const target = testApp({
    tables: v3,
    history: { v1: { tables: v1 }, v2: { tables: v2 } },
    migrations: [
      { from: "v1", to: "v2", migrate: { notes: { a: { add: "string", default: "" } } } },
      { from: "v2", migrate: { notes: { b: { add: "string", default: "" } } } },
    ],
  });
  const hashes = await Promise.all(
    [app, middle, target].map((app) => computeSchemaHash(app.wasmSchema)),
  );
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return reply({ latestBundleSchemaHash: hashes[0], schemas: [hashes[0]], migrations: [] });
      expect(input.endsWith("/admin/bundles")).toBe(true);
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
  const result = await deploy({ ...server, app: target, permissions: {} });
  expect(result.published.migrations).toHaveLength(2);
});

it("preserves bigint and byte defaults in the deployment JSON", async () => {
  const target = testApp({
    tables: { notes: { columns: { title: "string", count: "bigint", data: "bytes" } } },
    history: { v1: { tables: { notes: { columns: { title: "string" } } } } },
    migrations: [
      {
        from: "v1",
        migrate: {
          notes: {
            count: { add: "bigint", default: "9223372036854775807" },
            // base64 of [0, 128, 255]
            data: { add: "bytes", default: "AID/" },
          },
        },
      },
    ],
  });
  const fromHash = await computeSchemaHash(app.wasmSchema);
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return reply({ latestBundleSchemaHash: fromHash, schemas: [fromHash], migrations: [] });
      expect(input.endsWith("/admin/bundles")).toBe(true);
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
  await deploy({ ...server, app: target, permissions: {} });
});

it("keeps a single historical migration separate from the deployment head", async () => {
  const old = { notes: { columns: { title: "string" } } };
  const middle = { notes: { columns: { title: "string", a: "string" } } };
  const target = testApp({
    tables: { notes: { columns: { title: "string", a: "string", b: "string" } } },
    history: { old: { tables: old }, middle: { tables: middle } },
    migrations: [
      { from: "old", to: "middle", migrate: { notes: { a: { add: "string", default: "" } } } },
    ],
  });
  const hashes = await Promise.all(
    [testApp({ tables: old }), testApp({ tables: middle }), target].map((app) =>
      computeSchemaHash(app.wasmSchema),
    ),
  );
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/migrations/graph"))
        return reply({ latestBundleSchemaHash: hashes[2], schemas: hashes, migrations: [] });
      expect(input.endsWith("/admin/bundles")).toBe(true);
      const body = JSON.parse(String(init?.body)) as DeploymentRequest;
      expect(body.migrations.map(({ fromHash, toHash }) => [fromHash, toHash])).toEqual([
        [hashes[0], hashes[1]],
      ]);
      return reply({ changed: true, published: { schemas: [], migrations: body.migrations } });
    }),
  );
  await deploy({ ...server, app: target, permissions: {} });
});

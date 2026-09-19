import { afterEach, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { computeSchemaHash, deploy, MissingMigrationError } from "./catalogue.js";

const server = { appId: "deploy-test", serverUrl: "http://localhost:1625", adminSecret: "test" };
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
const reply = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
afterEach(() => vi.unstubAllGlobals());

it("publishes an explicit empty permission bundle on first deploy and reuses the schema on retry", async () => {
  const hash = await computeSchemaHash(app.wasmSchema);
  const writes: string[] = [];
  let stored = false;
  let failPermissions = true;
  let head: {
    schemaHash: string;
    bundleObjectId: string;
    version: number;
    parentBundleObjectId: string | null;
  } | null = null;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${server.appId}/schemas`))
        return reply({ hashes: stored ? [hash] : [] });
      if (input.endsWith(`/schema/${hash}`))
        return reply({ schema: { tables: app.wasmSchema }, publishedAt: 0 });
      if (input.endsWith("/permissions/head")) return reply({ head });
      const body = JSON.parse(String(init?.body));
      if (input.endsWith("/admin/schemas")) {
        writes.push("schema");
        stored = true;
        return reply({ hash, objectId: "schema-object" }, 201);
      }
      if (input.endsWith("/admin/permissions")) {
        writes.push("permissions");
        if (failPermissions) {
          failPermissions = false;
          return reply({ error: "temporary publication failure" }, 503);
        }
        expect(body.permissions).toEqual({});
        expect(body.expectedParentBundleObjectId).toBe(head?.bundleObjectId ?? null);
        head = {
          schemaHash: hash,
          version: (head?.version ?? 0) + 1,
          bundleObjectId: `bundle-${writes.length}`,
          parentBundleObjectId: head?.bundleObjectId ?? null,
        };
        return reply({ head }, 201);
      }
      throw new Error(`Unexpected request: ${input}`);
    }),
  );
  await expect(deploy({ ...server, schema: app, permissions: {} })).rejects.toThrow();
  expect(
    (await deploy({ ...server, schema: app, permissions: {} })).permissions.head,
  ).not.toBeNull();
  expect((await deploy({ ...server, schema: app, permissions: {} })).schema.status).toBe(
    "already-stored",
  );
  expect(writes).toEqual(["schema", "permissions", "permissions", "permissions"]);
});

it("rejects a missing required migration before publishing a new schema", async () => {
  const previous = s.defineApp({ notes: s.table({ oldTitle: s.string() }, {}) });
  const fromHash = await computeSchemaHash(previous.wasmSchema);
  const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
    expect(init?.method ?? "GET").toBe("GET");
    if (input.endsWith(`/apps/${server.appId}/schemas`)) return reply({ hashes: [fromHash] });
    if (input.endsWith(`/schema/${fromHash}`))
      return reply({ schema: { tables: previous.wasmSchema }, publishedAt: 0 });
    if (input.endsWith("/permissions/head"))
      return reply({
        head: {
          schemaHash: fromHash,
          bundleObjectId: "old-bundle",
          version: 1,
          parentBundleObjectId: null,
        },
      });
    throw new Error(`Unexpected request: ${input}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  await expect(deploy({ ...server, schema: app, permissions: {} })).rejects.toBeInstanceOf(
    MissingMigrationError,
  );
  expect(fetchMock.mock.calls.every(([, init]) => (init?.method ?? "GET") === "GET")).toBe(true);
});

it("rejects a concurrent deployment instead of replacing its permissions head", async () => {
  const hash = await computeSchemaHash(app.wasmSchema);
  let concurrentHead = false;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${server.appId}/schemas`)) return reply({ hashes: [] });
      if (input.endsWith("/permissions/head")) {
        return reply({
          head: concurrentHead
            ? {
                schemaHash: hash,
                bundleObjectId: "concurrent-bundle",
                version: 1,
                parentBundleObjectId: null,
              }
            : null,
        });
      }
      if (input.endsWith("/admin/schemas")) {
        concurrentHead = true;
        return reply({ hash, objectId: "schema-object" }, 201);
      }
      if (input.endsWith("/admin/permissions")) {
        expect(JSON.parse(String(init?.body)).expectedParentBundleObjectId).toBeNull();
        return reply({ error: "permissions head changed" }, 409);
      }
      throw new Error(`Unexpected request: ${input}`);
    }),
  );
  await expect(deploy({ ...server, schema: app, permissions: {} })).rejects.toThrow(
    "permissions head changed",
  );
});

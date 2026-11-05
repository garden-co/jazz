import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { Db } from "./db.js";

it("qualifies owner catalogue readiness with the requested schema's local admission", async () => {
  const requested = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const unpublishedSchema = new Error("Requested schema has not been admitted");
  let admitted = false;
  let published = false;
  const events: string[] = [];
  const ownerReady = vi.fn(async () => {
    events.push("owner-publication");
    return published;
  });
  const queryInternal = vi.fn(async (_query: string, _options: unknown, _session: unknown) => {
    events.push("requested-schema");
    if (!admitted) throw unpublishedSchema;
    return [];
  });
  const requestedClient = { hasAuthenticatedCatalogue: ownerReady, queryInternal };
  const ensureReady = vi.fn(async (_tier: string) => undefined);
  const tableIdentity = vi.fn(async (_table: unknown, _localOnly: boolean) => {
    events.push("local-identity");
    return "00000000-0000-4000-8000-000000000001";
  });
  const getClient = vi.fn((schema: unknown) => {
    expect(schema).toBe(requested.projects._schema);
    return requestedClient;
  });
  const db = Object.create(Db.prototype) as Db;
  Object.defineProperties(db, {
    getClient: { value: getClient },
    ensureReady: { value: ensureReady },
    getAccessContext: { value: () => undefined },
    tableIdentity: { value: tableIdentity },
  });

  await expect(db.authenticatedTableIdentity(requested.projects)).resolves.toBeNull();
  expect(queryInternal).not.toHaveBeenCalled();
  expect(tableIdentity).not.toHaveBeenCalled();
  expect(events).toEqual(["owner-publication"]);
  expect(ensureReady).toHaveBeenCalledWith("runtime");

  published = true;
  events.length = 0;
  ownerReady.mockClear();

  await expect(db.authenticatedTableIdentity(requested.projects)).rejects.toBe(unpublishedSchema);
  expect(ownerReady).toHaveBeenCalledOnce();
  expect(tableIdentity).not.toHaveBeenCalled();
  expect(events).toEqual(["owner-publication", "requested-schema"]);
  expect(queryInternal).toHaveBeenCalledWith(expect.any(String), { tier: "local-only" }, undefined);
  expect(JSON.parse(queryInternal.mock.calls[0]![0])).toEqual({
    table: "projects",
    conditions: [],
    array_subqueries: [],
    limit: 0,
  });
  expect(ensureReady).toHaveBeenCalledWith("runtime");

  admitted = true;
  events.length = 0;
  await expect(db.authenticatedTableIdentity(requested.projects)).resolves.toBe(
    "00000000-0000-4000-8000-000000000001",
  );
  expect(events).toEqual(["owner-publication", "requested-schema", "local-identity"]);
  expect(tableIdentity).toHaveBeenCalledWith(requested.projects, true);
  expect(ensureReady.mock.calls.every(([tier]) => tier === "runtime")).toBe(true);
  expect(queryInternal.mock.calls).toHaveLength(2);
});

it("keeps authenticated local lookups separate from fresh general identities", async () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const identity = "00000000-0000-4000-8000-000000000001";
  let published = true;
  const coverCatalogue = vi.fn(async () => undefined);
  const queryInternal = vi.fn(async () => []);
  const runtime = { tableIdentity: vi.fn(async () => identity) };
  const client = {
    getRuntime: () => runtime,
    hasAuthenticatedCatalogue: async () => published,
    queryInternal,
    coverCatalogue,
  };
  const ensureReady = vi.fn(async (_tier: string) => undefined);
  const db = Object.create(Db.prototype) as Db;
  Object.defineProperties(db, {
    getClient: { value: () => client },
    ensureReady: { value: ensureReady },
    getAccessContext: { value: () => undefined },
    assertOpen: { value: () => undefined },
    connection: {
      value: {
        initialExplicitOfflineState: () => undefined,
        isExplicitlyOffline: () => false,
      },
    },
  });

  await expect(db.authenticatedTableIdentity(app.projects)).resolves.toBe(identity);
  expect(queryInternal).toHaveBeenCalledOnce();
  expect(coverCatalogue).not.toHaveBeenCalled();
  expect(ensureReady.mock.calls.every(([tier]) => tier === "runtime")).toBe(true);

  await expect(db.tableIdentity(app.projects)).resolves.toBe(identity);
  expect(coverCatalogue).toHaveBeenCalledOnce();
  expect(coverCatalogue).toHaveBeenCalledWith("projects", undefined);
  expect(ensureReady).toHaveBeenCalledWith("server");

  published = false;
  await expect(db.authenticatedTableIdentity(app.projects)).resolves.toBeNull();
  await expect(db.tableIdentity(app.projects)).resolves.toBe(identity);
  expect(queryInternal).toHaveBeenCalledOnce();
  expect(coverCatalogue).toHaveBeenCalledTimes(2);
});

it("returns no local identity before catalogue admission without fetching coverage", async () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  let published = false;
  const identity = "00000000-0000-4000-8000-000000000001";
  const runtime = {
    tableIdentity: vi.fn(async () => identity),
    columnIdentity: vi.fn(async () => identity),
  };
  const client = {
    getRuntime: () => runtime,
    hasAuthenticatedCatalogue: async () => published,
    coverCatalogue: vi.fn(async () => undefined),
  };
  const ensureReady = vi.fn(async (_tier: string) => undefined);
  const db = Object.create(Db.prototype) as Db;
  Object.defineProperties(db, {
    getClient: { value: () => client },
    ensureReady: { value: ensureReady },
    assertOpen: { value: () => undefined },
  });
  await expect(db.tableIdentity(app.projects, true)).resolves.toBeNull();
  await expect(db.columnIdentity(app.projects, "title", true)).resolves.toBeNull();
  expect(runtime.tableIdentity).not.toHaveBeenCalled();
  expect(runtime.columnIdentity).not.toHaveBeenCalled();
  expect(client.coverCatalogue).not.toHaveBeenCalled();
  published = true;
  await expect(db.tableIdentity(app.projects, true)).resolves.toBe(identity);
  await expect(db.columnIdentity(app.projects, "title", true)).resolves.toBe(identity);
  expect(ensureReady.mock.calls.every(([tier]) => tier === "runtime")).toBe(true);
  expect(client.coverCatalogue).not.toHaveBeenCalled();
});

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
  expect(ensureReady).toHaveBeenCalledWith("local");

  published = true;
  events.length = 0;
  ownerReady.mockClear();

  await expect(db.authenticatedTableIdentity(requested.projects)).rejects.toBe(unpublishedSchema);
  expect(ownerReady).toHaveBeenCalledOnce();
  expect(tableIdentity).not.toHaveBeenCalled();
  expect(events).toEqual(["owner-publication", "requested-schema"]);
  expect(queryInternal).toHaveBeenCalledWith(
    expect.any(String),
    { tier: "local", propagation: "local-only" },
    undefined,
  );
  expect(JSON.parse(queryInternal.mock.calls[0]![0])).toEqual({
    table: "projects",
    conditions: [],
    array_subqueries: [],
    limit: 0,
  });
  expect(ensureReady).toHaveBeenCalledWith("local");

  admitted = true;
  events.length = 0;
  await expect(db.authenticatedTableIdentity(requested.projects)).resolves.toBe(
    "00000000-0000-4000-8000-000000000001",
  );
  expect(events).toEqual(["owner-publication", "requested-schema", "local-identity"]);
  expect(tableIdentity).toHaveBeenCalledWith(requested.projects, true);
  expect(ensureReady.mock.calls.every(([tier]) => tier === "local")).toBe(true);
  expect(queryInternal.mock.calls).toHaveLength(2);
});

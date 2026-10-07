import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";
import type { Db } from "../../src/runtime/db.js";

it("resolves one accepted scope on fresh browser clients before querying rows", async () => {
  const server = await getJazzServerInfo(`e2ee-scope-${crypto.randomUUID()}`);
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.projects.allowRead.always();
  });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({ ...server, schema: app, permissions });
    const account = await acquireBrowserTestAccount(server);
    for (let index = 0; index < 2; index++) {
      clients.push(
        await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
        }),
      );
    }
    const identity = await clients[0]!.tableIdentity(app.projects);
    expect(identity).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(await clients[1]!.tableIdentity(app.projects)).toBe(identity);
    await clients[0]!.shutdown();
    await expect(clients[0]!.tableIdentity(app.projects)).rejects.toThrow();
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 30_000);

it("resolves the same scope without permission to read its rows", async () => {
  const server = await getJazzServerInfo(`e2ee-scope-denied-${crypto.randomUUID()}`);
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, () => {});
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({ ...server, schema: app, permissions });
    const account = await acquireBrowserTestAccount(server);
    for (let index = 0; index < 2; index++) {
      clients.push(
        await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
        }),
      );
    }
    const identity = await clients[0]!.tableIdentity(app.projects);
    expect(identity).not.toBeNull();
    expect(await clients[1]!.tableIdentity(app.projects)).toBe(identity);
    expect(await clients[0]!.all(app.projects, { tier: "remote" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 30_000);

it.each(["memory", "persistent"] as const)(
  "refreshes catalogue identities across reconnect and rename with %s storage",
  async (storage) => {
    const server = await getJazzServerInfo(`e2ee-identity-refresh-${crypto.randomUUID()}`);
    const before = { projects: s.table({ title: s.string() }, {}) };
    const afterColumn = { projects: s.table({ body: s.string() }, {}) };
    const afterTable = { initiatives: s.table({ body: s.string() }, {}) };
    const oldApp = s.defineApp(before);
    const columnApp = s.defineApp(afterColumn);
    const tableApp = s.defineApp(afterTable);
    let db: Db | undefined;
    try {
      await deploy({
        ...server,
        schema: oldApp,
        permissions: definePermissions(oldApp, ({ policy }) => policy.projects.allowRead.always()),
      });
      db = await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        account: await acquireBrowserTestAccount(server),
        driver:
          storage === "memory"
            ? { type: "memory" }
            : { type: "persistent", dbName: `e2ee-identity-${crypto.randomUUID()}` },
      });
      const table = await db.tableIdentity(oldApp.projects);
      const column = await db.columnIdentity(oldApp.projects, "title");
      expect(column).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
      await db.disconnect();
      await deploy({
        ...server,
        schema: columnApp,
        permissions: definePermissions(columnApp, ({ policy }) =>
          policy.projects.allowRead.always(),
        ),
        migration: s.defineMigration({
          from: before,
          to: afterColumn,
          migrate: { projects: { body: s.renameFrom("title") } },
        }),
      });
      await db.reconnect();
      expect(await db.columnIdentity(oldApp.projects, "body")).toBe(column);
      expect(await db.columnIdentity(oldApp.projects, "title")).toBeNull();
      expect(await db.tableIdentity(oldApp.projects)).toBe(table);

      await db.disconnect();
      await deploy({
        ...server,
        schema: tableApp,
        permissions: definePermissions(tableApp, ({ policy }) =>
          policy.initiatives.allowRead.always(),
        ),
        migration: s.defineMigration({
          from: afterColumn,
          to: afterTable,
          renameTables: { initiatives: s.renameTableFrom("projects") },
        }),
      });
      await db.reconnect();
      expect(await db.tableIdentity(oldApp.projects)).toBeNull();
      expect(await db.columnIdentity(oldApp.projects, "body")).toBeNull();
    } finally {
      await db?.shutdown();
      await stopJazzServer(server.serverUrl);
    }
  },
  60_000,
);

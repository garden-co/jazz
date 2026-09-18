import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

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
    expect(await clients[0]!.all(app.projects, { tier: "edge" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 30_000);

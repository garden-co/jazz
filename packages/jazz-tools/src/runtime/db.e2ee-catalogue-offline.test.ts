import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("resolves already loaded E2EE catalogue identities without remote coverage while offline", async () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: definePermissions(app, ({ policy }) => {
        policy.projects.allowRead.always();
      }),
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const table = await db.tableIdentity(app.projects);
    const column = await db.columnIdentity(app.projects, "title");
    expect(table).toEqual(expect.any(String));
    expect(column).toEqual(expect.any(String));
    await db.disconnect();
    const identities = Promise.all([
      db.tableIdentity(app.projects),
      db.columnIdentity(app.projects, "title"),
    ]);
    expect(
      await Promise.race([
        identities,
        new Promise<string>((resolve) => {
          timer = setTimeout(() => resolve("waiting for remote coverage"), 2_000);
        }),
      ]),
    ).toEqual([table, column]);
  } finally {
    clearTimeout(timer);
    if (db) {
      await db.reconnect();
      await db.shutdown();
    }
    await server.stop();
  }
}, 30_000);

import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { startTestJazz, testApp } from "../testing/index.js";

it("accepts a serializable read of an unchanged table after migration", async () => {
  const projects = { columns: { title: "string" } };
  const before = { projects, notes: { columns: { title: "string" } } };
  const after = { projects, notes: { columns: { body: "string" } } };
  const oldApp = testApp({ tables: before });
  const newApp = testApp({
    tables: after,
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
  });
  const permissions = {
    format: 1,
    tables: { projects: { read: "true", insert: "true" }, notes: { read: "true" } },
  };
  const server = await startTestJazz({
    app: oldApp,
    permissions,
    coreOptions: { inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const project = await db.insert(oldApp.projects, { title: "Project" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await server.redeploy({ app: newApp, permissions });
    db = await createDb(account);
    const query = newApp.projects.where({ id: project.id });
    expect(await db.one(query, { tier: "remote" })).toEqual(project);
    const tx = db.beginSerializableTransaction();
    expect(await tx.one(query, { tier: "local-first" })).toEqual(project);
    await tx.commit().wait();
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

it("preserves serializable conflict detection after renaming the queried table", async () => {
  const before = { projects: { columns: { title: "string" } } };
  const after = { initiatives: { columns: { title: "string" } } };
  const oldApp = testApp({ tables: before });
  const newApp = testApp({
    tables: after,
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", renameTables: { initiatives: "projects" } }],
  });
  const oldPermissions = { format: 1, tables: { projects: { read: "true", insert: "true" } } };
  const newPermissions = { format: 1, tables: { initiatives: { read: "true", insert: "true" } } };
  const server = await startTestJazz({
    app: oldApp,
    permissions: oldPermissions,
    coreOptions: { inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const project = await db.insert(oldApp.projects, { title: "Project" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await server.redeploy({ app: newApp, permissions: newPermissions });
    db = await createDb(account);
    writer = await createDb(await localAccountConfig(server.appId, server.url));
    expect(await db.all(newApp.initiatives, { tier: "remote" })).toEqual([project]);
    const stable = db.beginSerializableTransaction();
    expect(await stable.all(newApp.initiatives, { tier: "local-first" })).toEqual([project]);
    await stable.commit().wait();
    const changed = db.beginSerializableTransaction();
    expect(await changed.all(newApp.initiatives, { tier: "local-first" })).toEqual([project]);
    await writer
      .insert(newApp.initiatives, { title: "Concurrent project" })
      .wait({ tier: "global" });
    await expect(changed.commit().wait()).rejects.toMatchObject({
      code: "serialization_conflict",
    });
  } finally {
    await writer?.shutdown();
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

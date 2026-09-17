import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("accepts an exclusive read of an unchanged table after migration", async () => {
  const projects = s.table({ title: s.string() }, {});
  const before = { projects, notes: s.table({ title: s.string() }, {}) };
  const after = { projects, notes: s.table({ body: s.string() }, {}) };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    migrate: { notes: { body: s.renameFrom("title") } },
  });
  const permissions = (app: typeof oldApp | typeof newApp) =>
    definePermissions(app, ({ policy }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
    });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: permissions(oldApp) });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const project = await db.insert(oldApp.projects, { title: "Project" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({ ...target, schema: newApp, permissions: permissions(newApp), migration });
    db = await createDb(account);
    const query = newApp.projects.where({ id: project.id });
    expect(await db.one(query, { tier: "global" })).toEqual(project);
    const tx = db.beginExclusiveTransaction();
    expect(await tx.one(query, { tier: "local" })).toEqual(project);
    await tx.commit().wait();
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

it("preserves exclusive conflict detection after renaming the queried table", async () => {
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = { initiatives: s.table({ title: s.string() }, {}) };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    renameTables: { initiatives: s.renameTableFrom("projects") },
  });
  const oldPermissions = definePermissions(oldApp, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
  });
  const newPermissions = definePermissions(newApp, ({ policy }) => {
    policy.initiatives.allowRead.always();
    policy.initiatives.allowInsert.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb(account);
    const project = await db.insert(oldApp.projects, { title: "Project" }).wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({ ...target, schema: newApp, permissions: newPermissions, migration });
    db = await createDb(account);
    writer = await createDb(await localAccountConfig(server.appId, server.url));
    expect(await db.all(newApp.initiatives, { tier: "global" })).toEqual([project]);
    const stable = db.beginExclusiveTransaction();
    expect(await stable.all(newApp.initiatives, { tier: "local" })).toEqual([project]);
    await stable.commit().wait();
    const changed = db.beginExclusiveTransaction();
    expect(await changed.all(newApp.initiatives, { tier: "local" })).toEqual([project]);
    await writer
      .insert(newApp.initiatives, { title: "Concurrent project" })
      .wait({ tier: "global" });
    await expect(changed.commit().wait()).rejects.toMatchObject({
      code: "exclusive_conflict",
    });
  } finally {
    await writer?.shutdown();
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

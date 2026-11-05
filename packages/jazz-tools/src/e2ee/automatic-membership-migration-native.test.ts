import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("migrates an existing app to its first encrypted table without declaring managed tables", async () => {
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = {
    ...before,
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  };
  const oldApp = s.defineApp(before);
  const app = s.defineApp(after);
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const account = await localAccountConfig(server.appId, server.url);
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: oldApp,
      permissions: definePermissions(oldApp, ({ policy }) => {
        policy.projects.allowRead.always();
        policy.projects.allowInsert.always();
      }),
    });
    const oldDb = await createDb(account);
    clients.push(oldDb);
    const project = await oldDb
      .insert(oldApp.projects, { title: "Before encryption" })
      .wait({ tier: "global" });

    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: definePermissions(app, ({ policy }) => {
        policy.projects.allowRead.always();
        policy.notes.allowRead.always();
      }),
      migration: m.defineMigration({ from: before, to: after, createTables: { notes: true } }),
    });
    const db = await createDb(account);
    clients.push(db);
    expect(await db.one(app.projects.where({ id: project.id }), { tier: "remote" })).toEqual(
      project,
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

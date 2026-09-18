import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("reads and searches existing ciphertext after renaming its column", async () => {
  const projects = s.table({ title: s.string() }, {});
  const before = {
    projects,
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };
  const after = {
    projects,
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"], indexes: { body: "equality" } }),
  };
  const oldApp = s.defineApp(before);
  const newApp = s.defineApp(after);
  const migration = s.defineMigration({
    from: before,
    to: after,
    migrate: { notes: { body: s.renameFrom("title") } },
  });
  const permissions = (app: typeof oldApp | typeof newApp) =>
    definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.notes.allowRead.always();
      policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let retained: string | null = null;
  const store = {
    async read() {
      return retained;
    },
    async update(transform: (current: string | null) => string) {
      retained = transform(retained);
    },
  };
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: permissions(oldApp) });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({ ...account, e2ee: { app: oldApp, store } });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(oldApp.projects, { title: "Project" });
    const note = tx.insert(oldApp.notes, { projectId: project.id, title: "Private title" });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await deploy({ ...target, schema: newApp, permissions: permissions(newApp), migration });
    db = await createDb({ ...account, e2ee: { app: newApp, store } });
    expect(await db.all(newApp.notes.where({ projectId: project.id }), { tier: "global" })).toEqual(
      [{ id: note.id, projectId: project.id, body: "Private title" }],
    );
    expect(
      await db.all(newApp.notes.where({ projectId: project.id, body: "Private title" }), {
        tier: "global",
      }),
    ).toEqual([{ id: note.id, projectId: project.id, body: "Private title" }]);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
